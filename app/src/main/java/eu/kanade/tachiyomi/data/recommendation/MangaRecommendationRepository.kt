package eu.kanade.tachiyomi.data.recommendation

import eu.kanade.domain.manga.model.toSManga
import eu.kanade.tachiyomi.data.track.anilist.dto.ALRecommendation
import eu.kanade.tachiyomi.network.HttpException
import eu.kanade.tachiyomi.source.Source
import eu.kanade.tachiyomi.source.isLocalOrStub
import eu.kanade.tachiyomi.source.model.FilterList
import eu.kanade.tachiyomi.source.model.SManga
import eu.kanade.tachiyomi.source.online.HttpSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import logcat.LogPriority
import tachiyomi.core.common.util.system.logcat
import tachiyomi.domain.manga.model.Manga
import java.net.URI
import java.security.SecureRandom
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class MangaRecommendationRepository internal constructor(
    private val localCandidateLoader: suspend (sourceId: Long, excludedUrl: String) -> List<Manga>,
    private val aniListLoader: suspend (mediaId: Long) -> List<ALRecommendation>,
    private val now: () -> Long = System::currentTimeMillis,
    private val monotonicNowNanos: () -> Long = System::nanoTime,
    private val onSourceRequest: (String) -> Unit = {},
    private val random: java.util.Random = SecureRandom(),
    private val exposureStore: RecommendationExposureStore = RecommendationExposureStore(now = now),
    private val requestScheduler: RecommendationRequestScheduler = RecommendationRequestScheduler(),
    private val sourcePolicyProvider: (Long) -> RecommendationSourcePolicy = { RecommendationSourcePolicy() },
    private val recommendationFilterProvider: () -> String = { "" },
    private val awaitProgressiveCardFrame: suspend () -> Unit = {
        delay(PROGRESSIVE_CARD_INTERVAL_MS)
    },
) {

    private val detailCache = DetailCache()
    private val keywordFilterCacheLock = Any()

    @Volatile
    private var keywordFilterCache: Pair<String, List<String>>? = null

    /**
     * Observes details/chapters traffic without putting page-critical work behind recommendation
     * pacing or cooldowns. A real 429 is still shared with recommendation requests so background
     * work backs off, while the source remains responsible for its own foreground request policy.
     */
    internal suspend fun <T> observeSourceRefresh(
        source: Source,
        block: suspend () -> T,
    ): T {
        requestScheduler.prepare(source.id, now())
        return try {
            block()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            if (e.isRecommendationRateLimited()) {
                requestScheduler.recordRateLimit(
                    sourceId = source.id,
                    nowMillis = now(),
                    retryAfterMillis = e.recommendationRetryAfterMillis(now()),
                    serverRequestLimit = e.recommendationRateLimit(),
                )
                logcat(LogPriority.WARN, e) { "Manga source refresh received HTTP 429" }
            }
            throw e
        }
    }

    internal fun resetLearnedRateProfile(sourceId: Long) {
        requestScheduler.resetLearnedRateProfile(sourceId)
    }

    internal fun observeRecommendations(
        source: Source,
        manga: SManga,
        aniListId: Long?,
        forceRefresh: Boolean = false,
        sessionExcludedUrls: Set<String> = emptySet(),
        sessionExcludedWorkKeys: Set<String> = emptySet(),
    ): Flow<RecommendationRows> = channelFlow {
        val currentUrl = RecommendationMetadata.safeUrl(manga)
        if (currentUrl.isBlank() || source.isLocalOrStub() || !sourcePolicyProvider(source.id).enabled) {
            send(
                RecommendationRows(
                    creatorAuthoritative = true,
                    similarAuthoritative = true,
                    isFinal = true,
                ),
            )
            return@channelFlow
        }

        requestScheduler.prepare(source.id, now())

        val databasePool = try {
            localCandidateLoader(source.id, currentUrl)
                .asSequence()
                .filter {
                    it.source == source.id &&
                        RecommendationMetadata.recommendationUrlKey(it.url) !=
                        RecommendationMetadata.recommendationUrlKey(currentUrl) &&
                        it.initialized
                }
                .take(MAX_LOCAL_POOL)
                .map(Manga::toSManga)
                .toList()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logcat(LogPriority.DEBUG, e) { "Unable to load local recommendation pool" }
            emptyList()
        }

        // The database is a relevance index only. Never render it directly: a card must come
        // from this page's source response, otherwise a previous page can leak into this one.
        val localPool = databasePool

        val targetCreators = RecommendationMetadata.extractCreators(manga)
        val targetIdentity = RecommendationMetadata.identity(source.id, manga)
        val targetExposureKey = targetIdentity.exposureKey
        val targetGenreIdentities = RecommendationMetadata.extractGenreIdentities(manga)
        val targetGenres = targetGenreIdentities.mapTo(linkedSetOf(), GenreIdentity::normalizedName)
        val documentFrequency = RecommendationRanking.documentFrequency(localPool)
        val targetTagProfile = RecommendationRanking.tagProfile(
            targetGenres = targetGenres,
            targetGenreIdentities = targetGenreIdentities,
            documentFrequency = documentFrequency,
            documentCount = localPool.size,
        )
        val exposureSnapshot = exposureStore.snapshot(source.id, targetExposureKey)
        val sourceExposureSnapshot = exposureStore.snapshot(source.id)
        // Start blank. UI rows appear only after a current-source query produces verified cards.
        // Coil still loads each card's cover independently after the card is composed.
        var rows = RecommendationRows()
        send(rows.forSession(source.id, sessionExcludedUrls, sessionExcludedWorkKeys))

        val stateMutex = Mutex()
        var hasPublishedVisibleCard = false
        suspend fun publish(transform: (RecommendationRows) -> RecommendationRows) {
            stateMutex.withLock {
                if (!sourcePolicyProvider(source.id).enabled) {
                    rows = RecommendationRows(
                        creatorAuthoritative = true,
                        similarAuthoritative = true,
                        isFinal = true,
                    )
                    send(rows)
                    return@withLock
                }
                val previousVisibleRows = rows.forSession(
                    source.id,
                    sessionExcludedUrls,
                    sessionExcludedWorkKeys,
                )
                val nextRows = transform(rows)
                val visibleRows = nextRows.forSession(
                    source.id,
                    sessionExcludedUrls,
                    sessionExcludedWorkKeys,
                )
                val previousVisibleCardCount =
                    previousVisibleRows.creatorWorks.size + previousVisibleRows.similarManga.size
                val visibleCardCount = visibleRows.creatorWorks.size + visibleRows.similarManga.size
                val addedVisibleCard = visibleCardCount > previousVisibleCardCount
                if (addedVisibleCard && hasPublishedVisibleCard) {
                    awaitProgressiveCardFrame()
                }
                rows = nextRows
                send(visibleRows)
                if (addedVisibleCard) hasPublishedVisibleCard = true
            }
        }

        val pacingDeadlineAllowanceMillis = requestScheduler.qualityDeadlineAllowanceMillis(
            sourceId = source.id,
            minRequestIntervalMillis = sourcePolicyProvider(source.id).minRequestIntervalMillis,
            nowMillis = now(),
        )

        val requestBudget = SourceRequestBudget(
            maxRequests = MAX_SOURCE_REQUESTS,
            semaphore = requestScheduler.semaphore(
                sourceId = source.id,
                maxConcurrency = MAX_SOURCE_CONCURRENCY,
            ),
            maxTextFilterRoutes = when {
                targetGenres.isEmpty() && aniListId == null -> MAX_CREATOR_ONLY_TEXT_FILTER_ROUTES
                else -> MAX_TEXT_FILTER_ROUTES
            },
            onRequest = onSourceRequest,
            softDeadlineNanos = monotonicNowNanos() +
                (SOFT_TIMEOUT_MS + pacingDeadlineAllowanceMillis) * NANOS_PER_MILLISECOND,
            monotonicNowNanos = monotonicNowNanos,
            nowMillis = now,
            requestScheduler = requestScheduler,
            sourceId = source.id,
            sourcePolicyProvider = sourcePolicyProvider,
            rateLimitRetryAt = {
                requestScheduler.sourceCooldownUntil(source.id, now())
            },
        )
        val detailBudget = SharedDetailBudget(
            maxDetails = MAX_DETAILS,
            reservedPerRow = RESERVED_DETAILS_PER_ROW,
        )
        val similarNetworkComplete = CompletableDeferred<Unit>()
        val prioritizeSimilarRequests = targetTagProfile.allTags.isNotEmpty() ||
            aniListId != null

        withTimeoutOrNull((HARD_TIMEOUT_MS + pacingDeadlineAllowanceMillis).coerceAtMost(MAX_HARD_TIMEOUT_MS)) {
            supervisorScope {
                val creatorJob = launch {
                    try {
                        val networkCreator = loadCreatorWorks(
                            source = source,
                            manga = manga,
                            creators = targetCreators,
                            requestBudget = requestBudget,
                            detailBudget = detailBudget,
                            forceRefresh = forceRefresh,
                            knownByUrl = localPool.associateBy(::recommendationKey),
                            fallbackStart = similarNetworkComplete,
                            prioritizeSimilarRequests = prioritizeSimilarRequests,
                        )
                        val combined = networkCreator.manga
                            .filterNot(::isBlockedRecommendation)
                            .filterNot {
                                RecommendationMetadata.sameWork(
                                    targetIdentity,
                                    RecommendationMetadata.identity(source.id, it),
                                )
                            }
                            .distinctWorks(source.id)
                            .take(MAX_INTERNAL_RESULTS)
                        // Publish verified works one at a time. Once a card is visible during this
                        // run it is never replaced or reordered by a later response.
                        combined.forEach { creatorCandidate ->
                            publish { current ->
                                val creatorIdentity = RecommendationMetadata.identity(source.id, creatorCandidate)
                                val alreadyVisible = current.creatorWorks.any {
                                    RecommendationMetadata.sameWork(
                                        RecommendationMetadata.identity(source.id, it),
                                        creatorIdentity,
                                    )
                                }
                                val visibleInSimilar = current.similarManga.any {
                                    RecommendationMetadata.sameWork(
                                        RecommendationMetadata.identity(source.id, it),
                                        creatorIdentity,
                                    )
                                }
                                if (
                                    alreadyVisible ||
                                    visibleInSimilar ||
                                    current.creatorWorks.size >= MAX_DISPLAY_RESULTS
                                ) {
                                    current
                                } else {
                                    current.copy(creatorWorks = current.creatorWorks + creatorCandidate)
                                }
                            }
                        }
                        publish { current ->
                            current.copy(creatorAuthoritative = networkCreator.complete)
                        }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        logcat(LogPriority.DEBUG, e) { "Creator recommendation task failed" }
                    } finally {
                        withContext(NonCancellable) {
                            detailBudget.completeReservedPhase(DetailRow.CREATOR)
                        }
                    }
                }

                val similarJob = launch {
                    try {
                        val networkSimilar = loadSimilarManga(
                            source = source,
                            manga = manga,
                            aniListId = aniListId,
                            localPool = localPool,
                            tagProfile = targetTagProfile,
                            documentFrequency = documentFrequency,
                            exposureSnapshot = exposureSnapshot,
                            sourceExposureSnapshot = sourceExposureSnapshot,
                            excludedUrls = sessionExcludedUrls,
                            excludedWorkKeys = sessionExcludedWorkKeys,
                            requestBudget = requestBudget,
                            detailBudget = detailBudget,
                            forceRefresh = forceRefresh,
                            onPartial = { partial ->
                                publish { current ->
                                    val creatorIdentities = current.creatorWorks.map {
                                        RecommendationMetadata.identity(source.id, it)
                                    }
                                    val additions = partial.filterNot { similarCandidate ->
                                        val similarIdentity = RecommendationMetadata.identity(
                                            source.id,
                                            similarCandidate,
                                        )
                                        creatorIdentities.any { creatorIdentity ->
                                            RecommendationMetadata.sameWork(creatorIdentity, similarIdentity)
                                        }
                                    }
                                        .filterNot { similarCandidate ->
                                            val identity = RecommendationMetadata.identity(source.id, similarCandidate)
                                            current.similarManga.any {
                                                RecommendationMetadata.sameWork(
                                                    RecommendationMetadata.identity(source.id, it),
                                                    identity,
                                                )
                                            }
                                        }
                                    current.copy(
                                        similarManga = (current.similarManga + additions)
                                            .take(MAX_DISPLAY_RESULTS),
                                        similarAuthoritative = false,
                                    )
                                }
                            },
                        )
                        val combined = networkSimilar.manga.take(MAX_INTERNAL_RESULTS)
                        publish { current ->
                            val creatorIdentities = current.creatorWorks.map {
                                RecommendationMetadata.identity(source.id, it)
                            }
                            val additions = combined.filterNot { similarCandidate ->
                                val similarIdentity = RecommendationMetadata.identity(source.id, similarCandidate)
                                creatorIdentities.any { creatorIdentity ->
                                    RecommendationMetadata.sameWork(creatorIdentity, similarIdentity)
                                }
                            }
                                .filterNot { similarCandidate ->
                                    val identity = RecommendationMetadata.identity(source.id, similarCandidate)
                                    current.similarManga.any {
                                        RecommendationMetadata.sameWork(
                                            RecommendationMetadata.identity(source.id, it),
                                            identity,
                                        )
                                    }
                                }
                            current.copy(
                                similarManga = (current.similarManga + additions)
                                    .take(MAX_DISPLAY_RESULTS),
                                similarAuthoritative = networkSimilar.complete,
                                retryAtMillis = networkSimilar.retryAtMillis,
                            )
                        }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        logcat(LogPriority.DEBUG, e) { "Similar recommendation task failed" }
                    } finally {
                        withContext(NonCancellable) {
                            detailBudget.completeReservedPhase(DetailRow.SIMILAR)
                            if (!similarNetworkComplete.isCompleted) similarNetworkComplete.complete(Unit)
                        }
                    }
                }

                joinAll(creatorJob, similarJob)
            }
        }
        publish { current ->
            val sourceRetryAt = requestBudget.retryAtMillis()
            current.copy(
                retryAtMillis = sourceRetryAt ?: current.retryAtMillis,
                isFinal = true,
            )
        }
    }

    internal fun recordShown(sourceId: Long, target: SManga, manga: List<SManga>) {
        val targetKey = RecommendationMetadata.identity(sourceId, target).exposureKey
        val works = manga.map { RecommendationMetadata.identity(sourceId, it).exposureKeys }
        exposureStore.recordShownWorks(
            sourceId = sourceId,
            works = works,
            targetKey = targetKey,
        )
        exposureStore.recordShownWorks(sourceId = sourceId, works = works)
    }

    private suspend fun loadCreatorWorks(
        source: Source,
        manga: SManga,
        creators: List<CreatorIdentity>,
        requestBudget: SourceRequestBudget,
        detailBudget: SharedDetailBudget,
        forceRefresh: Boolean,
        knownByUrl: Map<String, SManga>,
        fallbackStart: CompletableDeferred<Unit>,
        prioritizeSimilarRequests: Boolean,
    ): RowLoadOutcome {
        if (creators.isEmpty()) return RowLoadOutcome(emptyList(), complete = true)
        val complete = AtomicBoolean(true)
        val currentKey = recommendationKey(manga)
        val targetCreators = creators.mapTo(linkedSetOf(), CreatorIdentity::normalizedName)
        val routedResults = coroutineScope {
            creators.take(MAX_CREATOR_SEARCHES).map { creator ->
                async {
                    val filters = freshFilters(source) { complete.set(false) }
                        ?: return@async emptyList()
                    val filterMatch = applyExactCreatorTextFilter(filters, creator)
                    if (prioritizeSimilarRequests) {
                        detailBudget.completeReservedPhase(DetailRow.CREATOR)
                        fallbackStart.await()
                    }
                    val query = if (filterMatch == null) creator.displayName else ""
                    loadSearchRoute(
                        source = source,
                        query = query,
                        filters = filters,
                        requestName = "creator-search",
                        requestBudget = requestBudget,
                        onFailure = { complete.set(false) },
                        timeoutMillis = if (filterMatch == null) {
                            SOURCE_CALL_TIMEOUT_MS
                        } else {
                            TEXT_FILTER_SOURCE_CALL_TIMEOUT_MS
                        },
                        allowAfterSoftDeadline = filterMatch != null,
                        isTextFilterRoute = filterMatch != null,
                    )
                        .let(::randomizeFreshCandidates)
                }
            }.awaitAll().flatten()
        }
        val searchResults = linkedMapOf<String, SManga>()
        routedResults.forEach { routeManga ->
            val key = recommendationKey(routeManga)
            if (key.isBlank()) return@forEach
            val existing = searchResults[key]
            searchResults[key] = if (existing == null) {
                routeManga
            } else {
                richerManga(existing, routeManga)
            }
        }

        val verified = mutableListOf<SManga>()
        val unresolved = mutableListOf<SManga>()
        for ((_, routeManga) in searchResults) {
            val key = recommendationKey(routeManga)
            if (key.isBlank() || key == currentKey) continue
            val candidate = knownByUrl[key]?.let { mergeIndexedMetadata(routeManga, it) } ?: routeManga
            val candidateCreators = RecommendationMetadata.extractCreators(candidate)
            if (
                RecommendationMetadata.hasExactCreator(candidate, targetCreators)
            ) {
                verified += candidate
            } else if (candidateCreators.isEmpty()) {
                unresolved += candidate
            }
            if (verified.size >= MAX_INTERNAL_RESULTS) break
        }

        var unresolvedIndex = 0
        suspend fun takeHydrationBatch(acquire: suspend () -> Boolean): List<SManga> {
            val batch = mutableListOf<SManga>()
            while (
                unresolvedIndex < unresolved.size &&
                verified.size + batch.size < MAX_INTERNAL_RESULTS &&
                batch.size < MAX_SOURCE_CONCURRENCY &&
                acquire()
            ) {
                batch += unresolved[unresolvedIndex++]
            }
            return batch
        }

        suspend fun hydrateBatch(batch: List<SManga>) {
            val resolved = coroutineScope {
                batch.map { candidate ->
                    async {
                        loadDetails(
                            source = source,
                            identity = candidate,
                            requestBudget = requestBudget,
                            onFailure = { complete.set(false) },
                            forceRefresh = forceRefresh,
                        )?.takeIf { RecommendationMetadata.hasExactCreator(it, targetCreators) }
                    }
                }.awaitAll().filterNotNull()
            }
            verified += resolved
        }

        hydrateBatch(takeHydrationBatch { detailBudget.tryAcquireReserved(DetailRow.CREATOR) })
        detailBudget.completeReservedPhase(DetailRow.CREATOR)
        while (unresolvedIndex < unresolved.size && verified.size < MAX_INTERNAL_RESULTS) {
            val batch = takeHydrationBatch { detailBudget.tryAcquireShared(DetailRow.CREATOR) }
            if (batch.isEmpty()) break
            hydrateBatch(batch)
        }
        val hydrationComplete = unresolvedIndex >= unresolved.size
        return RowLoadOutcome(
            manga = verified.distinctBy(::recommendationKey).take(MAX_INTERNAL_RESULTS),
            complete = verified.size >= MAX_DISPLAY_RESULTS || (complete.get() && hydrationComplete),
        )
    }

    private suspend fun loadSimilarManga(
        source: Source,
        manga: SManga,
        aniListId: Long?,
        localPool: List<SManga>,
        tagProfile: TagProfile,
        documentFrequency: Map<String, Int>,
        exposureSnapshot: RecommendationExposureSnapshot,
        sourceExposureSnapshot: RecommendationExposureSnapshot,
        excludedUrls: Set<String>,
        excludedWorkKeys: Set<String>,
        requestBudget: SourceRequestBudget,
        detailBudget: SharedDetailBudget,
        forceRefresh: Boolean,
        onPartial: suspend (List<SManga>) -> Unit,
    ): RowLoadOutcome {
        val complete = AtomicBoolean(true)
        val currentUrl = RecommendationMetadata.safeUrl(manga)
        val sourceExposureRound = sourceExposureSnapshot.leastRecentlyShownWorksFirst.size / MAX_DISPLAY_RESULTS
        val routingSeed = "$currentUrl|$sourceExposureRound"
        val localByUrl = localPool.associateBy(::recommendationKey)
        val candidates = linkedMapOf<String, SimilarCandidate>()
        val randomPriorities = mutableMapOf<String, Double>()
        val publishedCandidates = mutableListOf<SManga>()
        val targetIdentity = RecommendationMetadata.identity(source.id, manga)
        val routeGenres = tagProfile.routeIdentities
            .mapTo(linkedSetOf(), GenreIdentity::normalizedName)
        val excludedUrlValues = excludedUrls.filter(String::isNotBlank)
        fun workKeys(item: SManga): Set<String> =
            RecommendationMetadata.identity(source.id, item).let { identity ->
                identity.exposureKeys.ifEmpty { setOf(identity.exposureKey) }
            }

        fun rankCandidates(): List<SManga> {
            val ranked = RecommendationRanking.scoreCandidates(
                profile = tagProfile,
                candidates = candidates.values,
                documentFrequency = documentFrequency,
                documentCount = localPool.size,
            )
            val distinct = distinctRankedWorks(
                sourceId = source.id,
                ranked = ranked.filterNot {
                    val identity = RecommendationMetadata.identity(source.id, it.manga)
                    RecommendationMetadata.sameWork(targetIdentity, identity) ||
                        excludedUrlValues.any { excludedUrl ->
                            RecommendationMetadata.sameRecommendationUrl(
                                excludedUrl,
                                RecommendationMetadata.safeUrl(it.manga),
                            )
                        } ||
                        identity.exposureKeys.any(excludedWorkKeys::contains)
                },
            )
            return RecommendationSampler.sample(
                candidates = distinct,
                maxResults = MAX_SAMPLED_RESULTS,
                unseenTargetSize = MAX_DISPLAY_RESULTS,
                maxPoolSize = MAX_INTERNAL_RESULTS,
                exposureSnapshot = exposureSnapshot,
                sourceExposureSnapshot = sourceExposureSnapshot,
                randomPriorities = randomPriorities,
                pinnedWorkKeys = publishedCandidates.map(::workKeys),
                workKeys = ::workKeys,
                nextRandomDouble = random::nextDouble,
            )
        }

        fun appendPublishedCandidates(latest: List<SManga>): List<List<SManga>> {
            if (publishedCandidates.size >= MAX_DISPLAY_RESULTS) return emptyList()
            val snapshots = mutableListOf<List<SManga>>()
            latest.forEach { candidate ->
                if (publishedCandidates.size >= MAX_DISPLAY_RESULTS) return@forEach
                val candidateIdentity = RecommendationMetadata.identity(source.id, candidate)
                val alreadyPublished = publishedCandidates.any {
                    RecommendationMetadata.sameWork(
                        RecommendationMetadata.identity(source.id, it),
                        candidateIdentity,
                    )
                }
                if (!alreadyPublished) {
                    publishedCandidates += candidate
                    snapshots += publishedCandidates.toList()
                }
            }
            return snapshots
        }

        suspend fun publishRankedCandidates() {
            appendPublishedCandidates(rankCandidates()).forEach { snapshot ->
                onPartial(snapshot)
            }
        }

        fun retryAtMillis(): Long? = requestBudget.retryAtMillis()

        requestBudget.retryAtMillis()?.let { retryAt ->
            publishRankedCandidates()
            return RowLoadOutcome(
                manga = publishedCandidates.toList(),
                complete = false,
                retryAtMillis = retryAt,
            )
        }

        // With no content evidence, popular is the same pool for every target and is not a
        // similarity recommendation. Creator results are handled by their own row.
        if (tagProfile.allTags.isEmpty() && aniListId == null) {
            return RowLoadOutcome(
                emptyList(),
                complete = complete.get(),
                retryAtMillis = retryAtMillis(),
            )
        }

        val isEhentai = source.isEhentaiRecommendationSource()
        if (isEhentai) {
            collectEhentaiCombinedCandidates(
                source = source,
                targetGenreIdentities = tagProfile.routeIdentities,
                documentFrequency = documentFrequency,
                localByUrl = localByUrl,
                candidates = candidates,
                currentUrl = currentUrl,
                routeSeed = routingSeed,
                requestBudget = requestBudget,
                onFailure = { complete.set(false) },
                onRouteCompleted = {
                    publishRankedCandidates()
                },
            )
        } else {
            collectCombinedStructuredCandidates(
                source = source,
                targetGenreIdentities = tagProfile.routeIdentities,
                documentFrequency = documentFrequency,
                localByUrl = localByUrl,
                candidates = candidates,
                currentUrl = currentUrl,
                routeSeed = routingSeed,
                requestBudget = requestBudget,
                onFailure = { complete.set(false) },
                onRouteCompleted = {
                    publishRankedCandidates()
                },
            )
            if (rankCandidates().size < MAX_DISPLAY_RESULTS) {
                collectExactGenreCandidates(
                    source = source,
                    targetGenres = routeGenres,
                    targetGenreIdentities = tagProfile.routeIdentities,
                    documentFrequency = documentFrequency,
                    localByUrl = localByUrl,
                    candidates = candidates,
                    currentUrl = currentUrl,
                    routeSeed = routingSeed,
                    maxGenreRoutes = MAX_GENRE_ROUTES,
                    requestBudget = requestBudget,
                    onFailure = { complete.set(false) },
                    onRouteCompleted = {
                        publishRankedCandidates()
                    },
                )
            }
        }
        val firstFullBatch = rankCandidates()
        if (firstFullBatch.size >= MAX_DISPLAY_RESULTS) {
            publishRankedCandidates()
            return RowLoadOutcome(
                manga = publishedCandidates.toList(),
                complete = true,
            )
        }
        if (aniListId != null) {
            val recommendations = try {
                withTimeoutOrNull(EXTERNAL_TIMEOUT_MS) { aniListLoader(aniListId) }
                    ?: emptyList<ALRecommendation>().also { complete.set(false) }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                complete.set(false)
                logcat(LogPriority.DEBUG, e) { "AniList recommendation lookup failed" }
                emptyList()
            }
            mapAniListRecommendations(
                source = source,
                targetManga = manga,
                recommendations = recommendations,
                localByUrl = localByUrl,
                candidates = candidates,
                currentUrl = currentUrl,
                requestBudget = requestBudget,
                onFailure = { complete.set(false) },
            )
        }
        if (aniListId == null) {
            val reliableAfterExact = rankCandidates()
            // Do not pad a short, target-specific row with the source's generic popular pool.
            // A broader tag query is still target-derived and its candidates require tag evidence.
            if (!isEhentai && reliableAfterExact.size < MAX_DISPLAY_RESULTS) {
                collectTagQueryCandidates(
                    source = source,
                    targetGenres = routeGenres,
                    targetGenreIdentities = tagProfile.routeIdentities,
                    documentFrequency = documentFrequency,
                    localByUrl = localByUrl,
                    candidates = candidates,
                    currentUrl = currentUrl,
                    routeSeed = routingSeed,
                    requestBudget = requestBudget,
                    onFailure = { complete.set(false) },
                )
            }
        }

        publishRankedCandidates()

        val hydrationComplete = hydrateSimilarCandidates(
            source = source,
            candidates = candidates,
            localByUrl = localByUrl,
            requestBudget = requestBudget,
            detailBudget = detailBudget,
            onFailure = { complete.set(false) },
            forceRefresh = forceRefresh,
            onBatchCompleted = {
                publishRankedCandidates()
            },
        )
        publishRankedCandidates()
        val filled = publishedCandidates.toList()
        if (!hydrationComplete && filled.size < MAX_DISPLAY_RESULTS) complete.set(false)
        return RowLoadOutcome(
            manga = filled,
            complete = filled.size >= MAX_DISPLAY_RESULTS || complete.get(),
            retryAtMillis = retryAtMillis().takeIf { filled.size < MAX_DISPLAY_RESULTS },
        )
    }

    private suspend fun collectEhentaiCombinedCandidates(
        source: Source,
        targetGenreIdentities: List<GenreIdentity>,
        documentFrequency: Map<String, Int>,
        localByUrl: Map<String, SManga>,
        candidates: MutableMap<String, SimilarCandidate>,
        currentUrl: String,
        routeSeed: String,
        requestBudget: SourceRequestBudget,
        onFailure: () -> Unit,
        onRouteCompleted: suspend () -> Unit,
    ) {
        val identitiesByGenre = targetGenreIdentities.associateBy(GenreIdentity::normalizedName)
        val ordered = RecommendationRanking.routeGenres(
            identitiesByGenre.keys,
            documentFrequency,
            routeSeed,
        )
            .mapNotNull(identitiesByGenre::get)
            .take(TagProfile.MAX_CORE_TAGS)
        if (ordered.isEmpty()) return
        val routes = when (ordered.size) {
            1 -> listOf(listOf(ordered[0]))
            2 -> listOf(listOf(ordered[0], ordered[1]))
            else -> listOf(
                listOf(ordered[0], ordered[1]),
                listOf(ordered[0], ordered[2]),
            )
        }
        routes.take(MAX_EHENTAI_COMBINED_ROUTES).forEachIndexed { routeIndex, route ->
            val query = RecommendationMetadata.ehentaiExactTagQuery(route)
            if (query.isBlank()) return@forEachIndexed
            val filters = freshFilters(source, onFailure) ?: return@forEachIndexed
            val results = loadSearchRoute(
                source = source,
                query = query,
                filters = filters,
                requestName = "ehentai-combined-tags",
                requestBudget = requestBudget,
                onFailure = onFailure,
                allowAfterSoftDeadline = true,
                preferredPage = recommendationPage(
                    targetKey = routeSeed,
                    routeKey = "ehentai:$query",
                    maxPage = 4,
                ),
                minimumCandidatePool = MIN_QUALITY_POOL_SIZE,
            )
            val queriedGenres = route.mapTo(linkedSetOf(), GenreIdentity::normalizedName)
            var acceptedCount = 0
            results.take(MAX_CANDIDATES_PER_GENRE_ROUTE).forEachIndexed { index, item ->
                val resolved = localByUrl[recommendationKey(item)]
                    ?.let { mergeIndexedMetadata(item, it) }
                    ?: item
                mergeCandidate(
                    candidates = candidates,
                    manga = resolved,
                    evidence = CandidateEvidence(
                        genreSearchRank = routeIndex * MAX_CANDIDATES_PER_GENRE_ROUTE + index,
                        strongRouteGenres = queriedGenres,
                    ),
                    currentUrl = currentUrl,
                    sourceId = source.id,
                )?.let { acceptedCount += 1 }
            }
            if (results.isNotEmpty()) onRouteCompleted()
            if (acceptedCount >= MAX_DISPLAY_RESULTS) return
        }
    }

    private suspend fun collectCombinedStructuredCandidates(
        source: Source,
        targetGenreIdentities: List<GenreIdentity>,
        documentFrequency: Map<String, Int>,
        localByUrl: Map<String, SManga>,
        candidates: MutableMap<String, SimilarCandidate>,
        currentUrl: String,
        routeSeed: String,
        requestBudget: SourceRequestBudget,
        onFailure: () -> Unit,
        onRouteCompleted: suspend () -> Unit,
    ): Int {
        val identitiesByGenre = targetGenreIdentities.associateBy(GenreIdentity::normalizedName)
        val route = RecommendationRanking.routeGenres(
            identitiesByGenre.keys,
            documentFrequency,
            routeSeed,
        )
            .mapNotNull(identitiesByGenre::get)
            .take(MAX_COMBINED_STRUCTURED_TAGS)
        if (route.size < 2) return 0
        val filters = freshGenreFilters(source, onFailure) ?: return 0
        if (!applyCombinedStructuredGenreFilters(filters, route)) return 0
        preferFreshRecommendationSort(filters)
        val results = loadSearchRoute(
            source = source,
            query = "",
            filters = filters,
            requestName = "combined-genre-search",
            requestBudget = requestBudget,
            onFailure = onFailure,
            allowAfterSoftDeadline = true,
            preferredPage = recommendationPage(
                targetKey = routeSeed,
                routeKey = "combined:${route.joinToString { it.normalizedName }}",
                maxPage = 4,
            ),
            minimumCandidatePool = MIN_QUALITY_POOL_SIZE,
        )
        val routeGenres = route.mapTo(linkedSetOf(), GenreIdentity::normalizedName)
        val acceptedKeys = linkedSetOf<String>()
        results.take(MAX_CANDIDATES_PER_GENRE_ROUTE).forEachIndexed { index, item ->
            val resolved = localByUrl[recommendationKey(item)]
                ?.let { mergeIndexedMetadata(item, it) }
                ?: item
            mergeCandidate(
                candidates = candidates,
                manga = resolved,
                evidence = CandidateEvidence(
                    genreSearchRank = index,
                    strongRouteGenres = routeGenres,
                ),
                currentUrl = currentUrl,
                sourceId = source.id,
            )?.let(acceptedKeys::add)
        }
        if (acceptedKeys.isNotEmpty()) onRouteCompleted()
        return acceptedKeys.size
    }

    private suspend fun mapAniListRecommendations(
        source: Source,
        targetManga: SManga,
        recommendations: List<ALRecommendation>,
        localByUrl: Map<String, SManga>,
        candidates: MutableMap<String, SimilarCandidate>,
        currentUrl: String,
        requestBudget: SourceRequestBudget,
        onFailure: () -> Unit,
    ) {
        coroutineScope {
            recommendations.take(MAX_ANILIST_RESULTS).mapIndexed { index, recommendation ->
                async {
                    val variants = recommendation.media.title.variants(recommendation.media.synonyms)
                    val query = selectTitleVariant(RecommendationMetadata.safeTitle(targetManga), variants)
                        ?: return@async
                    val filters = freshFilters(source, onFailure) ?: return@async
                    val results = loadSearchRoute(
                        source = source,
                        query = query,
                        filters = filters,
                        requestName = "anilist-map",
                        requestBudget = requestBudget,
                        onFailure = onFailure,
                        allowAfterSoftDeadline = true,
                    )
                    val normalizedVariants = variants.mapTo(linkedSetOf(), RecommendationMetadata::normalizeTitle)
                    val exact = results
                        .filter {
                            RecommendationMetadata.normalizeTitle(RecommendationMetadata.safeTitle(it)) in
                                normalizedVariants
                        }
                        .distinctBy(::recommendationKey)
                    if (exact.size != 1) return@async
                    val item = exact.single()
                    val resolved = localByUrl[recommendationKey(item)]
                        ?.let { mergeIndexedMetadata(item, it) }
                        ?: item
                    val externalGenres = buildSet {
                        recommendation.media.genres.flatMapTo(this, RecommendationMetadata::normalizeGenres)
                        recommendation.media.tags
                            .filter { it.rank >= MIN_ANILIST_TAG_RANK }
                            .flatMapTo(this) { RecommendationMetadata.normalizeGenres(it.name) }
                    }.filterTo(linkedSetOf(), RecommendationMetadata::isInformativeGenre)
                    synchronized(candidates) {
                        mergeCandidate(
                            candidates,
                            resolved,
                            CandidateEvidence(
                                aniListRank = index,
                                externalGenres = externalGenres,
                            ),
                            currentUrl,
                            source.id,
                        )
                    }
                }
            }.awaitAll()
        }
    }

    private suspend fun collectExactGenreCandidates(
        source: Source,
        targetGenres: Set<String>,
        targetGenreIdentities: List<GenreIdentity>,
        documentFrequency: Map<String, Int>,
        localByUrl: Map<String, SManga>,
        candidates: MutableMap<String, SimilarCandidate>,
        currentUrl: String,
        routeSeed: String,
        maxGenreRoutes: Int,
        requestBudget: SourceRequestBudget,
        onFailure: () -> Unit,
        onRouteCompleted: suspend () -> Unit,
    ) {
        val identitiesByGenre = targetGenreIdentities.associateBy(GenreIdentity::normalizedName)
        var routeIndex = 0
        val routedDisplayNames = hashSetOf<String>()
        for (routeGenre in RecommendationRanking.routeGenres(targetGenres, documentFrequency, routeSeed)) {
            if (routeIndex >= maxGenreRoutes) break
            val identity = identitiesByGenre[routeGenre] ?: continue
            val displayKey = RecommendationMetadata.normalize(identity.displayName)
            if (!routedDisplayNames.add(displayKey)) continue
            val routeCanonicalGenres = targetGenreIdentities
                .filter { RecommendationMetadata.normalize(it.displayName) == displayKey }
                .mapTo(linkedSetOf(), GenreIdentity::normalizedName)
            val filters = freshGenreFilters(source, onFailure) ?: continue
            val filterKind = applyGenreFilter(filters, identity)
            if (filterKind == GenreFilterKind.NONE) continue
            preferFreshRecommendationSort(filters)
            val strongEvidence = filterKind.isStrongEvidence
            val results = loadSearchRoute(
                source = source,
                query = "",
                filters = filters,
                requestName = "genre-search",
                requestBudget = requestBudget,
                onFailure = onFailure,
                timeoutMillis = if (filterKind.isText) {
                    TEXT_FILTER_SOURCE_CALL_TIMEOUT_MS
                } else {
                    SOURCE_CALL_TIMEOUT_MS
                },
                allowAfterSoftDeadline = true,
                isTextFilterRoute = filterKind.isText,
                preferredPage = recommendationPage(
                    targetKey = routeSeed,
                    routeKey = "genre:${identity.normalizedName}",
                    maxPage = if (routeIndex > 0) 1 else 4,
                ),
                minimumCandidatePool = MIN_QUALITY_POOL_SIZE.takeIf { strongEvidence } ?: 0,
            )
            val routeResults = results
                .filter { item ->
                    if (!strongEvidence && !filterKind.isText) return@filter true
                    val itemGenres = RecommendationMetadata.extractGenres(item)
                    itemGenres.isEmpty() || itemGenres.any(routeCanonicalGenres::contains)
                }
                .take(if (strongEvidence) MAX_CANDIDATES_PER_GENRE_ROUTE else MAX_CANDIDATES_PER_REQUEST)
            val acceptedKeys = linkedSetOf<String>()
            routeResults.forEachIndexed { index, item ->
                val resolved = localByUrl[recommendationKey(item)]
                    ?.let { mergeIndexedMetadata(item, it) }
                    ?: item
                mergeCandidate(
                    candidates = candidates,
                    manga = resolved,
                    evidence = CandidateEvidence(
                        genreSearchRank = (routeIndex * MAX_CANDIDATES_PER_REQUEST + index)
                            .takeIf { strongEvidence },
                        queryRank = (routeIndex * MAX_CANDIDATES_PER_REQUEST + index)
                            .takeUnless { strongEvidence },
                        strongRouteGenres = routeCanonicalGenres.takeIf { strongEvidence }.orEmpty(),
                        queriedGenres = routeCanonicalGenres.takeUnless { strongEvidence }.orEmpty(),
                    ),
                    currentUrl = currentUrl,
                    sourceId = source.id,
                )?.let(acceptedKeys::add)
            }
            routeIndex += 1
            val usableRouteResults = acceptedKeys.size
            // Weak text/category routes need detail verification. Publishing here would show only
            // locally initialized (usually older) cards while fresh cards are still hydrating.
            if (usableRouteResults > 0 && strongEvidence) onRouteCompleted()
        }
    }

    private suspend fun collectTagQueryCandidates(
        source: Source,
        targetGenres: Set<String>,
        targetGenreIdentities: List<GenreIdentity>,
        documentFrequency: Map<String, Int>,
        localByUrl: Map<String, SManga>,
        candidates: MutableMap<String, SimilarCandidate>,
        currentUrl: String,
        routeSeed: String,
        requestBudget: SourceRequestBudget,
        onFailure: () -> Unit,
    ) {
        val identitiesByGenre = targetGenreIdentities.associateBy(GenreIdentity::normalizedName)
        val routeGenre = RecommendationRanking.routeGenres(targetGenres, documentFrequency, routeSeed)
            .firstOrNull() ?: return
        val identity = identitiesByGenre[routeGenre] ?: return
        val filters = freshFilters(source, onFailure) ?: return
        val results = loadSearchRoute(
            source = source,
            query = identity.displayName,
            filters = filters,
            requestName = "tag-query",
            requestBudget = requestBudget,
            onFailure = onFailure,
            preferredPage = recommendationPage(
                targetKey = routeSeed,
                routeKey = "query:${identity.normalizedName}",
                maxPage = 4,
            ),
        )
        results.take(MAX_CANDIDATES_PER_REQUEST)
            .forEachIndexed { index, item ->
                val resolved = localByUrl[recommendationKey(item)]
                    ?.let { mergeIndexedMetadata(item, it) }
                    ?: item
                mergeCandidate(
                    candidates = candidates,
                    manga = resolved,
                    evidence = CandidateEvidence(
                        queryRank = index,
                        queriedGenres = setOf(routeGenre),
                    ),
                    currentUrl = currentUrl,
                    sourceId = source.id,
                )
            }
    }

    private suspend fun hydrateSimilarCandidates(
        source: Source,
        candidates: MutableMap<String, SimilarCandidate>,
        localByUrl: Map<String, SManga>,
        requestBudget: SourceRequestBudget,
        detailBudget: SharedDetailBudget,
        onFailure: () -> Unit,
        forceRefresh: Boolean,
        onBatchCompleted: suspend () -> Unit,
    ): Boolean {
        val unresolved = candidates.values
            .filter {
                RecommendationMetadata.extractGenres(it.manga).isEmpty() &&
                    it.evidence.externalGenres.isEmpty() &&
                    it.evidence.strongRouteGenres.isEmpty() &&
                    !it.evidence.hasAniListEvidence
            }
            .filterNot { recommendationKey(it.manga) in localByUrl }
            .sortedWith(
                compareBy<SimilarCandidate> { it.evidence.aniListRank ?: Int.MAX_VALUE }
                    .thenBy { it.evidence.genreSearchRank ?: Int.MAX_VALUE }
                    .thenBy { it.evidence.queryRank ?: Int.MAX_VALUE }
                    .thenBy { it.evidence.popularRank ?: Int.MAX_VALUE },
            )
        val toHydrate = mutableListOf<SimilarCandidate>()
        unresolved.forEach { candidate ->
            val cached = if (forceRefresh) {
                null
            } else {
                detailCache.get(
                    DetailCacheKey(
                        sourceId = source.id,
                        mangaUrl = recommendationKey(candidate.manga),
                    ),
                    now(),
                )
            }
            if (cached == null) {
                toHydrate += candidate
            } else {
                candidates[recommendationKey(cached)] = candidate.copy(manga = cached)
            }
        }
        var hydrateIndex = 0
        suspend fun takeHydrationBatch(acquire: suspend () -> Boolean): List<SimilarCandidate> {
            val batch = mutableListOf<SimilarCandidate>()
            while (
                hydrateIndex < toHydrate.size &&
                batch.size < MAX_SOURCE_CONCURRENCY &&
                acquire()
            ) {
                batch += toHydrate[hydrateIndex++]
            }
            return batch
        }

        suspend fun hydrateBatch(batch: List<SimilarCandidate>) {
            val hydrated = coroutineScope {
                batch.map { candidate ->
                    async {
                        candidate to loadDetails(
                            source = source,
                            identity = candidate.manga,
                            requestBudget = requestBudget,
                            onFailure = onFailure,
                            forceRefresh = forceRefresh,
                        )
                    }
                }.awaitAll()
            }
            hydrated.forEach { (candidate, detailed) ->
                if (detailed == null) return@forEach
                candidates[recommendationKey(detailed)] = candidate.copy(manga = detailed)
            }
            if (batch.isNotEmpty()) onBatchCompleted()
        }

        hydrateBatch(takeHydrationBatch { detailBudget.tryAcquireReserved(DetailRow.SIMILAR) })
        detailBudget.completeReservedPhase(DetailRow.SIMILAR)
        while (hydrateIndex < toHydrate.size) {
            val batch = takeHydrationBatch { detailBudget.tryAcquireShared(DetailRow.SIMILAR) }
            if (batch.isEmpty()) break
            hydrateBatch(batch)
        }
        return hydrateIndex >= toHydrate.size
    }

    private suspend fun loadDetails(
        source: Source,
        identity: SManga,
        requestBudget: SourceRequestBudget,
        onFailure: () -> Unit,
        forceRefresh: Boolean,
    ): SManga? {
        val mangaUrl = recommendationKey(identity)
        val cacheKey = DetailCacheKey(source.id, mangaUrl)
        if (!forceRefresh && mangaUrl.isNotBlank()) {
            detailCache.get(cacheKey, now())?.let { return it }
        }
        val merged = requestBudget.call("details", onFailure = onFailure) {
            source.getMangaUpdate(
                manga = identity.copy(),
                chapters = emptyList(),
                fetchDetails = true,
                fetchChapters = false,
            ).manga
        }?.let { details -> mergeDetails(identity, details) }
        if (merged != null && mangaUrl.isNotBlank()) detailCache.put(cacheKey, merged, now())
        return merged
    }

    private fun freshFilters(source: Source, onFailure: () -> Unit): FilterList? {
        return try {
            source.getFilterList()
        } catch (e: Exception) {
            onFailure()
            logcat(LogPriority.DEBUG, e) { "Unable to obtain source filters for recommendations" }
            null
        }
    }

    private suspend fun freshGenreFilters(source: Source, onFailure: () -> Unit): FilterList? {
        var filters = freshFilters(source, onFailure) ?: return null
        for (delayMillis in DEFERRED_FILTER_RETRY_DELAYS_MS) {
            if (!hasDeferredGenreFilterMarker(filters)) return filters
            delay(delayMillis)
            filters = freshFilters(source, onFailure) ?: return filters
        }
        return filters
    }

    private suspend fun loadSearchRoute(
        source: Source,
        query: String,
        filters: FilterList,
        requestName: String,
        requestBudget: SourceRequestBudget,
        onFailure: () -> Unit,
        timeoutMillis: Long = SOURCE_CALL_TIMEOUT_MS,
        allowAfterSoftDeadline: Boolean = false,
        isTextFilterRoute: Boolean = false,
        preferredPage: Int = 1,
        minimumCandidatePool: Int = 0,
    ): List<SManga> {
        if (isTextFilterRoute && !requestBudget.tryAcquireTextFilterRoute()) {
            onFailure()
            return emptyList()
        }
        suspend fun load(pageNumber: Int) = requestBudget.call(
            name = if (pageNumber == 1) requestName else "$requestName-page-$pageNumber",
            timeoutMillis = timeoutMillis,
            onFailure = onFailure,
            allowAfterSoftDeadline = allowAfterSoftDeadline,
        ) {
            source.getSearchManga(pageNumber, query, filters)
        }

        val selectedPage = preferredPage.coerceAtLeast(1)
        val selected = load(selectedPage) ?: return emptyList()
        if (selected.mangas.isEmpty() && selectedPage > 1) {
            // A source may expose only its first page for a particular filter. Fall back once
            // instead of spending two calls on every route and starving detail verification.
            return load(1)?.mangas.orEmpty().take(MAX_SOURCE_RESULT_SAMPLE)
        }
        if (
            minimumCandidatePool <= 0 ||
            selected.mangas.size >= minimumCandidatePool ||
            !selected.hasNextPage
        ) {
            return selected.mangas.take(MAX_SOURCE_RESULT_SAMPLE)
        }

        // Preserve recommendation quality by expanding the exact same route, never by mixing a
        // broader tag or popular pool merely to reach the randomization target.
        val next = load(selectedPage + 1)
        return (selected.mangas + next?.mangas.orEmpty())
            .distinctBy(::recommendationKey)
            .take(MAX_SOURCE_RESULT_SAMPLE)
    }

    private fun recommendationPage(targetKey: String, routeKey: String, maxPage: Int): Int {
        if (maxPage <= 1) return 1
        return Math.floorMod("$targetKey\u0000$routeKey".hashCode(), maxPage) + 1
    }

    private fun mergeDetails(identity: SManga, details: SManga): SManga {
        return identity.copy().also { merged ->
            merged.url = RecommendationMetadata.safeUrl(identity)
            merged.title = RecommendationMetadata.safeTitle(identity)
            details.author?.takeIf(String::isNotBlank)?.let { merged.author = it }
            details.artist?.takeIf(String::isNotBlank)?.let { merged.artist = it }
            details.description?.takeIf(String::isNotBlank)?.let { merged.description = it }
            details.genre?.takeIf(String::isNotBlank)?.let { merged.genre = it }
            details.thumbnail_url?.takeIf(String::isNotBlank)?.let { merged.thumbnail_url = it }
            if (details.status != SManga.UNKNOWN) merged.status = details.status
            merged.update_strategy = details.update_strategy
            merged.memo = details.memo
            merged.initialized = true
        }
    }

    private fun mergeCandidate(
        candidates: MutableMap<String, SimilarCandidate>,
        manga: SManga,
        evidence: CandidateEvidence,
        currentUrl: String,
        sourceId: Long,
    ): String? {
        if (isBlockedRecommendation(manga)) return null
        val key = recommendationKey(manga)
        val currentKey = RecommendationMetadata.recommendationUrlKey(currentUrl)
        if (key.isBlank() || key == currentKey) return null
        val identity = RecommendationMetadata.identity(sourceId, manga)
        val matchingEntry = candidates.entries.firstOrNull { (_, candidate) ->
            RecommendationMetadata.sameWork(
                RecommendationMetadata.identity(sourceId, candidate.manga),
                identity,
            )
        }
        val existing = matchingEntry?.value
        val merged = if (existing == null) {
            SimilarCandidate(manga, evidence)
        } else {
            SimilarCandidate(
                manga = richerManga(existing.manga, manga),
                evidence = existing.evidence.merge(evidence),
            )
        }
        matchingEntry?.let { candidates.remove(it.key) }
        val mergedKey = recommendationKey(merged.manga).ifBlank { key }
        candidates[mergedKey] = merged
        return mergedKey
    }

    private fun isBlockedRecommendation(manga: SManga): Boolean {
        return RecommendationKeywordFilter.matches(
            manga = manga,
            terms = recommendationFilterTerms(),
        )
    }

    private fun recommendationFilterTerms(): List<String> {
        val value = recommendationFilterProvider()
        keywordFilterCache?.takeIf { it.first == value }?.let { return it.second }
        return synchronized(keywordFilterCacheLock) {
            keywordFilterCache?.takeIf { it.first == value }?.second
                ?: RecommendationKeywordFilter.parse(value).also { terms ->
                    keywordFilterCache = value to terms
                }
        }
    }

    private fun richerManga(left: SManga, right: SManga): SManga {
        fun richness(manga: SManga): Int {
            return listOf(
                manga.author,
                manga.artist,
                manga.description,
                manga.genre,
                manga.thumbnail_url,
            ).count { !it.isNullOrBlank() } + if (manga.initialized) 2 else 0
        }
        return if (richness(right) > richness(left)) right else left
    }

    private fun distinctRankedWorks(
        sourceId: Long,
        ranked: List<RankedSimilarCandidate>,
    ): List<RankedSimilarCandidate> {
        val identities = mutableListOf<RecommendationIdentity>()
        return ranked.filter { candidate ->
            val identity = RecommendationMetadata.identity(sourceId, candidate.manga)
            if (identities.any { RecommendationMetadata.sameWork(it, identity) }) {
                false
            } else {
                identities += identity
                true
            }
        }
    }

    /**
     * The database pool may enrich a candidate returned by the current source request, but it
     * must never replace that fresh card's canonical identity or a fresh cover.
     */
    private fun mergeIndexedMetadata(fresh: SManga, indexed: SManga): SManga {
        return fresh.copy().also { merged ->
            if (merged.author.isNullOrBlank()) merged.author = indexed.author
            if (merged.artist.isNullOrBlank()) merged.artist = indexed.artist
            if (merged.description.isNullOrBlank()) merged.description = indexed.description
            if (merged.genre.isNullOrBlank()) merged.genre = indexed.genre
            if (merged.thumbnail_url.isNullOrBlank()) merged.thumbnail_url = indexed.thumbnail_url
            if (merged.status == SManga.UNKNOWN) merged.status = indexed.status
            merged.initialized = fresh.initialized || indexed.initialized
        }
    }

    private fun recommendationKey(manga: SManga): String {
        return RecommendationMetadata.recommendationUrlKey(RecommendationMetadata.safeUrl(manga))
    }

    private fun RecommendationRows.forSession(
        sourceId: Long,
        excludedUrls: Set<String>,
        excludedWorkKeys: Set<String>,
    ): RecommendationRows {
        fun isExcludedUrl(manga: SManga): Boolean {
            val candidateUrl = RecommendationMetadata.safeUrl(manga)
            return excludedUrls.any { excludedUrl ->
                RecommendationMetadata.sameRecommendationUrl(excludedUrl, candidateUrl)
            }
        }
        val creator = creatorWorks
            .filterNot {
                val identity = RecommendationMetadata.identity(sourceId, it)
                isExcludedUrl(it) || identity.exposureKeys.any(excludedWorkKeys::contains)
            }
            .distinctWorks(sourceId)
            .take(MAX_DISPLAY_RESULTS)
        val creatorIdentities = creator.map { RecommendationMetadata.identity(sourceId, it) }
        val similar = similarManga
            .filterNot {
                val identity = RecommendationMetadata.identity(sourceId, it)
                isExcludedUrl(it) ||
                    identity.exposureKeys.any(excludedWorkKeys::contains) ||
                    creatorIdentities.any { creatorIdentity ->
                        RecommendationMetadata.sameWork(creatorIdentity, identity)
                    }
            }
            .distinctWorks(sourceId)
            .take(MAX_DISPLAY_RESULTS)
        return copy(creatorWorks = creator, similarManga = similar)
    }

    private fun List<SManga>.distinctWorks(sourceId: Long): List<SManga> {
        val identities = mutableListOf<RecommendationIdentity>()
        return filter { manga ->
            val identity = RecommendationMetadata.identity(sourceId, manga)
            if (identities.any { RecommendationMetadata.sameWork(it, identity) }) {
                false
            } else {
                identities += identity
                true
            }
        }
    }

    /** Randomize only candidates returned by this observation. */
    private fun randomizeFreshCandidates(manga: List<SManga>): List<SManga> {
        if (manga.size < 2) return manga
        return manga.toMutableList().also { Collections.shuffle(it, random) }
    }

    private fun selectTitleVariant(targetTitle: String, variants: List<String>): String? {
        if (variants.isEmpty()) return null
        val targetScript = dominantScript(targetTitle)
        return variants.firstOrNull { dominantScript(it) == targetScript } ?: variants.first()
    }

    private fun dominantScript(value: String): Character.UnicodeScript? {
        return value.asSequence()
            .filter(Char::isLetter)
            .map { Character.UnicodeScript.of(it.code) }
            .groupingBy { it }
            .eachCount()
            .maxByOrNull(Map.Entry<Character.UnicodeScript, Int>::value)
            ?.key
    }

    private fun Source.isEhentaiRecommendationSource(): Boolean {
        if (this !is HttpSource) return false
        val host = runCatching { URI(baseUrl).host?.lowercase() }.getOrNull() ?: return false
        return host == EHENTAI_HOST || host.endsWith(".$EHENTAI_HOST") ||
            host == EXHENTAI_HOST || host.endsWith(".$EXHENTAI_HOST")
    }

    private enum class DetailRow {
        CREATOR,
        SIMILAR,
    }

    private class SharedDetailBudget(
        private val maxDetails: Int,
        private val reservedPerRow: Int,
    ) {
        private val mutex = Mutex()
        private val reservedUsed = mutableMapOf<DetailRow, Int>()
        private val completedRows = mutableSetOf<DetailRow>()
        private val allReservedPhasesCompleted = CompletableDeferred<Unit>()
        private var totalUsed = 0

        suspend fun tryAcquireReserved(row: DetailRow): Boolean = mutex.withLock {
            if (row in completedRows || totalUsed >= maxDetails) return@withLock false
            val rowUsed = reservedUsed[row] ?: 0
            if (rowUsed >= reservedPerRow) return@withLock false
            reservedUsed[row] = rowUsed + 1
            totalUsed += 1
            true
        }

        suspend fun completeReservedPhase(row: DetailRow) {
            val shouldComplete = mutex.withLock {
                completedRows += row
                completedRows.size == DetailRow.entries.size && !allReservedPhasesCompleted.isCompleted
            }
            if (shouldComplete) allReservedPhasesCompleted.complete(Unit)
        }

        suspend fun tryAcquireShared(row: DetailRow): Boolean {
            allReservedPhasesCompleted.await()
            return mutex.withLock {
                if (totalUsed >= maxDetails) return@withLock false
                val rowUsed = reservedUsed[row] ?: 0
                if (row == DetailRow.CREATOR && rowUsed >= MAX_CREATOR_DETAILS) return@withLock false
                reservedUsed[row] = rowUsed + 1
                totalUsed += 1
                true
            }
        }
    }

    private class SourceRequestBudget(
        private val maxRequests: Int,
        private val semaphore: Semaphore,
        private val maxTextFilterRoutes: Int,
        private val onRequest: (String) -> Unit,
        private val softDeadlineNanos: Long,
        private val monotonicNowNanos: () -> Long,
        private val nowMillis: () -> Long,
        private val requestScheduler: RecommendationRequestScheduler,
        private val sourceId: Long,
        private val sourcePolicyProvider: (Long) -> RecommendationSourcePolicy,
        private val rateLimitRetryAt: () -> Long?,
    ) {
        private val requests = AtomicInteger()
        private val textFilterRoutes = AtomicInteger()
        private val rateLimitedThisRound = AtomicBoolean(false)
        private val roundRetryAtMillis = AtomicLong(0L)

        fun tryAcquireTextFilterRoute(): Boolean {
            while (true) {
                val current = textFilterRoutes.get()
                if (current >= maxTextFilterRoutes) return false
                if (textFilterRoutes.compareAndSet(current, current + 1)) return true
            }
        }

        fun retryAtMillis(): Long? = roundRetryAtMillis.get().takeIf { it > 0L } ?: rateLimitRetryAt()

        suspend fun <T> call(
            name: String,
            timeoutMillis: Long = SOURCE_CALL_TIMEOUT_MS,
            onFailure: () -> Unit = {},
            allowAfterSoftDeadline: Boolean = false,
            block: suspend () -> T,
        ): T? {
            if (!sourcePolicyProvider(sourceId).enabled || rateLimitedThisRound.get()) {
                onFailure()
                return null
            }
            if (retryAtMillis() != null) {
                onFailure()
                return null
            }
            if (!allowAfterSoftDeadline && softDeadlineReached()) {
                onFailure()
                return null
            }
            while (true) {
                val current = requests.get()
                if (current >= maxRequests) {
                    onFailure()
                    return null
                }
                if (requests.compareAndSet(current, current + 1)) break
            }
            return semaphore.withPermit {
                if (!sourcePolicyProvider(sourceId).enabled || rateLimitedThisRound.get()) {
                    onFailure()
                    return@withPermit null
                }
                if (retryAtMillis() != null) {
                    onFailure()
                    return@withPermit null
                }
                if (!allowAfterSoftDeadline && softDeadlineReached()) {
                    onFailure()
                    return@withPermit null
                }
                val intervalMillis = sourcePolicyProvider(sourceId).minRequestIntervalMillis
                requestScheduler.withRatePermit(
                    sourceId = sourceId,
                    minIntervalMillis = intervalMillis,
                    monotonicNowNanos = monotonicNowNanos,
                    nowMillis = nowMillis,
                ) ratePermit@{
                    if (
                        !sourcePolicyProvider(sourceId).enabled ||
                        rateLimitedThisRound.get() ||
                        retryAtMillis() != null
                    ) {
                        onFailure()
                        return@ratePermit null
                    }
                    if (!allowAfterSoftDeadline && softDeadlineReached()) {
                        onFailure()
                        return@ratePermit null
                    }
                    try {
                        onRequest(name)
                        withTimeoutOrNull(timeoutMillis) { block() }.also { result ->
                            if (result == null) {
                                onFailure()
                            } else {
                                if (!rateLimitedThisRound.get()) {
                                    requestScheduler.recordSuccess(sourceId)
                                }
                            }
                        }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        if (
                            e.isRecommendationRateLimited() &&
                            rateLimitedThisRound.compareAndSet(false, true)
                        ) {
                            roundRetryAtMillis.set(
                                requestScheduler.recordRateLimit(
                                    sourceId = sourceId,
                                    nowMillis = nowMillis(),
                                    retryAfterMillis = e.recommendationRetryAfterMillis(nowMillis()),
                                    serverRequestLimit = e.recommendationRateLimit(),
                                ),
                            )
                            logcat(LogPriority.WARN, e) {
                                "Recommendation source request received HTTP 429: $name"
                            }
                        }
                        onFailure()
                        logcat(LogPriority.DEBUG, e) { "Recommendation source request failed: $name" }
                        null
                    }
                }
            }
        }

        private fun softDeadlineReached(): Boolean = monotonicNowNanos() >= softDeadlineNanos
    }

    private data class RowLoadOutcome(
        val manga: List<SManga>,
        val complete: Boolean,
        val retryAtMillis: Long? = null,
    )

    private data class DetailCacheKey(
        val sourceId: Long,
        val mangaUrl: String,
    )

    private data class DetailCacheEntry(
        val timestamp: Long,
        val value: SManga,
    )

    private class DetailCache {
        private val entries = LinkedHashMap<DetailCacheKey, DetailCacheEntry>(16, 0.75f, true)

        @Synchronized
        fun get(key: DetailCacheKey, now: Long): SManga? {
            val entry = entries[key] ?: return null
            if (now - entry.timestamp >= DETAIL_CACHE_TTL_MS) {
                entries.remove(key)
                return null
            }
            return entry.value.copy()
        }

        @Synchronized
        fun put(key: DetailCacheKey, value: SManga, now: Long) {
            entries[key] = DetailCacheEntry(now, value.copy())
            while (entries.size > MAX_DETAIL_CACHE_ENTRIES) {
                entries.remove(entries.keys.first())
            }
        }
    }

    companion object {
        private const val MAX_LOCAL_POOL = 200
        private const val MAX_INTERNAL_RESULTS = 40
        private const val MAX_DISPLAY_RESULTS = 10
        private const val MAX_SAMPLED_RESULTS = MAX_DISPLAY_RESULTS
        private const val MAX_CREATOR_SEARCHES = 2
        private const val MAX_GENRE_ROUTES = 2
        private val DEFERRED_FILTER_RETRY_DELAYS_MS = longArrayOf(75L, 150L)
        private const val MAX_EHENTAI_COMBINED_ROUTES = 2
        private const val MAX_COMBINED_STRUCTURED_TAGS = 2
        private const val MAX_CANDIDATES_PER_GENRE_ROUTE = 24
        private const val MAX_DETAILS = 10
        private const val MAX_CREATOR_DETAILS = 4
        private const val RESERVED_DETAILS_PER_ROW = 2
        private const val MAX_ANILIST_RESULTS = 4
        private const val MAX_CANDIDATES_PER_REQUEST = 12
        private const val MAX_SOURCE_RESULT_SAMPLE = 36
        private const val MAX_SOURCE_REQUESTS = 12
        private const val MAX_SOURCE_CONCURRENCY = 2
        private const val MAX_TEXT_FILTER_ROUTES = 4
        private const val MAX_CREATOR_ONLY_TEXT_FILTER_ROUTES = 2
        private const val EHENTAI_HOST = "e-hentai.org"
        private const val EXHENTAI_HOST = "exhentai.org"
        private const val MIN_QUALITY_POOL_SIZE = MAX_SAMPLED_RESULTS
        private const val MIN_ANILIST_TAG_RANK = 60
        private const val HARD_TIMEOUT_MS = 8_000L
        private const val MAX_HARD_TIMEOUT_MS = 20_000L
        private const val SOFT_TIMEOUT_MS = 4_000L
        private const val EXTERNAL_TIMEOUT_MS = 4_000L
        private const val SOURCE_CALL_TIMEOUT_MS = 4_000L
        private const val TEXT_FILTER_SOURCE_CALL_TIMEOUT_MS = 6_000L
        private const val PROGRESSIVE_CARD_INTERVAL_MS = 20L
        private const val NANOS_PER_MILLISECOND = 1_000_000L
        private const val DETAIL_CACHE_TTL_MS = 6 * 60 * 60 * 1000L
        private const val MAX_DETAIL_CACHE_ENTRIES = 512
    }
}

private fun Throwable.isRecommendationRateLimited(): Boolean =
    generateSequence(this) { it.cause }
        .any { cause -> cause is HttpException && cause.code == HTTP_TOO_MANY_REQUESTS }

private fun Throwable.recommendationRetryAfterMillis(nowMillis: Long): Long? {
    val value = generateSequence(this) { it.cause }
        .filterIsInstance<HttpException>()
        .firstOrNull { it.code == HTTP_TOO_MANY_REQUESTS }
        ?.retryAfter
        ?.trim()
        ?.takeIf(String::isNotEmpty)
        ?: return null
    value.toLongOrNull()?.let { seconds ->
        if (seconds < 0L) return null
        return seconds.coerceAtMost(Long.MAX_VALUE / 1_000L) * 1_000L
    }
    return runCatching {
        val retryAt = ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
            .toInstant()
            .toEpochMilli()
        (retryAt - Instant.ofEpochMilli(nowMillis).toEpochMilli()).coerceAtLeast(0L)
    }.getOrNull()
}

private fun Throwable.recommendationRateLimit(): Int? =
    generateSequence(this) { it.cause }
        .filterIsInstance<HttpException>()
        .firstOrNull { it.code == HTTP_TOO_MANY_REQUESTS }
        ?.rateLimit

private const val HTTP_TOO_MANY_REQUESTS = 429
