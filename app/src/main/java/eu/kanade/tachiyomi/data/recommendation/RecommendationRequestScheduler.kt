package eu.kanade.tachiyomi.data.recommendation

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.ArrayDeque
import java.util.concurrent.ConcurrentHashMap

/**
 * Coordinates recommendation traffic without changing a source's shared HTTP client.
 *
 * State is isolated by source ID. Unlimited sources keep the repository's normal concurrency,
 * while an explicit interval serializes only recommendation requests for that source.
 */
internal class RecommendationRequestScheduler(
    private val cooldownStore: RecommendationCooldownStore = NoopRecommendationCooldownStore,
    private val rateProfileStore: RecommendationRateProfileStore = NoopRecommendationRateProfileStore,
    private val delayMillis: suspend (Long) -> Unit = { delay(it) },
    private val fallbackJitterMillis: (sourceId: Long, failureCount: Int, baseMillis: Long) -> Long =
        ::deterministicJitterMillis,
) {
    private val concurrencyGates = ConcurrentHashMap<Long, SharedGate>()
    private val pacingStates = ConcurrentHashMap<Long, PacingState>()
    private val cooldownStates = ConcurrentHashMap<Long, RecommendationCooldownState>()
    private val rateProfiles = ConcurrentHashMap<Long, RecommendationRateProfile>()
    private val preparedSources = ConcurrentHashMap.newKeySet<Long>()

    fun prepare(sourceId: Long, nowMillis: Long) {
        if (!preparedSources.add(sourceId)) return
        cooldownStore.load(sourceId)?.let { stored ->
            if (nowMillis - stored.lastRateLimitAtMillis >= FAILURE_COUNT_RESET_WINDOW_MS) {
                cooldownStore.delete(sourceId)
            } else {
                cooldownStates[sourceId] = stored
            }
        }
        rateProfileStore.load(sourceId)?.let { stored ->
            if (stored.isExpired(nowMillis)) {
                rateProfileStore.delete(sourceId)
            } else {
                rateProfiles[sourceId] = stored
            }
        }
    }

    fun semaphore(sourceId: Long, maxConcurrency: Int = UNLIMITED_MAX_CONCURRENCY): Semaphore {
        require(maxConcurrency > 0) { "maxConcurrency must be positive" }
        return concurrencyGates.compute(sourceId) { _, existing ->
            when {
                existing == null -> SharedGate(maxConcurrency, Semaphore(maxConcurrency))
                existing.maxConcurrency == maxConcurrency -> existing
                else -> error("Source $sourceId changed concurrency policy")
            }
        }!!.semaphore
    }

    fun sourceCooldownUntil(sourceId: Long, nowMillis: Long): Long? {
        val state = cooldownStates[sourceId] ?: return null
        return state.cooldownUntilMillis.takeIf { it > nowMillis }
    }

    fun recordRateLimit(
        sourceId: Long,
        nowMillis: Long,
        retryAfterMillis: Long?,
        serverRequestLimit: Int? = null,
    ): Long {
        val previous = cooldownStates[sourceId]
        val failureCount = if (
            previous == null || nowMillis - previous.lastRateLimitAtMillis >= FAILURE_COUNT_RESET_WINDOW_MS
        ) {
            1
        } else {
            (previous.consecutiveRateLimits + 1).coerceAtMost(FALLBACK_BACKOFF_MILLIS.size)
        }
        val cooldownMillis = retryAfterMillis?.coerceAtLeast(0L)
            ?: fallbackCooldownMillis(sourceId, failureCount)
        val state = RecommendationCooldownState(
            lastRateLimitAtMillis = nowMillis,
            cooldownUntilMillis = saturatingAdd(nowMillis, cooldownMillis),
            consecutiveRateLimits = failureCount,
        )
        cooldownStates[sourceId] = state
        preparedSources.add(sourceId)
        cooldownStore.save(sourceId, state)
        serverRequestLimit?.takeIf { it > 0 }?.let { requestLimit ->
            val profile = RecommendationRateProfile(
                requestLimit = requestLimit,
                windowMillis = (retryAfterMillis ?: DEFAULT_RATE_WINDOW_MS)
                    .coerceIn(MIN_RATE_WINDOW_MS, MAX_RATE_WINDOW_MS),
                learnedAtMillis = nowMillis,
            )
            rateProfiles[sourceId] = profile
            rateProfileStore.save(sourceId, profile)
        }
        return state.cooldownUntilMillis
    }

    /** Clears persisted 429 recovery state after a successful recommendation request. */
    fun recordSuccess(sourceId: Long) {
        if (cooldownStates.remove(sourceId) != null) {
            cooldownStore.delete(sourceId)
        }
    }

    /**
     * Applies a user-selected start interval. The first request is immediate; subsequent starts are
     * evenly spaced. [delayMillis] is cancellable, so obsolete page work leaves the FIFO promptly.
     */
    suspend fun <T> withRatePermit(
        sourceId: Long,
        minIntervalMillis: Long,
        monotonicNowNanos: () -> Long,
        nowMillis: () -> Long = System::currentTimeMillis,
        block: suspend () -> T,
    ): T {
        require(minIntervalMillis in SUPPORTED_RECOMMENDATION_INTERVALS)
        val initialProfile = activeRateProfile(sourceId, nowMillis())
        if (minIntervalMillis == UNLIMITED_INTERVAL_MILLIS && initialProfile == null) return block()

        val state = pacingStates.computeIfAbsent(sourceId) { PacingState() }
        return state.serialGate.withPermit {
            while (true) {
                val requestedAt = maxOf(monotonicNowNanos(), state.logicalNowNanos)
                val profile = activeRateProfile(sourceId, nowMillis())
                val effectiveIntervalMillis = maxOf(
                    minIntervalMillis,
                    profile?.minimumRecommendationIntervalMillis() ?: 0L,
                )
                val intervalWaitNanos = (state.nextStartNanos - requestedAt).coerceAtLeast(0L)
                val quotaWaitNanos = profile?.let { state.quotaWaitNanos(it, requestedAt) } ?: 0L
                val waitNanos = maxOf(intervalWaitNanos, quotaWaitNanos)
                if (waitNanos <= 0L) break
                delayMillis(nanosToCeilMillis(waitNanos))
            }

            val profile = activeRateProfile(sourceId, nowMillis())
            val effectiveIntervalMillis = maxOf(
                minIntervalMillis,
                profile?.minimumRecommendationIntervalMillis() ?: 0L,
            )
            val startedAt = maxOf(monotonicNowNanos(), state.nextStartNanos, state.logicalNowNanos)
            state.logicalNowNanos = startedAt
            state.nextStartNanos = saturatingAdd(
                startedAt,
                effectiveIntervalMillis.saturatingMillisToNanos(),
            )
            profile?.let { state.recordStart(it, startedAt) }
            block()
        }
    }

    fun qualityDeadlineAllowanceMillis(
        sourceId: Long,
        minRequestIntervalMillis: Long,
        nowMillis: Long,
    ): Long {
        val effectiveIntervalMillis = maxOf(
            minRequestIntervalMillis,
            activeRateProfile(sourceId, nowMillis)?.minimumRecommendationIntervalMillis() ?: 0L,
        )
        if (effectiveIntervalMillis <= 0L) return 0L
        return (effectiveIntervalMillis * QUALITY_REQUEST_SLOTS)
            .coerceAtMost(MAX_QUALITY_DEADLINE_ALLOWANCE_MS)
    }

    internal fun clearInMemoryState(sourceId: Long) {
        pacingStates.remove(sourceId)
        concurrencyGates.remove(sourceId)
    }

    internal fun resetLearnedRateProfile(sourceId: Long) {
        rateProfiles.remove(sourceId)
        rateProfileStore.delete(sourceId)
        pacingStates.remove(sourceId)
    }

    private fun activeRateProfile(sourceId: Long, nowMillis: Long): RecommendationRateProfile? {
        val profile = rateProfiles[sourceId] ?: return null
        if (!profile.isExpired(nowMillis)) return profile
        rateProfiles.remove(sourceId, profile)
        rateProfileStore.delete(sourceId)
        return null
    }

    private fun fallbackCooldownMillis(sourceId: Long, failureCount: Int): Long {
        val base = FALLBACK_BACKOFF_MILLIS[(failureCount - 1).coerceIn(FALLBACK_BACKOFF_MILLIS.indices)]
        return saturatingAdd(base, fallbackJitterMillis(sourceId, failureCount, base).coerceIn(0L, base / 5L))
    }

    private data class SharedGate(
        val maxConcurrency: Int,
        val semaphore: Semaphore,
    )

    private class PacingState {
        val serialGate = Semaphore(1)
        val requestStartsNanos = ArrayDeque<Long>()
        var nextStartNanos = 0L
        var logicalNowNanos = 0L

        fun quotaWaitNanos(profile: RecommendationRateProfile, nowNanos: Long): Long {
            removeExpiredStarts(profile, nowNanos)
            if (requestStartsNanos.size < profile.recommendationRequestLimit()) return 0L
            return (requestStartsNanos.first() + profile.windowMillis.saturatingMillisToNanos() - nowNanos)
                .coerceAtLeast(0L)
        }

        fun recordStart(profile: RecommendationRateProfile, startedAtNanos: Long) {
            removeExpiredStarts(profile, startedAtNanos)
            requestStartsNanos.addLast(startedAtNanos)
        }

        private fun removeExpiredStarts(profile: RecommendationRateProfile, nowNanos: Long) {
            val earliestActive = nowNanos - profile.windowMillis.saturatingMillisToNanos()
            while (requestStartsNanos.isNotEmpty() && requestStartsNanos.first() <= earliestActive) {
                requestStartsNanos.removeFirst()
            }
        }
    }

    internal companion object {
        const val UNLIMITED_MAX_CONCURRENCY = 2
        const val MAX_QUALITY_DEADLINE_ALLOWANCE_MS = 12_000L
        const val FAILURE_COUNT_RESET_WINDOW_MS = 30 * 60 * 1_000L
        private const val QUALITY_REQUEST_SLOTS = 3L
        internal const val RATE_PROFILE_TTL_MS = 6 * 60 * 60 * 1_000L
        private const val DEFAULT_RATE_WINDOW_MS = 60_000L
        private const val MIN_RATE_WINDOW_MS = 1_000L
        private const val MAX_RATE_WINDOW_MS = 10 * 60 * 1_000L
        private val FALLBACK_BACKOFF_MILLIS = longArrayOf(15_000L, 30_000L, 60_000L, 120_000L, 300_000L)
    }
}

private fun RecommendationRateProfile.isExpired(nowMillis: Long): Boolean =
    nowMillis - learnedAtMillis >= RecommendationRequestScheduler.RATE_PROFILE_TTL_MS

private fun RecommendationRateProfile.recommendationRequestLimit(): Int {
    if (requestLimit <= 1) return 1
    val foregroundReserve = ((requestLimit + 3) / 4).coerceAtLeast(1)
    return (requestLimit - foregroundReserve).coerceAtLeast(1)
}

private fun RecommendationRateProfile.minimumRecommendationIntervalMillis(): Long {
    val recommendationLimit = recommendationRequestLimit().toLong()
    return (windowMillis + recommendationLimit - 1L) / recommendationLimit
}

private fun deterministicJitterMillis(sourceId: Long, failureCount: Int, baseMillis: Long): Long {
    var mixed = sourceId xor (failureCount.toLong() * -7046029254386353131L)
    mixed = (mixed xor (mixed ushr 30)) * -4658895280553007687L
    mixed = (mixed xor (mixed ushr 27)) * -7723592293110705685L
    mixed = mixed xor (mixed ushr 31)
    val bound = baseMillis / 5L
    return if (bound <= 0L) 0L else (mixed and Long.MAX_VALUE) % (bound + 1L)
}

private fun Long.saturatingMillisToNanos(): Long =
    if (this > Long.MAX_VALUE / 1_000_000L) Long.MAX_VALUE else this * 1_000_000L

private fun nanosToCeilMillis(nanos: Long): Long =
    (nanos / 1_000_000L) + if (nanos % 1_000_000L == 0L) 0L else 1L

private fun saturatingAdd(left: Long, right: Long): Long =
    if (right > 0L && left > Long.MAX_VALUE - right) Long.MAX_VALUE else left + right
