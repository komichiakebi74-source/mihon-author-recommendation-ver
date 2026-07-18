package eu.kanade.tachiyomi.data.recommendation

import eu.kanade.tachiyomi.source.model.SManga
import kotlin.math.exp
import kotlin.math.ln

/** Quality-weighted random sampling with source-local recency and diversity penalties. */
internal object RecommendationSampler {

    private const val MIN_QUALITY_SCORE = 0.20
    private const val TEMPERATURE = 0.15
    private const val MMR_LAMBDA = 0.85
    private const val MIN_RANDOM_UNIT = 1.0e-12
    fun sample(
        candidates: List<RankedSimilarCandidate>,
        maxResults: Int,
        unseenTargetSize: Int = maxResults,
        maxPoolSize: Int,
        exposureSnapshot: RecommendationExposureSnapshot,
        sourceExposureSnapshot: RecommendationExposureSnapshot = exposureSnapshot,
        randomPriorities: MutableMap<String, Double>,
        pinnedWorkKeys: List<Set<String>> = emptyList(),
        workKeys: (SManga) -> Set<String>,
        nextRandomDouble: () -> Double,
    ): List<SManga> {
        if (maxResults <= 0) return emptyList()
        val qualified = candidates.asSequence()
            .filter { it.score >= MIN_QUALITY_SCORE }
            .sortedWith(
                compareByDescending<RankedSimilarCandidate>(RankedSimilarCandidate::score)
                    .thenBy { RecommendationMetadata.safeUrl(it.manga) },
            )
            .toList()
        if (qualified.isEmpty()) return emptyList()

        fun matches(candidate: RankedSimilarCandidate, keys: Set<String>): Boolean =
            keys.isNotEmpty() && workKeys(candidate.manga).any(keys::contains)

        // Preserve cards already published by this observation while they still pass the current
        // quality gate. Pins that become invalid after detail hydration are deliberately ignored.
        val pinned = pinnedWorkKeys.mapNotNull { keys ->
            qualified.firstOrNull { candidate -> matches(candidate, keys) }
        }.distinct()
        val pool = buildList {
            addAll(pinned.take(maxPoolSize))
            qualified.forEach { candidate ->
                if (size < maxPoolSize && candidate !in this) add(candidate)
            }
        }
        if (pool.isEmpty()) return emptyList()

        val selected = pinned.take(maxResults).toMutableList()
        val selectedSet = selected.toHashSet()
        val unseen = pool.filterNot(selectedSet::contains).filter { candidate ->
            val keys = workKeys(candidate.manga)
            exposureSnapshot.exposureIndex(keys) == null &&
                sourceExposureSnapshot.exposureIndex(keys) == null
        }
        selectWeighted(
            candidates = unseen,
            maxResults = maxResults,
            selected = selected,
            randomPriorities = randomPriorities,
            workKeys = workKeys,
            nextRandomDouble = nextRandomDouble,
        )
        if (selected.size >= unseenTargetSize || selected.size >= maxResults) {
            return selected.map(RankedSimilarCandidate::manga)
        }

        val selectedAfterUnseen = selected.toSet()
        val oldestSeen = pool.asSequence()
            .filterNot(selectedAfterUnseen::contains)
            .mapNotNull { candidate ->
                val keys = workKeys(candidate.manga)
                listOfNotNull(
                    sourceExposureSnapshot.lastShownExposure(keys),
                    exposureSnapshot.lastShownExposure(keys),
                ).maxWithOrNull(exposureRecencyComparator)?.let { it to candidate }
            }
            .sortedWith(
                compareBy<Pair<RecommendationExposure, RankedSimilarCandidate>> { it.first.shownAtMillis }
                    .thenBy { it.first.sequence }
                    .thenByDescending { it.second.score },
            )
            .map(Pair<RecommendationExposure, RankedSimilarCandidate>::second)
        oldestSeen
            .take(maxResults - selected.size)
            .forEach(selected::add)
        return selected.map(RankedSimilarCandidate::manga)
    }

    private fun selectWeighted(
        candidates: List<RankedSimilarCandidate>,
        maxResults: Int,
        selected: MutableList<RankedSimilarCandidate>,
        randomPriorities: MutableMap<String, Double>,
        workKeys: (SManga) -> Set<String>,
        nextRandomDouble: () -> Double,
    ) {
        val remaining = candidates.toMutableList()
        while (remaining.isNotEmpty() && selected.size < maxResults) {
            val utilities = remaining.associateWith { candidate ->
                val redundancy = selected.maxOfOrNull { chosen ->
                    jaccard(candidate.genres, chosen.genres)
                } ?: 0.0
                MMR_LAMBDA * candidate.score - (1.0 - MMR_LAMBDA) * redundancy
            }
            val bestUtility = utilities.values.maxOrNull() ?: break
            val next = remaining.minByOrNull { candidate ->
                val key = workKeys(candidate.manga).minOrNull().orEmpty()
                val uniform = randomPriorities.getOrPut(key) {
                    nextRandomDouble().coerceIn(MIN_RANDOM_UNIT, 1.0 - MIN_RANDOM_UNIT)
                }
                val weight = exp((utilities.getValue(candidate) - bestUtility) / TEMPERATURE)
                -ln(uniform) / weight.coerceAtLeast(MIN_RANDOM_UNIT)
            } ?: break
            selected += next
            remaining -= next
        }
    }

    private fun jaccard(left: Set<String>, right: Set<String>): Double {
        if (left.isEmpty() || right.isEmpty()) return 0.0
        val union = left union right
        return if (union.isEmpty()) 0.0 else (left intersect right).size.toDouble() / union.size
    }

    private val exposureRecencyComparator =
        compareBy<RecommendationExposure>(RecommendationExposure::shownAtMillis)
            .thenBy(RecommendationExposure::sequence)
}
