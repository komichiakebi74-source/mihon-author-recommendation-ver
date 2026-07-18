package eu.kanade.tachiyomi.data.recommendation

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.combine
import tachiyomi.core.common.preference.Preference
import tachiyomi.core.common.preference.PreferenceStore
import java.util.concurrent.ConcurrentHashMap

data class RecommendationSourcePolicy(
    val enabled: Boolean = true,
    val minRequestIntervalMillis: Long = UNLIMITED_INTERVAL_MILLIS,
) {
    init {
        require(minRequestIntervalMillis in SUPPORTED_RECOMMENDATION_INTERVALS)
    }

    val isRateLimited: Boolean
        get() = minRequestIntervalMillis > UNLIMITED_INTERVAL_MILLIS
}

class RecommendationSourcePolicyStore(
    private val preferenceStore: PreferenceStore,
) {
    private val enabledPreferences = ConcurrentHashMap<Long, Preference<Boolean>>()
    private val intervalPreferences = ConcurrentHashMap<Long, Preference<Long>>()

    fun get(sourceId: Long): RecommendationSourcePolicy = RecommendationSourcePolicy(
        enabled = enabledPreference(sourceId).get(),
        minRequestIntervalMillis = intervalPreference(sourceId).get().sanitizeRecommendationInterval(),
    )

    fun changes(sourceId: Long): Flow<RecommendationSourcePolicy> = combine(
        enabledPreference(sourceId).changes(),
        intervalPreference(sourceId).changes(),
    ) { enabled, interval ->
        RecommendationSourcePolicy(
            enabled = enabled,
            minRequestIntervalMillis = interval.sanitizeRecommendationInterval(),
        )
    }

    fun setEnabled(sourceId: Long, enabled: Boolean) {
        enabledPreference(sourceId).set(enabled)
    }

    fun setInterval(sourceId: Long, intervalMillis: Long) {
        require(intervalMillis in SUPPORTED_RECOMMENDATION_INTERVALS)
        intervalPreference(sourceId).set(intervalMillis)
    }

    internal fun enabledPreference(sourceId: Long): Preference<Boolean> =
        enabledPreferences.computeIfAbsent(sourceId) {
            preferenceStore.getBoolean("recommendation_source_${sourceId}_enabled_v1", true)
        }

    internal fun intervalPreference(sourceId: Long): Preference<Long> =
        intervalPreferences.computeIfAbsent(sourceId) {
            preferenceStore.getLong(
                "recommendation_source_${sourceId}_interval_v1",
                UNLIMITED_INTERVAL_MILLIS,
            )
        }
}

private fun Long.sanitizeRecommendationInterval(): Long =
    takeIf(SUPPORTED_RECOMMENDATION_INTERVALS::contains) ?: UNLIMITED_INTERVAL_MILLIS

internal const val UNLIMITED_INTERVAL_MILLIS = 0L

internal val SUPPORTED_RECOMMENDATION_INTERVALS = setOf(
    UNLIMITED_INTERVAL_MILLIS,
    250L,
    500L,
    1_000L,
    2_000L,
    5_000L,
    10_000L,
)
