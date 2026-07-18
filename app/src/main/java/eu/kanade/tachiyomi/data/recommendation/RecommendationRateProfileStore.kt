package eu.kanade.tachiyomi.data.recommendation

import tachiyomi.core.common.preference.Preference
import tachiyomi.core.common.preference.PreferenceStore

/** Server-advertised request quota learned from an HTTP 429 response. */
internal data class RecommendationRateProfile(
    val requestLimit: Int,
    val windowMillis: Long,
    val learnedAtMillis: Long,
)

internal interface RecommendationRateProfileStore {
    fun load(sourceId: Long): RecommendationRateProfile?
    fun save(sourceId: Long, profile: RecommendationRateProfile)
    fun delete(sourceId: Long)
}

internal object NoopRecommendationRateProfileStore : RecommendationRateProfileStore {
    override fun load(sourceId: Long): RecommendationRateProfile? = null
    override fun save(sourceId: Long, profile: RecommendationRateProfile) = Unit
    override fun delete(sourceId: Long) = Unit
}

/** Persists only server-advertised quotas; user-selected intervals remain in the policy store. */
internal class PreferenceRecommendationRateProfileStore(
    private val profiles: Preference<Set<String>>,
) : RecommendationRateProfileStore {

    constructor(preferenceStore: PreferenceStore) : this(
        preferenceStore.getStringSet(
            Preference.appStateKey("recommendation_source_rate_profiles_v1"),
            emptySet(),
        ),
    )

    private val lock = Any()

    override fun load(sourceId: Long): RecommendationRateProfile? = synchronized(lock) {
        profiles.get()
            .asSequence()
            .mapNotNull(::decode)
            .firstOrNull { it.first == sourceId }
            ?.second
    }

    override fun save(sourceId: Long, profile: RecommendationRateProfile) {
        synchronized(lock) {
            profiles.set(
                profiles.get()
                    .asSequence()
                    .mapNotNull(::decode)
                    .filterNot { it.first == sourceId }
                    .mapTo(mutableSetOf()) { encode(it.first, it.second) }
                    .apply { add(encode(sourceId, profile)) },
            )
        }
    }

    override fun delete(sourceId: Long) {
        synchronized(lock) {
            profiles.set(
                profiles.get()
                    .asSequence()
                    .mapNotNull(::decode)
                    .filterNot { it.first == sourceId }
                    .mapTo(mutableSetOf()) { encode(it.first, it.second) },
            )
        }
    }

    private fun encode(sourceId: Long, profile: RecommendationRateProfile): String = listOf(
        sourceId,
        profile.requestLimit,
        profile.windowMillis,
        profile.learnedAtMillis,
    ).joinToString(FIELD_SEPARATOR)

    private fun decode(value: String): Pair<Long, RecommendationRateProfile>? {
        val fields = value.split(FIELD_SEPARATOR)
        if (fields.size != FIELD_COUNT) return null
        val sourceId = fields[0].toLongOrNull() ?: return null
        return sourceId to RecommendationRateProfile(
            requestLimit = fields[1].toIntOrNull()?.takeIf { it > 0 } ?: return null,
            windowMillis = fields[2].toLongOrNull()?.takeIf { it > 0L } ?: return null,
            learnedAtMillis = fields[3].toLongOrNull()?.takeIf { it >= 0L } ?: return null,
        )
    }

    private companion object {
        const val FIELD_SEPARATOR = "|"
        const val FIELD_COUNT = 4
    }
}
