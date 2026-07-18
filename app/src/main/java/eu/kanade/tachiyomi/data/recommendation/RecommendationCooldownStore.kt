package eu.kanade.tachiyomi.data.recommendation

import tachiyomi.core.common.preference.Preference
import tachiyomi.core.common.preference.PreferenceStore

internal data class RecommendationCooldownState(
    val lastRateLimitAtMillis: Long,
    val cooldownUntilMillis: Long,
    val consecutiveRateLimits: Int,
)

internal interface RecommendationCooldownStore {
    fun load(sourceId: Long): RecommendationCooldownState?
    fun save(sourceId: Long, state: RecommendationCooldownState)
    fun delete(sourceId: Long)
}

internal object NoopRecommendationCooldownStore : RecommendationCooldownStore {
    override fun load(sourceId: Long): RecommendationCooldownState? = null
    override fun save(sourceId: Long, state: RecommendationCooldownState) = Unit
    override fun delete(sourceId: Long) = Unit
}

/** Persists only temporary HTTP 429 recovery state; user-selected rates live in their own store. */
internal class PreferenceRecommendationCooldownStore(
    private val states: Preference<Set<String>>,
) : RecommendationCooldownStore {

    constructor(preferenceStore: PreferenceStore) : this(
        preferenceStore.getStringSet(
            Preference.appStateKey("recommendation_source_cooldowns_v1"),
            emptySet(),
        ),
    )

    private val lock = Any()

    override fun load(sourceId: Long): RecommendationCooldownState? = synchronized(lock) {
        states.get()
            .asSequence()
            .mapNotNull(::decode)
            .firstOrNull { it.first == sourceId }
            ?.second
    }

    override fun save(sourceId: Long, state: RecommendationCooldownState) {
        synchronized(lock) {
            states.set(
                states.get()
                    .asSequence()
                    .mapNotNull(::decode)
                    .filterNot { it.first == sourceId }
                    .mapTo(mutableSetOf()) { encode(it.first, it.second) }
                    .apply { add(encode(sourceId, state)) },
            )
        }
    }

    override fun delete(sourceId: Long) {
        synchronized(lock) {
            states.set(
                states.get()
                    .asSequence()
                    .mapNotNull(::decode)
                    .filterNot { it.first == sourceId }
                    .mapTo(mutableSetOf()) { encode(it.first, it.second) },
            )
        }
    }

    private fun encode(sourceId: Long, state: RecommendationCooldownState): String = listOf(
        sourceId,
        state.lastRateLimitAtMillis,
        state.cooldownUntilMillis,
        state.consecutiveRateLimits,
    ).joinToString(FIELD_SEPARATOR)

    private fun decode(value: String): Pair<Long, RecommendationCooldownState>? {
        val fields = value.split(FIELD_SEPARATOR)
        if (fields.size != FIELD_COUNT) return null
        val sourceId = fields[0].toLongOrNull() ?: return null
        return sourceId to RecommendationCooldownState(
            lastRateLimitAtMillis = fields[1].toLongOrNull() ?: return null,
            cooldownUntilMillis = fields[2].toLongOrNull() ?: return null,
            consecutiveRateLimits = fields[3].toIntOrNull()?.takeIf { it > 0 } ?: return null,
        )
    }

    private companion object {
        const val FIELD_SEPARATOR = "|"
        const val FIELD_COUNT = 4
    }
}
