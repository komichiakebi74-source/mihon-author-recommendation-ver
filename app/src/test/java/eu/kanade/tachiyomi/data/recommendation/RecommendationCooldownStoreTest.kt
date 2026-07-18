package eu.kanade.tachiyomi.data.recommendation

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import tachiyomi.core.common.preference.InMemoryPreferenceStore

class RecommendationCooldownStoreTest {

    @Test
    fun `cooldowns round trip replace and delete independently`() {
        val preference = InMemoryPreferenceStore.InMemoryPreference<Set<String>>(
            key = "cooldowns",
            data = setOf("malformed", "2|1|2|1"),
            defaultValue = emptySet(),
        )
        val store = PreferenceRecommendationCooldownStore(preference)
        val first = RecommendationCooldownState(10L, 20L, 1)
        val replacement = RecommendationCooldownState(40L, 50L, 2)

        store.save(1L, first)
        assertEquals(first, store.load(1L))
        store.save(1L, replacement)
        assertEquals(replacement, store.load(1L))
        assertEquals(RecommendationCooldownState(1L, 2L, 1), store.load(2L))

        store.delete(1L)
        assertNull(store.load(1L))
        assertEquals(RecommendationCooldownState(1L, 2L, 1), store.load(2L))
    }
}
