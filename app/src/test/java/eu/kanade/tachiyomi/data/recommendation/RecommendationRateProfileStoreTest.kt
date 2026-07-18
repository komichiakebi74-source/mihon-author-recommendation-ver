package eu.kanade.tachiyomi.data.recommendation

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import tachiyomi.core.common.preference.InMemoryPreferenceStore

class RecommendationRateProfileStoreTest {

    @Test
    fun `profiles round trip replace and delete independently`() {
        val preference = InMemoryPreferenceStore.InMemoryPreference<Set<String>>(
            key = "rate_profiles",
            data = setOf("malformed", "2|20|60000|1"),
            defaultValue = emptySet(),
        )
        val store = PreferenceRecommendationRateProfileStore(preference)
        val first = RecommendationRateProfile(10, 60_000L, 20L)
        val replacement = RecommendationRateProfile(100, 60_000L, 40L)

        store.save(1L, first)
        assertEquals(first, store.load(1L))
        store.save(1L, replacement)
        assertEquals(replacement, store.load(1L))
        assertEquals(RecommendationRateProfile(20, 60_000L, 1L), store.load(2L))

        store.delete(1L)
        assertNull(store.load(1L))
        assertEquals(RecommendationRateProfile(20, 60_000L, 1L), store.load(2L))
    }
}
