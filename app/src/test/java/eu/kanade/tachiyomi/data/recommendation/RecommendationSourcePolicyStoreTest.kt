package eu.kanade.tachiyomi.data.recommendation

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import tachiyomi.core.common.preference.InMemoryPreferenceStore

class RecommendationSourcePolicyStoreTest {

    @Test
    fun `default policy is enabled and unlimited`() {
        val store = RecommendationSourcePolicyStore(InMemoryPreferenceStore())

        assertEquals(RecommendationSourcePolicy(), store.get(100L))
        assertEquals("recommendation_source_100_enabled_v1", store.enabledPreference(100L).key())
        assertEquals("recommendation_source_100_interval_v1", store.intervalPreference(100L).key())
    }

    @Test
    fun `enabled state and every rate preset are isolated by source ID`() {
        val store = RecommendationSourcePolicyStore(InMemoryPreferenceStore())
        store.setEnabled(1L, false)

        assertFalse(store.get(1L).enabled)
        assertTrue(store.get(2L).enabled)
        SUPPORTED_RECOMMENDATION_INTERVALS.forEach { interval ->
            store.setInterval(1L, interval)
            assertEquals(interval, store.get(1L).minRequestIntervalMillis)
            assertEquals(0L, store.get(2L).minRequestIntervalMillis)
        }
    }

    @Test
    fun `unsupported intervals are rejected`() {
        val store = RecommendationSourcePolicyStore(InMemoryPreferenceStore())

        assertThrows(IllegalArgumentException::class.java) { store.setInterval(1L, 123L) }
    }
}
