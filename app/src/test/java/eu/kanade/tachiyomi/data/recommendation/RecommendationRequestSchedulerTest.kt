package eu.kanade.tachiyomi.data.recommendation

import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RecommendationRequestSchedulerTest {

    @Test
    fun `unlimited requests start immediately`() = runTest {
        var nowNanos = 1_000_000_000L
        val starts = mutableListOf<Long>()
        val delays = mutableListOf<Long>()
        val scheduler = RecommendationRequestScheduler(delayMillis = { delays += it })

        repeat(4) {
            scheduler.withRatePermit(1L, 0L, { nowNanos }) { starts += nowNanos }
        }

        assertEquals(listOf(1_000_000_000L, 1_000_000_000L, 1_000_000_000L, 1_000_000_000L), starts)
        assertTrue(delays.isEmpty())
        assertEquals(0L, scheduler.qualityDeadlineAllowanceMillis(1L, 0L, 0L))
    }

    @Test
    fun `every configured preset starts immediately then uses its exact interval`() = runTest {
        SUPPORTED_RECOMMENDATION_INTERVALS.filter { it > 0L }.forEach { interval ->
            var nowNanos = 0L
            val starts = mutableListOf<Long>()
            val scheduler = RecommendationRequestScheduler(delayMillis = { waitMillis ->
                nowNanos += waitMillis * 1_000_000L
            })

            repeat(3) {
                scheduler.withRatePermit(7L, interval, { nowNanos }) { starts += nowNanos }
            }

            assertEquals(listOf(0L, interval, interval * 2L), starts.map { it / 1_000_000L })
        }
    }

    @Test
    fun `configured waits are cancellable`() = runTest {
        val scheduler = RecommendationRequestScheduler()
        scheduler.withRatePermit(1L, 5_000L, { testScheduler.currentTime * 1_000_000L }) { }
        var invoked = false
        val waiting = launch {
            scheduler.withRatePermit(1L, 5_000L, { testScheduler.currentTime * 1_000_000L }) {
                invoked = true
            }
        }

        runCurrent()
        waiting.cancel()
        advanceUntilIdle()

        assertTrue(waiting.isCancelled)
        assertEquals(false, invoked)
    }

    @Test
    fun `server Retry-After is authoritative and isolated by source ID`() {
        val scheduler = RecommendationRequestScheduler(fallbackJitterMillis = { _, _, _ -> 0L })

        assertEquals(4_000L, scheduler.recordRateLimit(10L, 1_000L, 3_000L))
        assertEquals(4_000L, scheduler.sourceCooldownUntil(10L, 1_001L))
        assertNull(scheduler.sourceCooldownUntil(11L, 1_001L))
        assertNull(scheduler.sourceCooldownUntil(10L, 4_000L))
    }

    @Test
    fun `fallback cooldown is truncated exponential and success clears it`() {
        val store = FakeCooldownStore()
        val scheduler = RecommendationRequestScheduler(
            cooldownStore = store,
            fallbackJitterMillis = { _, _, _ -> 0L },
        )
        var now = 1_000L
        val expected = listOf(15_000L, 30_000L, 60_000L, 120_000L, 300_000L, 300_000L)

        expected.forEach { duration ->
            assertEquals(now + duration, scheduler.recordRateLimit(1L, now, null))
            now += 1L
        }
        scheduler.recordSuccess(1L)

        assertNull(store.load(1L))
        assertNull(scheduler.sourceCooldownUntil(1L, now))
    }

    @Test
    fun `persisted cooldown is restored after scheduler recreation`() {
        val store = FakeCooldownStore()
        RecommendationRequestScheduler(
            cooldownStore = store,
            fallbackJitterMillis = { _, _, _ -> 0L },
        ).recordRateLimit(44L, 1_000L, 9_000L)

        val restored = RecommendationRequestScheduler(cooldownStore = store)
        restored.prepare(44L, 2_000L)

        assertEquals(10_000L, restored.sourceCooldownUntil(44L, 2_000L))
    }

    @Test
    fun `server quota reserves foreground capacity and evenly spaces recommendations`() = runTest {
        var nowMillis = 60_000L
        var nowNanos = 0L
        val profileStore = FakeRateProfileStore()
        val scheduler = RecommendationRequestScheduler(
            rateProfileStore = profileStore,
            delayMillis = { waitMillis ->
                nowMillis += waitMillis
                nowNanos += waitMillis * 1_000_000L
            },
        )
        scheduler.recordRateLimit(
            sourceId = 9L,
            nowMillis = 0L,
            retryAfterMillis = 60_000L,
            serverRequestLimit = 10,
        )
        val starts = mutableListOf<Long>()

        repeat(8) {
            scheduler.withRatePermit(9L, 0L, { nowNanos }, { nowMillis }) {
                starts += nowNanos / 1_000_000L
            }
        }

        assertEquals(
            listOf(0L, 8_572L, 17_144L, 25_716L, 34_288L, 42_860L, 51_432L, 60_004L),
            starts,
        )
        assertEquals(12_000L, scheduler.qualityDeadlineAllowanceMillis(9L, 0L, nowMillis))
        assertEquals(RecommendationRateProfile(10, 60_000L, 0L), profileStore.load(9L))
    }

    @Test
    fun `higher authenticated quota permits proportionally faster recommendations`() = runTest {
        var nowMillis = 60_000L
        var nowNanos = 0L
        val scheduler = RecommendationRequestScheduler(delayMillis = { waitMillis ->
            nowMillis += waitMillis
            nowNanos += waitMillis * 1_000_000L
        })
        scheduler.recordRateLimit(1L, 0L, 60_000L, serverRequestLimit = 100)
        val starts = mutableListOf<Long>()

        repeat(3) {
            scheduler.withRatePermit(1L, 0L, { nowNanos }, { nowMillis }) {
                starts += nowNanos / 1_000_000L
            }
        }

        assertEquals(listOf(0L, 800L, 1_600L), starts)
    }

    @Test
    fun `learned quota restores after recreation and can be reset`() = runTest {
        val profileStore = FakeRateProfileStore()
        RecommendationRequestScheduler(rateProfileStore = profileStore)
            .recordRateLimit(3L, 1_000L, 60_000L, serverRequestLimit = 10)
        val restored = RecommendationRequestScheduler(rateProfileStore = profileStore)
        restored.prepare(3L, 2_000L)

        assertEquals(12_000L, restored.qualityDeadlineAllowanceMillis(3L, 0L, 2_000L))
        restored.resetLearnedRateProfile(3L)

        assertNull(profileStore.load(3L))
        assertEquals(0L, restored.qualityDeadlineAllowanceMillis(3L, 0L, 2_000L))
    }

    private class FakeCooldownStore : RecommendationCooldownStore {
        private val values = mutableMapOf<Long, RecommendationCooldownState>()

        override fun load(sourceId: Long): RecommendationCooldownState? = values[sourceId]

        override fun save(sourceId: Long, state: RecommendationCooldownState) {
            values[sourceId] = state
        }

        override fun delete(sourceId: Long) {
            values.remove(sourceId)
        }
    }

    private class FakeRateProfileStore : RecommendationRateProfileStore {
        private val values = mutableMapOf<Long, RecommendationRateProfile>()

        override fun load(sourceId: Long): RecommendationRateProfile? = values[sourceId]

        override fun save(sourceId: Long, profile: RecommendationRateProfile) {
            values[sourceId] = profile
        }

        override fun delete(sourceId: Long) {
            values.remove(sourceId)
        }
    }
}
