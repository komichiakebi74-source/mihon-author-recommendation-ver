package eu.kanade.tachiyomi.ui.manga

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import tachiyomi.domain.manga.model.Manga

class RecommendationRowStateTest {

    @Test
    fun `fresh partial results contain no cards from the previous manga`() {
        val state = listOf(manga("new-1"), manga("old-1"))
            .toRecommendationState() as RecommendationRowState.Success

        assertEquals(listOf("new-1", "old-1"), state.manga.map(Manga::url))
    }

    @Test
    fun `empty current-source response hides the row`() {
        val state = emptyList<Manga>().toRecommendationState()

        assertSame(RecommendationRowState.Hidden, state)
    }

    @Test
    fun `transient empty refresh retains this manga last successful row`() {
        val previous = RecommendationRowState.Success(listOf(manga("kept")))

        val state = resolveRecommendationRow(
            previous = previous,
            fresh = emptyList(),
            authoritative = false,
        )

        assertSame(previous, state)
    }

    @Test
    fun `similar row retains no more than ten cards`() {
        val state = (1..25).map { manga("work-$it") }
            .toRecommendationState(maxResults = 10) as RecommendationRowState.Success

        assertEquals((1..10).map { "work-$it" }, state.manga.map(Manga::url))
    }

    @Test
    fun `returning to a page with visible recommendations keeps its snapshot`() {
        val visible = RecommendationRowState.Success(listOf(manga("kept")))

        assertFalse(shouldRestartRecommendationsAfterStop(visible, RecommendationRowState.Hidden))
        assertFalse(shouldRestartRecommendationsAfterStop(RecommendationRowState.Hidden, visible))
        assertTrue(
            shouldRestartRecommendationsAfterStop(
                RecommendationRowState.Hidden,
                RecommendationRowState.Hidden,
            ),
        )
    }

    @Test
    fun `adding a recommendation updates its library badge without replacing card metadata`() {
        val recommendation = manga("work").copy(title = "Fresh title", thumbnailUrl = "fresh-cover")
        val libraryManga = manga("work").copy(
            id = 99,
            favorite = true,
            dateAdded = 1234,
            title = "Stored title",
            thumbnailUrl = "stored-cover",
        )

        val state = RecommendationRowState.Success(listOf(recommendation))
            .withLibraryManga(libraryManga) as RecommendationRowState.Success

        assertEquals(99, state.manga.single().id)
        assertTrue(state.manga.single().favorite)
        assertEquals(1234, state.manga.single().dateAdded)
        assertEquals("Fresh title", state.manga.single().title)
        assertEquals("fresh-cover", state.manga.single().thumbnailUrl)
    }

    @Test
    fun `adding a recommendation can match an existing local id with an alias url`() {
        val recommendation = manga("alias-url").copy(id = 99)
        val libraryManga = manga("canonical-url").copy(id = 99, favorite = true)

        val state = RecommendationRowState.Success(listOf(recommendation))
            .withLibraryManga(libraryManga) as RecommendationRowState.Success

        assertTrue(state.manga.single().favorite)
    }

    @Test
    fun `library badge updates remain isolated by source`() {
        val recommendation = manga("work")
        val otherSourceManga = manga("work").copy(source = 84, id = 99, favorite = true)

        val state = RecommendationRowState.Success(listOf(recommendation))
            .withLibraryManga(otherSourceManga) as RecommendationRowState.Success

        assertFalse(state.manga.single().favorite)
    }

    @Test
    fun `hidden recommendation row remains hidden after a library update`() {
        assertSame(
            RecommendationRowState.Hidden,
            RecommendationRowState.Hidden.withLibraryManga(manga("work").copy(favorite = true)),
        )
    }

    private fun manga(url: String): Manga {
        return Manga.create().copy(
            source = 42,
            url = url,
            title = url,
            initialized = true,
        )
    }
}
