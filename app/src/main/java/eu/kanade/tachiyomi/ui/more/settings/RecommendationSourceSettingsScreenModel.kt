package eu.kanade.tachiyomi.ui.more.settings

import androidx.compose.runtime.Immutable
import cafe.adriel.voyager.core.model.StateScreenModel
import cafe.adriel.voyager.core.model.screenModelScope
import eu.kanade.domain.source.interactor.GetLanguagesWithSources
import eu.kanade.tachiyomi.data.recommendation.MangaRecommendationRepository
import eu.kanade.tachiyomi.data.recommendation.RecommendationSourcePolicy
import eu.kanade.tachiyomi.data.recommendation.RecommendationSourcePolicyStore
import eu.kanade.tachiyomi.util.system.LocaleHelper
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import tachiyomi.domain.source.model.Source
import uy.kohesive.injekt.Injekt
import uy.kohesive.injekt.api.get
import java.util.SortedMap

internal class RecommendationSourceSettingsScreenModel(
    private val getLanguagesWithSources: GetLanguagesWithSources = Injekt.get(),
    private val policyStore: RecommendationSourcePolicyStore = Injekt.get(),
    private val recommendationRepository: MangaRecommendationRepository = Injekt.get(),
) : StateScreenModel<RecommendationSourceSettingsScreenModel.State>(State.Loading) {

    init {
        screenModelScope.launch {
            getLanguagesWithSources.subscribe()
                .catch { throwable -> mutableState.update { State.Error(throwable) } }
                .collectLatest { sources ->
                    val sortedSources = sources
                        .mapValues { (_, languageSources) ->
                            languageSources.sortedWith(
                                compareBy<Source, String>(String.CASE_INSENSITIVE_ORDER) { it.name }
                                    .thenBy { it.id },
                            )
                        }
                        .toSortedMap(LocaleHelper.comparator)
                    mutableState.update {
                        State.Success(
                            sources = sortedSources,
                            policies = sortedSources.values
                                .flatten()
                                .associate { source -> source.id to policyStore.get(source.id) },
                        )
                    }
                }
        }
    }

    fun setEnabled(sourceId: Long, enabled: Boolean) {
        policyStore.setEnabled(sourceId, enabled)
        updatePolicy(sourceId)
    }

    fun setInterval(sourceId: Long, intervalMillis: Long) {
        policyStore.setInterval(sourceId, intervalMillis)
        recommendationRepository.resetLearnedRateProfile(sourceId)
        updatePolicy(sourceId)
    }

    private fun updatePolicy(sourceId: Long) {
        mutableState.update { current ->
            if (current !is State.Success) return@update current
            current.copy(policies = current.policies + (sourceId to policyStore.get(sourceId)))
        }
    }

    sealed interface State {
        @Immutable
        data object Loading : State

        @Immutable
        data class Error(val throwable: Throwable) : State

        @Immutable
        data class Success(
            val sources: SortedMap<String, List<Source>>,
            val policies: Map<Long, RecommendationSourcePolicy>,
        ) : State
    }
}
