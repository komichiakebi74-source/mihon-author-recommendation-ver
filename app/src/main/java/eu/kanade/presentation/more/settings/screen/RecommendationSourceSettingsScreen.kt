package eu.kanade.presentation.more.settings.screen

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.selection.selectable
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.RadioButton
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import cafe.adriel.voyager.core.model.rememberScreenModel
import cafe.adriel.voyager.navigator.LocalNavigator
import cafe.adriel.voyager.navigator.currentOrThrow
import eu.kanade.presentation.browse.components.BaseSourceItem
import eu.kanade.presentation.components.AppBar
import eu.kanade.presentation.more.settings.widget.PreferenceGroupHeader
import eu.kanade.presentation.util.Screen
import eu.kanade.presentation.util.animateItemFastScroll
import eu.kanade.tachiyomi.data.recommendation.RecommendationSourcePolicy
import eu.kanade.tachiyomi.data.recommendation.SUPPORTED_RECOMMENDATION_INTERVALS
import eu.kanade.tachiyomi.ui.more.settings.RecommendationSourceSettingsScreenModel
import eu.kanade.tachiyomi.util.system.LocaleHelper
import tachiyomi.domain.source.model.Source
import tachiyomi.i18n.MR
import tachiyomi.presentation.core.components.FastScrollLazyColumn
import tachiyomi.presentation.core.components.material.Scaffold
import tachiyomi.presentation.core.components.material.padding
import tachiyomi.presentation.core.i18n.stringResource
import tachiyomi.presentation.core.screens.EmptyScreen
import tachiyomi.presentation.core.screens.LoadingScreen
import tachiyomi.presentation.core.util.secondaryItemAlpha

class RecommendationSourceSettingsScreen : Screen() {

    @Composable
    override fun Content() {
        val navigator = LocalNavigator.currentOrThrow
        val screenModel = rememberScreenModel { RecommendationSourceSettingsScreenModel() }
        val state by screenModel.state.collectAsState()

        when (val current = state) {
            RecommendationSourceSettingsScreenModel.State.Loading -> LoadingScreen()
            is RecommendationSourceSettingsScreenModel.State.Error -> EmptyScreen(MR.strings.internal_error)
            is RecommendationSourceSettingsScreenModel.State.Success -> RecommendationSourceSettingsContent(
                state = current,
                navigateUp = navigator::pop,
                onEnabledChange = screenModel::setEnabled,
                onIntervalChange = screenModel::setInterval,
            )
        }
    }
}

@Composable
private fun RecommendationSourceSettingsContent(
    state: RecommendationSourceSettingsScreenModel.State.Success,
    navigateUp: () -> Unit,
    onEnabledChange: (Long, Boolean) -> Unit,
    onIntervalChange: (Long, Long) -> Unit,
) {
    var rateDialogSource by remember { mutableStateOf<Source?>(null) }
    Scaffold(
        topBar = { scrollBehavior ->
            AppBar(
                title = stringResource(MR.strings.pref_recommendation_source_settings),
                navigateUp = navigateUp,
                scrollBehavior = scrollBehavior,
            )
        },
    ) { contentPadding ->
        if (state.sources.isEmpty()) {
            EmptyScreen(
                stringRes = MR.strings.source_filter_empty_screen,
                modifier = Modifier.padding(contentPadding),
            )
            return@Scaffold
        }
        FastScrollLazyColumn(contentPadding = contentPadding) {
            item(key = "recommendation-source-info", contentType = "info") {
                Text(
                    text = stringResource(MR.strings.pref_recommendation_source_settings_info),
                    modifier = Modifier.padding(MaterialTheme.padding.medium),
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    style = MaterialTheme.typography.bodyMedium,
                )
            }
            state.sources.forEach { (language, sources) ->
                item(key = "recommendation-source-language-$language", contentType = "header") {
                    PreferenceGroupHeader(
                        title = LocaleHelper.getSourceDisplayName(language, LocalContext.current),
                    )
                }
                items(
                    items = sources,
                    key = { "recommendation-source-${it.id}" },
                    contentType = { "source" },
                ) { source ->
                    val policy = state.policies[source.id] ?: RecommendationSourcePolicy()
                    BaseSourceItem(
                        modifier = Modifier.animateItemFastScroll(),
                        source = source,
                        showLanguageInContent = false,
                        onClickItem = { rateDialogSource = source },
                        action = {
                            Switch(
                                checked = policy.enabled,
                                onCheckedChange = { onEnabledChange(source.id, it) },
                            )
                        },
                        content = { item, _ ->
                            RecommendationSourceContent(
                                source = item,
                                language = LocaleHelper.getSourceDisplayName(item.lang, LocalContext.current),
                                rate = recommendationRateLabel(policy.minRequestIntervalMillis),
                            )
                        },
                    )
                }
            }
        }
    }

    rateDialogSource?.let { source ->
        val policy = state.policies[source.id] ?: RecommendationSourcePolicy()
        RecommendationRateDialog(
            sourceName = source.name,
            selectedInterval = policy.minRequestIntervalMillis,
            onDismiss = { rateDialogSource = null },
            onSelected = {
                onIntervalChange(source.id, it)
                rateDialogSource = null
            },
        )
    }
}

@Composable
private fun RowScope.RecommendationSourceContent(source: Source, language: String, rate: String) {
    Column(
        modifier = Modifier
            .padding(horizontal = MaterialTheme.padding.medium)
            .weight(1f),
    ) {
        Text(
            text = source.name,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
            style = MaterialTheme.typography.bodyMedium,
        )
        Text(
            modifier = Modifier.secondaryItemAlpha(),
            text = "$language · $rate",
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
            style = MaterialTheme.typography.bodySmall,
        )
    }
}

@Composable
private fun RecommendationRateDialog(
    sourceName: String,
    selectedInterval: Long,
    onDismiss: () -> Unit,
    onSelected: (Long) -> Unit,
) {
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(stringResource(MR.strings.pref_recommendation_request_rate)) },
        text = {
            Column {
                Text(
                    text = sourceName,
                    modifier = Modifier.padding(bottom = 8.dp),
                    style = MaterialTheme.typography.bodyMedium,
                )
                SUPPORTED_RECOMMENDATION_INTERVALS.sorted().forEach { interval ->
                    Row(
                        modifier = Modifier
                            .fillMaxWidth()
                            .selectable(
                                selected = selectedInterval == interval,
                                onClick = { onSelected(interval) },
                            )
                            .padding(vertical = 8.dp),
                        verticalAlignment = Alignment.CenterVertically,
                    ) {
                        RadioButton(selected = selectedInterval == interval, onClick = null)
                        Text(
                            text = recommendationRateLabel(interval),
                            modifier = Modifier.padding(start = 16.dp),
                        )
                    }
                }
            }
        },
        confirmButton = {
            TextButton(onClick = onDismiss) {
                Text(stringResource(MR.strings.action_cancel))
            }
        },
    )
}

@Composable
private fun recommendationRateLabel(intervalMillis: Long): String = when (intervalMillis) {
    250L -> stringResource(MR.strings.pref_recommendation_rate_4_per_second)
    500L -> stringResource(MR.strings.pref_recommendation_rate_2_per_second)
    1_000L -> stringResource(MR.strings.pref_recommendation_rate_1_per_second)
    2_000L -> stringResource(MR.strings.pref_recommendation_rate_1_per_2_seconds)
    5_000L -> stringResource(MR.strings.pref_recommendation_rate_1_per_5_seconds)
    10_000L -> stringResource(MR.strings.pref_recommendation_rate_1_per_10_seconds)
    else -> stringResource(MR.strings.pref_recommendation_rate_unlimited)
}
