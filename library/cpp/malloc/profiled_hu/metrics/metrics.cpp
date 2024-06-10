#include "metrics.h"
#include "metrics_internal.h"

#include <type_traits>

static_assert(std::is_trivially_default_constructible_v<THuMetricsThreadListItem>);
static_assert(std::is_trivially_default_constructible_v<THuThreadMetricsHandle>);
static_assert(std::is_trivially_default_constructible_v<THuMetricsList>);

THuMetrics GetHuMetrics() {
    return THuMetricsList::GetAggregatedMetrics();
}
