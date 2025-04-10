#include "helpers.h"

#include <library/cpp/yt/logging/logger.h>

#include <util/string/join.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

namespace {

TString BuildMetric(const TString& requestType, const TString& duration)
{
    static const std::vector<TString> sources{"dynamic", "static_chunk"};

    if (requestType == "lookup_cpu") {
        return Format("double([/performance_counters/%v_time_%v_rate])", requestType, duration);
    }

    std::vector<TString> metrics;
    for (const auto& source : sources) {
        metrics.push_back(Format("double([/performance_counters/%v_row_%v_data_weight_%v_rate])", source, requestType, duration));
    }

    if (requestType == "write") {
        return metrics.front();
    }
    return Format("(%v)", JoinRange(" + ", metrics.begin(), metrics.end()));
}

THashMap<TString, TString> BuildMetricAliases()
{
    static const std::vector<TString> durations{"10m", "1h"};
    static const std::vector<TString> requestTypes{"write", "read", "lookup", "lookup_cpu"};

    THashMap<TString, TString> aliases;
    for (const auto& duration : durations) {
        for (const auto& requestType : requestTypes) {
            EmplaceOrCrash(
                aliases,
                Format("%v_%v", requestType, duration),
                BuildMetric(requestType, duration));
        }
    }

    return aliases;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TString ReplaceAliases(TString metric)
{
    static const auto aliases = BuildMetricAliases();

    for (const auto& [alias, replacement] : aliases) {
        SubstGlobal(metric, alias, replacement);
    }

    return metric;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
