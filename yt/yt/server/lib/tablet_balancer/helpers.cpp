#include "helpers.h"

#include <library/cpp/yt/logging/logger.h>

#include <util/string/join.h>
#include <util/string/split.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

namespace {

std::string BuildMetric(const std::string& requestType, const std::string& duration)
{
    static const std::vector<std::string> sources{"dynamic", "static_chunk", "static_hunk_chunk"};

    if (requestType == "lookup_cpu" || requestType == "select_cpu") {
        return Format("double([/performance_counters/%v_time_%v_rate])", requestType, duration);
    }

    std::vector<std::string> metrics;
    for (const auto& source : sources) {
        metrics.push_back(Format("double([/performance_counters/%v_row_%v_data_weight_%v_rate])", source, requestType, duration));
    }

    if (requestType == "write") {
        return metrics.front();
    }
    return Format("(%v)", JoinSeq(" + ", metrics));
}

THashMap<std::string, std::string> BuildMetricAliases()
{
    static const std::vector<std::string> durations{"10m", "1h"};
    static const std::vector<std::string> requestTypes{"write", "read", "lookup", "lookup_cpu", "select_cpu"};

    THashMap<std::string, std::string> aliases;
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

std::string ReplaceAliases(const std::string& metric)
{
    static const auto aliases = BuildMetricAliases();

    std::vector<std::string> parts;
    StringSplitter(metric).Split('[').Collect(&parts);
    std::vector<std::string> resultMetric;

    for (int partIndex = 0; partIndex < std::ssize(parts); ++partIndex) {
        std::vector<std::string> subParts;
        StringSplitter(parts[partIndex]).Split(']').Collect(&subParts);

        THROW_ERROR_EXCEPTION_IF(subParts.size() > 2 || partIndex == 0 && subParts.size() > 1,
            "Unexpected token. Invalid parentheses substring %Qv in string %Qv",
            parts[partIndex],
            metric);

        if (partIndex == 0 && subParts.size() == 1 || subParts.size() == 2) {
            for (const auto& [alias, replacement] : aliases) {
                SubstGlobal(subParts.back(), alias, replacement);
            }
        }
        resultMetric.push_back(JoinSeq(']', subParts));
    }

    return JoinSeq('[', resultMetric);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
