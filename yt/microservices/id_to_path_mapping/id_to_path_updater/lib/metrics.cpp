#include "metrics.h"

#include <yt/cpp/roren/interface/roren.h>

TClusterMetricsFn::TClusterMetricsFn(TString counterName)
    : CounterName_(std::move(counterName))
{ }

void TClusterMetricsFn::Do(TInputRow&& row, NRoren::TOutput<TOutputRow>& output)
{
    GetClusterCounter(row.GetCluster()).Increment();

    output.Add(std::move(row));
}

NYT::NProfiling::TCounter& TClusterMetricsFn::GetClusterCounter(TString cluster)
{
    auto it = Counters_.find(cluster);
    if (it == Counters_.end()) {
        const auto& profiler = GetExecutionContext()->GetProfiler();
        std::tie(it, std::ignore) = Counters_.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(cluster),
            std::forward_as_tuple(profiler.WithTag("served_cluster", cluster).Counter(CounterName_))
        );
    }
    return it->second;
}
