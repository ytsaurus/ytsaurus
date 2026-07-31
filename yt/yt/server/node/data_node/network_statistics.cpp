#include "network_statistics.h"

#include "private.h"
#include "config.h"

namespace NYT::NDataNode {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TNetworkStatistics::TNetworkStatistics(TDataNodeConfigPtr config)
    : Config_(std::move(config))
{ }

TNetworkCounters* TNetworkStatistics::GetOrCreateCounters(const std::string& name)
{
    return Counters_.FindOrInsert(name, [&] {
        auto counters = New<TNetworkCounters>();
        auto profiler = DataNodeProfiler().WithTag("network", name);
        counters->ThrottledReadsCounter = profiler.Counter("/net_throttled_reads");
        counters->ThrottledWritesCounter = profiler.Counter("/net_throttled_writes");
        return counters;
    }).first->Get();
}

void TNetworkStatistics::IncrementReadThrottlingCounter(const std::string& name)
{
    auto* counters = GetOrCreateCounters(name);

    counters->ReadUpdateTime = GetCpuInstant();
    counters->ThrottledReadsCounter.Increment();
}

void TNetworkStatistics::IncrementWriteThrottlingCounter(const std::string& name)
{
    auto* counters = GetOrCreateCounters(name);

    counters->WriteUpdateTime = GetCpuInstant();
    counters->ThrottledWritesCounter.Increment();
}

void TNetworkStatistics::UpdateStatistics(NNodeTrackerClient::NProto::TClusterNodeStatistics* statistics)
{
    Counters_.IterateReadOnly([&] (const auto& name, const auto& counters) {
        auto* network = statistics->add_network();
        network->set_network(name);

        auto now = GetCpuInstant();
        auto throttlingDuration = 2 * DurationToCpuDuration(Config_->NetOutThrottlingDuration);
        auto readUpdateTime = counters->ReadUpdateTime.load();
        auto writeUpdateTime = counters->WriteUpdateTime.load();
        network->set_throttling_reads(readUpdateTime != 0 && now < readUpdateTime + throttlingDuration);
        network->set_throttling_writes(writeUpdateTime != 0 && now < writeUpdateTime + throttlingDuration);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
