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

void TNetworkStatistics::IncrementReadThrottlingCounter(const TString& name)
{
    auto counters = Counters_.FindOrInsert(name, [&] {
        auto counters = New<TNetworkCounters>();
        counters->ThrottledReadsCounter = DataNodeProfiler
            .WithTag("network", name)
            .Counter("/net_throttled_reads");
        return counters;
    }).first->Get();

    counters->UpdateTime = GetCpuInstant();
    counters->ThrottledReadsCounter.Increment();
}

void TNetworkStatistics::UpdateStatistics(NNodeTrackerClient::NProto::TClusterNodeStatistics* statistics)
{
    Counters_.IterateReadOnly([&] (const auto& name, const auto& counters) {
        auto* network = statistics->add_network();
        network->set_network(name);

        auto resetAt = counters->UpdateTime.load() + 2 * DurationToCpuDuration(Config_->NetOutThrottlingDuration);
        network->set_throttling_reads(GetCpuInstant() < resetAt);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
