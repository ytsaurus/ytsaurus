#include "network_statistics.h"

#include "private.h"
#include "config.h"

#include <yt/core/profiling/profile_manager.h>

namespace NYT {
namespace NDataNode {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TNetworkStatistics::TNetworkStatistics(TDataNodeConfigPtr config)
    : Config_(config)
{ }

void TNetworkStatistics::IncrementReadThrottlingCounter(const TString& name)
{
    while (true) {
        {
            TReaderGuard guard(Lock_);
            auto it = Counters_.find(name);
            if (it != Counters_.end()) {
                DataNodeProfiler.Increment(it->second.ThrottledReadsCounter);
                break;
            }
        }

        TWriterGuard guard(Lock_);
        if (Counters_.find(name) == Counters_.end()) {
            TTagIdList tagIds{
                TProfileManager::Get()->RegisterTag("network", name)
            };

            auto& counters = Counters_[name];
            counters.ThrottledReadsCounter = TSimpleCounter(
                "/net_throttled_reads",
                tagIds,
                Config_->NetOutThrottleCounterInterval);
        }
    }
}

void TNetworkStatistics::UpdateStatistics(NNodeTrackerClient::NProto::TNodeStatistics* statistics)
{
    TReaderGuard guard(Lock_);
    for (auto& counter : Counters_) {
        auto network = statistics->add_network();
        network->set_network(counter.first);

        auto deadline = counter.second.ThrottledReadsCounter.GetUpdateDeadline();
        network->set_throttling_reads(
            GetCpuInstant() < deadline + 2 * DurationToCpuDuration(Config_->NetOutThrottleCounterInterval));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
