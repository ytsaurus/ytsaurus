#include "packing.h"

#include "packing_detail.h"
#include "scheduling_heartbeat_context.h"

#include <yt/yt/server/scheduler/strategy/pool_tree_element.h>

#include <yt/yt/ytlib/scheduler/config.h>
#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TPackingNodeResourcesSnapshot::TPackingNodeResourcesSnapshot(
    const TJobResources& usage,
    const TJobResources& limits,
    TDiskQuota diskQuota)
    : Usage_(usage)
    , Limits_(limits)
    , DiskQuota_(diskQuota)
{ }

TPackingHeartbeatSnapshot::TPackingHeartbeatSnapshot(
    NProfiling::TCpuInstant time,
    const TPackingNodeResourcesSnapshot& resources)
    : Time_(time)
    , Resources_(resources)
{ }

bool TPackingHeartbeatSnapshot::CanSchedule(const TJobResourcesWithQuota& allocationResourcesWithQuota) const
{
    return Dominates(Resources().Free(), allocationResourcesWithQuota);
}

TPackingHeartbeatSnapshot CreateHeartbeatSnapshot(const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext)
{
    TDiskQuota diskQuota;
    for (const auto& locationResources : schedulingHeartbeatContext->DiskResources().DiskLocationResources) {
        int mediumIndex = locationResources.MediumIndex;
        i64 freeDiskSpace = locationResources.Limit - locationResources.Usage;
        auto it = diskQuota.DiskSpacePerMedium.find(mediumIndex);
        if (it == diskQuota.DiskSpacePerMedium.end()) {
            diskQuota.DiskSpacePerMedium.emplace(mediumIndex, freeDiskSpace);
        } else {
            it->second = std::max(it->second, freeDiskSpace);
        }
    }

    auto resourcesSnapshot = TPackingNodeResourcesSnapshot(
        schedulingHeartbeatContext->ResourceUsage(),
        schedulingHeartbeatContext->ResourceLimits(),
        diskQuota);

    return TPackingHeartbeatSnapshot(schedulingHeartbeatContext->GetNow(), resourcesSnapshot);
}

////////////////////////////////////////////////////////////////////////////////

void TPackingStatistics::RecordHeartbeat(
    const TPackingHeartbeatSnapshot& heartbeatSnapshot,
    const TStrategyPackingConfigPtr& config)
{
    auto guard = Guard(Lock_);

    WindowOfHeartbeats_.push_front(heartbeatSnapshot);
    if (std::ssize(WindowOfHeartbeats_) > config->MaxHeartbeatWindowSize) {
        WindowOfHeartbeats_.pop_back();
    }
}

bool TPackingStatistics::CheckPacking(
    const TPoolTreeOperationElement* operationElement,
    const TPackingHeartbeatSnapshot& heartbeatSnapshot,
    const TJobResourcesWithQuota& allocationResourcesWithQuota,
    const TJobResources& totalResourceLimits,
    const TStrategyPackingConfigPtr& config) const
{
    auto metric = [&] (const auto& snapshot, const auto& allocationResources) -> double {
        return PackingMetric(snapshot.Resources(), allocationResources, totalResourceLimits, config);
    };

    auto betterThan = [&] (double lhsMetricValue, double rhsMetricValue) -> bool {
        return lhsMetricValue < rhsMetricValue - config->AbsoluteMetricValueTolerance
            && lhsMetricValue < rhsMetricValue / config->RelativeMetricValueTolerance;
    };

    auto timeThreshold = heartbeatSnapshot.Time() - NProfiling::DurationToCpuDuration(config->MaxHeartbeatAge);
    auto currentMetricValue = metric(heartbeatSnapshot, allocationResourcesWithQuota);
    int betterPastSnapshots = 0;

    std::deque<TPackingHeartbeatSnapshot> windowOfHeartbeats;
    {
        auto guard = Guard(Lock_);
        windowOfHeartbeats = WindowOfHeartbeats_;
    }

    for (const auto& pastSnapshot : windowOfHeartbeats) {
        // NB: Snapshots in WindowOfHeartbeats_ are not necessarily strictly monotonous in time.
        if (pastSnapshot.Time() < timeThreshold) {
            continue;
        }
        if (!pastSnapshot.CanSchedule(allocationResourcesWithQuota)) {
            continue;
        }
        auto pastSnapshotMetricValue = metric(pastSnapshot, allocationResourcesWithQuota);
        if (betterThan(pastSnapshotMetricValue, currentMetricValue)) {
            betterPastSnapshots++;
        }
    }

    bool decision = std::ssize(WindowOfHeartbeats_) >= config->MinWindowSizeForSchedule
        && betterPastSnapshots < config->MaxBetterPastSnapshots;

    YT_ELEMENT_LOG_DETAILED(
        operationElement,
        "Packing decision made (BetterPastSnapshots: %v, CurrentMetricValue: %v, "
        "WindowSize: %v, NodeResources: %v, AllocationResources: %v, Decision: %v)",
        betterPastSnapshots,
        currentMetricValue,
        WindowOfHeartbeats_.size(),
        // TODO(ignat): use TMediumDirectory to log disk resources.
        FormatResources(heartbeatSnapshot.Resources().Free().ToJobResources()),
        FormatResources(allocationResourcesWithQuota),
        decision);

    return decision;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
