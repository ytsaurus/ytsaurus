#include "packing.h"

#include "fair_share_tree_element.h"
#include "private.h"
#include "packing_detail.h"

namespace NYT::NScheduler {

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

bool TPackingHeartbeatSnapshot::CanSchedule(const TJobResourcesWithQuota& jobResourcesWithQuota) const
{
    return Dominates(Resources().Free(), jobResourcesWithQuota);
}

TPackingHeartbeatSnapshot CreateHeartbeatSnapshot(const ISchedulingContextPtr& schedulingContext)
{
    TDiskQuota diskQuota;
    for (const auto& locationResources : schedulingContext->DiskResources().disk_location_resources()) {
        int mediumIndex = locationResources.medium_index();
        i64 freeDiskSpace = locationResources.limit() - locationResources.usage();
        auto it = diskQuota.DiskSpacePerMedium.find(mediumIndex);
        if (it == diskQuota.DiskSpacePerMedium.end()) {
            diskQuota.DiskSpacePerMedium.emplace(mediumIndex, freeDiskSpace);
        } else {
            it->second = std::max(it->second, freeDiskSpace);
        }
    }

    auto resourcesSnapshot = TPackingNodeResourcesSnapshot(
        schedulingContext->ResourceUsage(),
        schedulingContext->ResourceLimits(),
        diskQuota);

    return TPackingHeartbeatSnapshot(schedulingContext->GetNow(), resourcesSnapshot);
}

////////////////////////////////////////////////////////////////////////////////

void TPackingStatistics::RecordHeartbeat(
    const TPackingHeartbeatSnapshot& heartbeatSnapshot,
    const TFairShareStrategyPackingConfigPtr& config)
{
    auto guard = Guard(Lock_);

    WindowOfHeartbeats_.push_front(heartbeatSnapshot);
    if (std::ssize(WindowOfHeartbeats_) > config->MaxHeartbeatWindowSize) {
        WindowOfHeartbeats_.pop_back();
    }
}

bool TPackingStatistics::CheckPacking(
    const TSchedulerOperationElement* operationElement,
    const TPackingHeartbeatSnapshot& heartbeatSnapshot,
    const TJobResourcesWithQuota& jobResourcesWithQuota,
    const TJobResources& totalResourceLimits,
    const TFairShareStrategyPackingConfigPtr& config) const
{
    auto metric = [&] (const auto& snapshot, const auto& jobResources) -> double {
        return PackingMetric(snapshot.Resources(), jobResources, totalResourceLimits, config);
    };

    auto betterThan = [&] (double lhsMetricValue, double rhsMetricValue) -> bool {
        return lhsMetricValue < rhsMetricValue - config->AbsoluteMetricValueTolerance
            && lhsMetricValue < rhsMetricValue / config->RelativeMetricValueTolerance;
    };

    auto timeThreshold = heartbeatSnapshot.Time() - NProfiling::DurationToCpuDuration(config->MaxHeartbeatAge);
    auto currentMetricValue = metric(heartbeatSnapshot, jobResourcesWithQuota);
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
        if (!pastSnapshot.CanSchedule(jobResourcesWithQuota)) {
            continue;
        }
        auto pastSnapshotMetricValue = metric(pastSnapshot, jobResourcesWithQuota);
        if (betterThan(pastSnapshotMetricValue, currentMetricValue)) {
            betterPastSnapshots++;
        }
    }

    bool decision = std::ssize(WindowOfHeartbeats_) >= config->MinWindowSizeForSchedule
        && betterPastSnapshots < config->MaxBetterPastSnapshots;

    YT_ELEMENT_LOG_DETAILED(operationElement,
        "Packing decision made (BetterPastSnapshots: %v, CurrentMetricValue: %v, "
        "WindowSize: %v, NodeResources: %v, JobResources: %v, Decision: %v)",
        betterPastSnapshots,
        currentMetricValue,
        WindowOfHeartbeats_.size(),
        // TODO(ignat): use TMediumDirectory to log disk resources.
        FormatResources(heartbeatSnapshot.Resources().Free().ToJobResources()),
        decision);

    return decision;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
