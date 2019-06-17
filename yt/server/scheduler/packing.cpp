#include "private.h"
#include "packing.h"
#include "packing_detail.h"

#include <yt/server/lib/scheduler/config.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

TPackingNodeResourcesSnapshot::TPackingNodeResourcesSnapshot(
    const TJobResources& usage,
    const TJobResources& limits,
    i64 diskQuota)
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
    return
        Dominates(Resources().Free(), jobResourcesWithQuota.ToJobResources()) &&
        Resources().DiskQuota() >= jobResourcesWithQuota.GetDiskQuota();
}

TPackingHeartbeatSnapshot CreateHeartbeatSnapshot(const ISchedulingContextPtr& schedulingContext)
{
    const auto& diskReports = schedulingContext->DiskInfo().disk_reports();
    auto bestDisk = std::max_element(diskReports.begin(), diskReports.end(), [](const auto& lhs, const auto& rhs) {
        return (lhs.limit() - lhs.usage()) < (rhs.limit() - rhs.usage());
    });

    i64 freeSpaceOnBestDisk;
    if (bestDisk != diskReports.end()) {
        freeSpaceOnBestDisk = bestDisk->limit() - bestDisk->usage();
    } else {
        freeSpaceOnBestDisk = 0;
    }

    auto resourcesSnapshot = TPackingNodeResourcesSnapshot(
        schedulingContext->ResourceUsage(),
        schedulingContext->ResourceLimits(),
        freeSpaceOnBestDisk);

    return TPackingHeartbeatSnapshot(schedulingContext->GetNow(), resourcesSnapshot);
}

////////////////////////////////////////////////////////////////////////////////

void TPackingStatistics::RecordHeartbeat(
    const TPackingHeartbeatSnapshot& heartbeatSnapshot,
    const TFairShareStrategyPackingConfigPtr& config)
{
    auto guard = Guard(Lock_);

    WindowOfHeartbeats_.push_front(heartbeatSnapshot);
    if (WindowOfHeartbeats_.size() > config->MaxHearbeatWindowSize) {
        WindowOfHeartbeats_.pop_back();
    }
}

bool TPackingStatistics::CheckPacking(
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

    bool decision = WindowOfHeartbeats_.size() >= config->MinWindowSizeForSchedule
        && betterPastSnapshots < config->MaxBetterPastSnapshots;

    YT_LOG_TRACE(
        "Packing decision made (BetterPastSnapshots: %v, CurrentMetricValue: %v, "
        "WindowSize: %v, NodeResources: %v, JobResources: %v, Decision: %v)",
        betterPastSnapshots,
        currentMetricValue,
        WindowOfHeartbeats_.size(),
        FormatResources(heartbeatSnapshot.Resources().Free()),
        FormatResources(jobResourcesWithQuota.ToJobResources()),
        decision);

    return decision;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
