#pragma once

#include "private.h"
#include "scheduling_context.h"

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <deque>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPackingNodeResourcesSnapshot
{
public:
    DEFINE_BYREF_RO_PROPERTY(TJobResources, Usage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, Limits);
    DEFINE_BYREF_RO_PROPERTY(TDiskQuota, DiskQuota);

public:
    TPackingNodeResourcesSnapshot() = default;
    TPackingNodeResourcesSnapshot(const TJobResources& usage, const TJobResources& limits, TDiskQuota diskQuota);


    Y_FORCE_INLINE TJobResourcesWithQuota Free() const
    {
        TJobResourcesWithQuota availableResources = Limits_ - Usage_;
        availableResources.DiskQuota() = DiskQuota_;
        return availableResources;
    }
};

class TPackingHeartbeatSnapshot
{
public:
    DEFINE_BYREF_RO_PROPERTY(NProfiling::TCpuInstant, Time);
    DEFINE_BYREF_RO_PROPERTY(TPackingNodeResourcesSnapshot, Resources);

public:
    TPackingHeartbeatSnapshot(NProfiling::TCpuInstant time, const TPackingNodeResourcesSnapshot& resources);

    bool CanSchedule(const TJobResourcesWithQuota& allocationResourcesWithQuota) const;
};

TPackingHeartbeatSnapshot CreateHeartbeatSnapshot(const ISchedulingContextPtr& schedulingContext);

////////////////////////////////////////////////////////////////////////////////

class TPackingStatistics
{
public:
    void RecordHeartbeat(
        const TPackingHeartbeatSnapshot& heartbeatSnapshot,
        const TStrategyPackingConfigPtr& config);

    bool CheckPacking(
        const TSchedulerOperationElement* operationElement,
        const TPackingHeartbeatSnapshot& heartbeatSnapshot,
        const TJobResourcesWithQuota& allocationResourcesWithQuota,
        const TJobResources& totalResourceLimits,
        const TStrategyPackingConfigPtr& config) const;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::deque<TPackingHeartbeatSnapshot> WindowOfHeartbeats_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
