#pragma once

#include "public.h"

#include <yt/yt/server/scheduler/strategy/policy/public.h>

#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/ytlib/scheduler/disk_resources.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Refactor this interface and maybe think of better naming.
/*!
 *  \note Thread affinity: any
 */
struct ISchedulingOperationController
    : public virtual TRefCounted
{
    //! Returns epoch of the controller.
    virtual TControllerEpoch GetEpoch() const = 0;

    //! Called during heartbeat processing to send a schedule allocation request to the controller.
    virtual TFuture<TControllerScheduleAllocationResultPtr> ScheduleAllocation(
        const NPolicy::ISchedulingHeartbeatContextPtr& context,
        const TJobResources& availableResources,
        const TDiskResources& availableDiskResources,
        const std::string& treeId,
        const NYPath::TYPath& poolPath,
        std::optional<TDuration> waitingForResourcesOnNodeTimeout) = 0;

    //! Called during scheduling to notify the controller that a (nonscheduled) allocation has been aborted.
    virtual void OnNonscheduledAllocationAborted(
        TAllocationId allocationId,
        EAbortReason abortReason,
        TControllerEpoch allocationEpoch) = 0;

    //! Returns the total resources that are additionally needed.
    virtual TCompositeNeededResources GetNeededResources() const = 0;

    //! Initiates updating min needed resources estimates.
    //! Note that the actual update may happen in background.
    virtual void UpdateGroupedNeededResources() = 0;

    //! Returns the latest grouped needed resources.
    virtual TAllocationGroupResourcesMap GetGroupedNeededResources() const = 0;

    //! Returns initial grouped needed resources (right after materialization).
    virtual TAllocationGroupResourcesMap GetInitialGroupedNeededResources() const = 0;

    // TODO(eshcherbin): Move it to some other place.
    //! Returns the mode which says how to preempt allocations of this operation.
    virtual EPreemptionMode GetPreemptionMode() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulingOperationController)

////////////////////////////////////////////////////////////////////////////////

class TOperationController
    : public TRefCounted
{
public:
    TOperationController(
        const IOperationPtr& operation,
        const TStrategyOperationControllerConfigPtr& config,
        const std::vector<IInvokerPtr>& nodeShardInvokers);

    void OnScheduleAllocationStarted(const NPolicy::ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext);
    void OnScheduleAllocationFinished(const NPolicy::ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext);

    TControllerEpoch GetEpoch() const;

    TCompositeNeededResources GetNeededResources() const;
    TAllocationGroupResourcesMap GetGroupedNeededResources() const;
    TAllocationGroupResourcesMap GetInitialGroupedNeededResources() const;
    TJobResources GetAggregatedMinNeededAllocationResources() const;
    TJobResources GetAggregatedInitialMinNeededAllocationResources() const;

    void UpdateGroupedNeededResources();

    void UpdateConcurrentScheduleAllocationThrottlingLimits(const TStrategyOperationControllerConfigPtr& config);
    bool CheckMaxScheduleAllocationCallsOverdraft(int maxScheduleAllocationCalls) const;
    bool IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(const NPolicy::ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext) const;
    bool IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(const NPolicy::ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext) const;
    bool HasRecentScheduleAllocationFailure(NProfiling::TCpuInstant now) const;
    bool ScheduleAllocationBackoffObserved() const;

    TControllerScheduleAllocationResultPtr ScheduleAllocation(
        const NPolicy::ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TJobResources& availableResources,
        const TDiskResources& availableDiskResources,
        TDuration timeLimit,
        const std::string& treeId,
        const TString& poolPath,
        std::optional<TDuration> waitingForResourcesOnNodeTimeout);

    // TODO(eshcherbin): Move to private.
    void AbortAllocation(
        TAllocationId allocationId,
        EAbortReason abortReason,
        TControllerEpoch allocationEpoch);

    void OnScheduleAllocationFailed(
        NProfiling::TCpuInstant now,
        const std::string& treeId,
        const TControllerScheduleAllocationResultPtr& scheduleAllocationResult);

    bool IsSaturatedInTentativeTree(
        NProfiling::TCpuInstant now,
        const std::string& treeId,
        TDuration saturationDeactivationTimeout) const;

    void UpdateConfig(const TStrategyOperationControllerConfigPtr& config);
    TStrategyOperationControllerConfigPtr GetConfig();

    void SetDetailedLogsEnabled(bool enabled);

private:
    const ISchedulingOperationControllerPtr Controller_;
    const TOperationId OperationId_;

    const NLogging::TLogger Logger;

    NThreading::TReaderWriterSpinLock ConfigLock_;
    TAtomicIntrusivePtr<TStrategyOperationControllerConfig> Config_;

    struct alignas(CacheLineSize) TStateShard
    {
        mutable std::atomic<int> ScheduleAllocationCallsSinceLastUpdate = 0;

        char Padding[CacheLineSize];

        int ConcurrentScheduleAllocationCalls = 0;
        int MaxConcurrentControllerScheduleAllocationCalls = std::numeric_limits<int>::max();
        TDuration ConcurrentScheduleAllocationExecDuration;
        TDuration MaxConcurrentControllerScheduleAllocationExecDuration = TDuration::Max();

        TDuration ScheduleAllocationExecDurationEstimate;
    };
    std::array<TStateShard, MaxNodeShardCount> StateShards_;

    const std::vector<IInvokerPtr> NodeShardInvokers_;

    std::atomic<bool> EnableConcurrentScheduleAllocationExecDurationThrottling_ = false;

    mutable int ScheduleAllocationCallsOverdraft_ = 0;

    std::atomic<NProfiling::TCpuDuration> ScheduleAllocationControllerThrottlingBackoff_;
    std::atomic<NProfiling::TCpuInstant> ScheduleAllocationBackoffDeadline_ = ::Min<NProfiling::TCpuInstant>();
    std::atomic<bool> ScheduleAllocationBackoffObserved_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SaturatedTentativeTreesLock_);
    THashMap<std::string, NProfiling::TCpuInstant> TentativeTreeIdToSaturationTime_;

    bool DetailedLogsEnabled_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
