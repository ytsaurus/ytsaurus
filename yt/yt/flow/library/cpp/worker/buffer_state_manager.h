#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/partition_buffer_state.h>
#include <yt/yt/flow/library/cpp/common/stream_inflight_limits.h>

#include <yt/yt/flow/library/cpp/buffers/public.h>

#include <yt/yt/core/actions/invoker.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

struct TJobStreamLimitUsageStates
{
    TStreamLimitUsageStateMap Input;
    TStreamLimitUsageStateMap Output;
    NFlow::TEpochCycleTrackerPtr InputEpochCycleTracker;
    THashMap<TStreamId, NFlow::TOfferedRateEstimatorPtr> InputOfferedRateEstimators;
};

struct IBufferStateManager
    : public TRefCounted
{
    virtual TJobStreamLimitUsageStates RegisterJob(TJobId jobId, const TJobSpecPtr& jobSpec) = 0;
    //! Idempotent warm-start seeding, fed by the job from its persisted partition
    //! state once it has been read (which happens after #RegisterJob).
    virtual void SeedJob(TJobId jobId, const TPartitionBufferWarmup& bufferWarmup) = 0;
    //! Current converged sizing of the job, persisted by the job into its partition
    //! state; empty when the v2 strategy is off.
    virtual TPartitionBufferWarmup GetJobWarmup(TJobId jobId) = 0;
    virtual void RemoveJob(TJobId jobId) = 0;
    //! Whether the v2 strategy is on; the warmup machinery is a no-op otherwise.
    virtual bool IsV2Enabled() = 0;
    virtual TDuration GetWarmupRefreshPeriod() = 0;
    virtual void Reconfigure(TDynamicBufferStateManagerSpecPtr dynamicSpec) = 0;
    virtual void ManageBuffers() = 0;
    virtual void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBufferStateManager);

////////////////////////////////////////////////////////////////////////////////

//! The per-job computation-facing view over #manager (see IPartitionBufferState).
IPartitionBufferStatePtr CreatePartitionBufferState(
    IBufferStateManagerPtr manager,
    TJobId jobId,
    TStreamLimitUsageStateMap outputStreamLimitUsageStates);

IBufferStateManagerPtr CreateBufferStateManager(
    IInvokerPtr invoker,
    IJobDirectoryPtr jobDirectory,
    TDynamicBufferStateManagerSpecPtr dynamicSpec,
    std::function<TInstant()> timeProvider = [] {
        return TInstant::Now();
    },
    std::vector<TWorkerGroupId> workerGroups = {}, bool enablePeriodicManagement = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
