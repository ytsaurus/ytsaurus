#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/partition_buffer_state.h>
#include <yt/yt/flow/library/cpp/common/stream_buffer_duration.h>
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

//! The v2 step that determined a stream's issued limit; later steps win.
DEFINE_ENUM(EBufferLimitDecision,
    (Override)
    (NoBacklog)
    (UsedPlusHeadroom)
    (DemandFloor)
    (DrainCap)
    (StreamFloor)
    (DemandBackedScale)
    (SpeculativeScale)
    (PoolRoom)
    (ResidentMinusOne)
    (HysteresisKeptPrevious)
);

//! Sizing of one stream side computed by one manage tick, in inflated bytes and
//! inflated bytes per second.
struct TBufferStreamDiagnostics
    : public NYTree::TYsonStructLite
{
    double MeasuredDemand{};
    //! The controller's 1-day speed per partition.
    double BaselineDemand{};
    //! Output side: produced over consumed inflated rate and its product with the
    //! job input demand (the fast path).
    double IORatio{};
    double FastPathDemand{};
    double OfferedRate{};
    //! The drain rate estimator buckets last #EpochCycleSeconds.
    std::optional<double> MaxDrainRate;
    i64 MaxDrainRateFilledBucketCount{};
    double SeedDemand{};
    double Demand{};
    double SmoothedDemand{};
    double EpochCycleSeconds{};
    double MaxDurationSeconds{};
    bool AutomaticDurationApplied{};
    std::optional<double> OverrideLimit;
    bool HasBacklog{};
    double ActiveBytes{};
    double PeakInflightBytes{};
    double UsedInflatedBytes{};
    double PendingInflatedBytes{};
    double Utilization{};
    i64 Headroom{};
    double LastProbeDemand{};
    i64 DemandFloor{};
    i64 DemandDrainCap{};
    double FairShareProbe{};
    i64 DrainCap{};
    i64 StreamFloor{};
    i64 WantedLimit{};
    i64 DemandBackedWantedLimit{};
    double SpeculativeResident{};
    double ResidentAwareSpeculativeWantedLimit{};
    double DemandBackedScale{};
    double SpeculativeScale{};
    //! Pool minus the other streams' used bytes; negative when they exhaust the pool.
    std::optional<double> PoolRoom;
    i64 FinalWantedLimit{};
    bool Published{};
    i64 IssuedLimit{};
    //! V2 only.
    std::optional<EBufferLimitDecision> LimitDecision;
    //! Input side only.
    std::optional<TInputBufferDiagnostics> InputBuffer;

    REGISTER_YSON_STRUCT_LITE(TBufferStreamDiagnostics);

    static void Register(TRegistrar registrar);
};

//! Worker totals of one side computed by one manage tick.
struct TBufferPoolDiagnostics
    : public NYTree::TYsonStructLite
{
    i64 EffectivePool{};
    double TotalDemand{};
    double DemandBackedTotal{};
    double UsedTotal{};
    double DemandBackedScale{};
    // Output side only.
    double SpeculativeResident{};
    double SpeculativeGrowthWanted{};
    double SpeculativeRoom{};
    double SpeculativeGrowthScale{};
    double RemainingPool{};
    double SpeculativeWantedTotal{};
    double SpeculativeScale{};

    REGISTER_YSON_STRUCT_LITE(TBufferPoolDiagnostics);

    static void Register(TRegistrar registrar);
};

struct TJobBufferDiagnostics
    : public NYTree::TYsonStructLite
{
    TComputationId ComputationId;
    std::optional<double> MedianEpochCycleSeconds;
    double SmoothedEpochCycleSeconds{};
    double InputDemand{};
    double InputInflatedRate{};
    THashMap<TStreamId, TBufferStreamDiagnostics> InputStreams;
    THashMap<TStreamId, TBufferStreamDiagnostics> OutputStreams;

    REGISTER_YSON_STRUCT_LITE(TJobBufferDiagnostics);

    static void Register(TRegistrar registrar);
};

//! Immutable once published.
struct TBufferDiagnostics
    : public NYTree::TYsonStruct
{
    TInstant ManageInstant;
    bool EnableV2 = false;
    TBufferPoolDiagnostics InputPool;
    TBufferPoolDiagnostics OutputPool;
    THashMap<TJobId, TJobBufferDiagnostics> Jobs;

    REGISTER_YSON_STRUCT(TBufferDiagnostics);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBufferDiagnostics);

////////////////////////////////////////////////////////////////////////////////

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
    virtual void Reconfigure(
        TDynamicBufferStateManagerSpecPtr dynamicSpec,
        TStreamBufferDurationSnapshotPtr durationSnapshot) = 0;
    virtual void ManageBuffers() = 0;
    virtual void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo) = 0;
    //! Snapshot of the last manage tick; null unless orchid diagnostics are enabled.
    /*!
     *  \note Thread affinity: any
     */
    virtual TBufferDiagnosticsPtr GetDiagnostics() = 0;
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
