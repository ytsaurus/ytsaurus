#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/computation_controller.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/throughput_autoscaler/processing_meter.h>
#include <yt/yt/flow/library/cpp/throughput_autoscaler/throughput_autoscaler.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NPartitioning {

////////////////////////////////////////////////////////////////////////////////

//! Persisted coordinator state for one computation.
struct TComputationPartitioningState
    : public NYTree::TYsonStruct
{
    std::optional<TVersion> LastAppliedSinkTopologyVersion;
    //! Source partitions to remove after their key disappears and they finish draining.
    THashSet<TPartitionId> RetiringSourcePartitions;
    TThroughputFeedbackState ThroughputAutoscaler;
    NYson::TYsonString ThroughputDemandHistory;

    REGISTER_YSON_STRUCT(TComputationPartitioningState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TComputationPartitioningState);

////////////////////////////////////////////////////////////////////////////////

//! Persisted pipeline-wide partitioning state.
struct TPartitioningCoordinatorState
    : public NYTree::TYsonStruct
{
    THashMap<TComputationId, TComputationPartitioningStatePtr> Computations;

    REGISTER_YSON_STRUCT(TPartitioningCoordinatorState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPartitioningCoordinatorState);

////////////////////////////////////////////////////////////////////////////////

//! Owns partitioning policy and all execution-layout mutations for the pipeline.
class TPartitioningCoordinator
    : public TRefCounted
{
public:
    static constexpr int DefaultMinInputPartitionCount = TPartitioningSpec::DefaultMinPartitionCount;
    static constexpr int DefaultMaxInputPartitionCount = TPartitioningSpec::DefaultMaxPartitionCount;
    static constexpr int DefaultSinkChannelMultiplier = 3;
    static constexpr double DefaultDesiredAveragePartitionCpuLoad = 0.1;
    static constexpr double DefaultDesiredAveragePartitionMemoryUsed = 300'000'000;
    static constexpr double DefaultDesiredAveragePartitionMessagesPerSecond = 10'000;
    static constexpr double DefaultDesiredAveragePartitionBytesPerSecond = 2'000'000;
    static constexpr double DefaultDesiredAveragePartitionTimerCount = 50'000;
    static constexpr double DefaultAllowedPartitionCountDeviation = 1.1;
    static constexpr TDuration DefaultPartitionCountDoubleDelay = TDuration::Minutes(20);
    static constexpr TDuration DefaultPartitionCountHalfDelay = TDuration::Minutes(200);

    TPartitioningCoordinator(
        const THashMap<TComputationId, IComputationControllerPtr>& computationControllers,
        TPipelineSpecPtr pipelineSpec,
        IInitContextPtr initContext,
        NLogging::TLogger logger,
        NLogging::TLogger publicLogger,
        NProfiling::TProfiler profiler = {},
        IStatusProfilerPtr statusProfiler = {});

    void BeginIteration(const TFlowViewPtr& committedFlowView);
    //! Returns whether the computation's current partitions cover its desired partitioning.
    bool IsFullCoverage(
        const TComputationId& computationId,
        const std::vector<TPartitionId>& computationPartitions,
        const TFlowViewPtr& flowView);
    void DoPartitioning(const TFlowViewPtr& flowView);
    void Sync();
    void Commit();

    //! Peak-hold envelope follower used to damp partition-count reductions.
    static double ApplyPeakHoldRelease(
        double previous,
        double target,
        TDuration elapsed,
        TDuration releaseHalfDelay);

private:
    struct TGroupedPartitions
    {
        THashMap<TPartitionId, TKeyRange> RangePartitions;
        THashMap<TPartitionId, TKey> KeyPartitions;
        THashSet<TPartitionId> BadPartitions;
        THashSet<TPartitionId> InterruptingPartitions;
    };

    struct TPreparedComputationPartitioning
    {
        TGroupedPartitions Grouped;
        IComputationController::TPartitioningDescription Description;
    };

    struct TCriterionEmaState
    {
        double ProposedCount{};
        TInstant UpdatedAt;
    };

    struct TThroughputSensors
    {
        NProfiling::TGauge Demand;
        NProfiling::TGauge SteadyDemand;
        NProfiling::TGauge PeakSteadyDemand;
        NProfiling::TGauge Processed;
        NProfiling::TGauge Capacity;
        NProfiling::TGauge DemandKnown;
        NProfiling::TGauge SteadyDemandKnown;
        NProfiling::TGauge ProcessedKnown;
        NProfiling::TGauge CapacityKnown;
        NProfiling::TGauge TargetPartitionCount;
        NProfiling::TGauge TargetPartitionCountKnown;
        NProfiling::TGauge AnchorPartitionCount;
        NProfiling::TGauge AnchorCapacity;
        NProfiling::TGauge AnchorKnown;
        NProfiling::TGauge ExplorationOriginPartitionCount;
        NProfiling::TGauge ExplorationOriginProcessed;
        NProfiling::TGauge ExplorationBestProcessed;
        NProfiling::TGauge ExplorationPartitionCountLimit;
        NProfiling::TGauge ExplorationKnown;
        NProfiling::TGauge SearchPhase;
        NProfiling::TGauge CapacityDegraded;
        NProfiling::TGauge RetryWarmupSeconds;
        NProfiling::TGauge WarmupReady;
        NProfiling::TGauge Repartitioning;

        explicit TThroughputSensors(const NProfiling::TProfiler& profiler);
    };

    struct TCriterionSensors
    {
        NProfiling::TGauge Average;
        NProfiling::TGauge ProposedPartitionCount;
        NProfiling::TCounter DecisionCount;

        explicit TCriterionSensors(const NProfiling::TProfiler& profiler);
    };

    struct TComputationState
    {
        IComputationControllerPtr Controller;
        TComputationSpecPtr Spec;
        NLogging::TLogger Logger;
        NLogging::TLogger PublicLogger;

        NProfiling::TProfiler Profiler;
        NProfiling::TGauge ProposedPartitionCountGauge;
        std::optional<TThroughputSensors> ThroughputSensors;
        THashMap<EPartitioningCriterion, TCriterionSensors> CriterionSensors;
        THashMap<std::string, NProfiling::TCounter> RepartitionCounters;

        TInstant LastRepartitionTime = TInstant::Zero();
        TInstant LastObservedCommonRepartitioningInstant = TInstant::Zero();
        THashMap<EPartitioningCriterion, TCriterionEmaState> CriterionProposedCountEma;
        TComputationPartitioningStatePtr PersistedState;
        IStatusErrorStatePtr ThroughputErrorState;
        std::optional<TVersion> PendingAppliedSinkTopologyVersion;
        std::optional<TVersion> PreviousAppliedSinkTopologyVersion;
        std::optional<THashSet<TPartitionId>> PreviousRetiringSourcePartitions;
    };

    void ProfileThroughput(
        TComputationState& state,
        const TThroughputAutoscalerTelemetry& telemetry);

    struct TRangePartitioningParameters
    {
        TComputationId ComputationId;
        TPartitioningSpecPtr PartitioningSpec;
        THashSet<TStreamId> TimerStreamIds;
        bool FirstKeyIsUint = false;
        bool EnableNonUintKey = false;
    };

    struct TInputAutoPartitioningContext
    {
        const TFlowViewPtr& FlowView;
        const THashMap<TPartitionId, TKeyRange>& PartitionRanges;

        bool AllPartitionsHaveStatuses = false;
        bool AllPartitionsHavePivots = false;
        bool AnotherComputationRecentlyRepartitioned = false;
        TDuration NormalFlightDuration;

        i64 ProposedCount = 0;
        bool RecreateNow = false;
        std::string DecisionInitiator;
        std::string DecisionReason;
        THashMap<TPartitionId, double> Weights;
        std::optional<i64> MaxSinkChannelCount;

        std::vector<TKeyRange> NewRanges;
        std::vector<double> NewWeights;

        TInputAutoPartitioningContext(
            const TFlowViewPtr& flowView,
            const THashMap<TPartitionId, TKeyRange>& partitionRanges);
    };

    struct TPendingPartitionChange
    {
        TComputationId ComputationId;
        i64 PreviousPartitionCount = 0;
        i64 NewPartitionCount = 0;
        std::string Initiator;
        std::string Reason;
    };

    void DoComputationPartitioning(
        const TComputationId& computationId,
        TComputationState& state,
        TPreparedComputationPartitioning prepared,
        const TFlowViewPtr& flowView);

    static TGroupedPartitions GroupPartitions(
        const std::vector<TPartitionId>& computationPartitions,
        const TFlowViewPtr& flowView);

    IComputationController::TPartitioningStatus BuildPartitioningStatus(
        const TGroupedPartitions& grouped,
        const TFlowViewPtr& flowView) const;

    TRangePartitioningParameters BuildRangePartitioningParameters(
        TPartitioningSpecPtr partitioningSpec,
        const TComputationId& computationId,
        const TFlowViewPtr& flowView) const;

    static NYTree::IMapNodePtr BuildRangeDynamicPartitionSpec(
        THashSet<TStreamId> blockedOutputStreams);
    static NYTree::IMapNodePtr BuildSourceDynamicPartitionSpec(
        NYTree::IMapNodePtr activeSourceSpec,
        THashSet<TStreamId> blockedOutputStreams,
        bool availabilityGroupUnavailable);

    void InputAutoPartitioningCollectData(
        TComputationState& state,
        TInputAutoPartitioningContext& context) const;
    void InputAutoPartitioningCalculateOptimalCount(
        TComputationState& state,
        const TRangePartitioningParameters& parameters,
        TInputAutoPartitioningContext& context);
    void InputAutoPartitioningBuildRanges(
        const TComputationState& state,
        const TRangePartitioningParameters& parameters,
        TInputAutoPartitioningContext& context) const;
    void InputAutoPartitioningTryRebalance(
        const TComputationState& state,
        const TRangePartitioningParameters& parameters,
        TInputAutoPartitioningContext& context) const;

    void InterruptPartition(
        const TFlowViewPtr& flowView,
        const TPartitionId& partitionId) const;
    void CompletePartition(
        const TFlowViewPtr& flowView,
        const TPartitionId& partitionId) const;
    void CreateSourcePartition(
        const TComputationId& computationId,
        const TFlowViewPtr& flowView,
        const TKey& sourceKey,
        const NYTree::IMapNodePtr& dynamicComputationPartitionSpec,
        const NLogging::TLogger& logger) const;
    void CreateRangePartition(
        const TComputationId& computationId,
        const TFlowViewPtr& flowView,
        const TKey& lowerKey,
        const TKey& upperKey,
        const NYTree::IMapNodePtr& dynamicComputationPartitionSpec,
        const NLogging::TLogger& logger) const;
    void UpdateDynamicPartitionSpec(
        const TFlowViewPtr& flowView,
        const TPartitionId& partitionId,
        const NYTree::IMapNodePtr& dynamicComputationPartitionSpec,
        const NLogging::TLogger& logger) const;

private:
    TMutableStateClient<TPartitioningCoordinatorState> PersistedState_;
    THashMap<TComputationId, TComputationState> Computations_;
    std::vector<TComputationId> ComputationOrder_;
    TInstant LastRepartitioningInstant_ = TInstant::Zero();
    bool EnableThroughputAutoscaler_{};
    bool ThroughputStatusPrepared_ = false;
    TThroughputAutoscaler ThroughputAutoscaler_;
    TProcessingMeter ProcessingMeter_;
    TFlowStatePtr CommittedState_;
    const NLogging::TLogger Logger;
    const NLogging::TLogger PublicLogger;
    std::vector<TPendingPartitionChange> PendingPartitionChanges_;
};

DEFINE_REFCOUNTED_TYPE(TPartitioningCoordinator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NPartitioning
