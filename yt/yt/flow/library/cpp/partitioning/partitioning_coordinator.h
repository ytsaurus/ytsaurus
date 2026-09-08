#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/computation_controller.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

namespace NYT::NFlow::NPartitioning {

////////////////////////////////////////////////////////////////////////////////

//! Persisted coordinator state for one computation.
struct TComputationPartitioningState
    : public NYTree::TYsonStruct
{
    std::optional<TVersion> LastAppliedSinkTopologyVersion;

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
    static constexpr int DefaultMinInputPartitionCount = 3;
    static constexpr int DefaultMaxInputPartitionCount = 20'000;
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
        IInitContextPtr initContext,
        NLogging::TLogger logger,
        NLogging::TLogger publicLogger);

    void BeginIteration();
    //! Returns whether the computation's current partitions cover its desired partitioning.
    bool IsFullCoverage(
        const TComputationId& computationId,
        const std::vector<TPartitionId>& computationPartitions,
        const TFlowViewPtr& flowView);
    void DoPartitioning(const TFlowViewPtr& flowView);
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

    struct TCriterionEmaState
    {
        double ProposedCount{};
        TInstant UpdatedAt;
    };

    struct TComputationState
    {
        IComputationControllerPtr Controller;
        NLogging::TLogger Logger;
        NLogging::TLogger PublicLogger;
        TInstant LastRepartitionTime = TInstant::Zero();
        TInstant LastObservedCommonRepartitioningInstant = TInstant::Zero();
        THashMap<std::string, TCriterionEmaState> CriterionProposedCountEma;
        TComputationPartitioningStatePtr PersistedState;
        std::optional<TVersion> PendingAppliedSinkTopologyVersion;
        std::optional<TVersion> PreviousAppliedSinkTopologyVersion;
    };

    struct TRangePartitioningParameters
    {
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
        THashMap<TPartitionId, double> Weights;
        std::optional<i64> MaxSinkChannelCount;

        std::vector<TKeyRange> NewRanges;
        std::vector<double> NewWeights;

        TInputAutoPartitioningContext(
            const TFlowViewPtr& flowView,
            const THashMap<TPartitionId, TKeyRange>& partitionRanges);
    };

    void DoComputationPartitioning(
        const TComputationId& computationId,
        TComputationState& state,
        const std::vector<TPartitionId>& computationPartitions,
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
    const NLogging::TLogger Logger;
    const NLogging::TLogger PublicLogger;
};

DEFINE_REFCOUNTED_TYPE(TPartitioningCoordinator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NPartitioning
