#pragma once

#include "public.h"

#include "controller_base.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TKey MakeUniversalPartitionKey(const TStreamId& streamId, const TKey& sourceKey);
std::pair<TStreamId, TKey> SplitUniversalPartitionKey(const TKey& partitionKey);

////////////////////////////////////////////////////////////////////////////////

struct TUniversalComputationControllerState
    : public virtual NYTree::TYsonStruct
{
    THashMap<TStreamId, NYTree::INodePtr> Sources;
    THashMap<TSinkId, NYTree::INodePtr> Sinks;

    REGISTER_YSON_STRUCT(TUniversalComputationControllerState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUniversalComputationControllerState);

////////////////////////////////////////////////////////////////////////////////

//! Persisted controller-side partitioning state. It lives in the per-computation job-manager state,
//! so it survives leader failover and job-manager recreation (unlike a plain member).
struct TUniversalComputationControllerPartitioningState
    : public NYTree::TYsonStruct
{
    //! Per-sink target-queue partition (channel) counts observed at the last partition recreation,
    //! keyed by sink id. A change for any sink invalidates that sink's persisted per-partition
    //! producer ids, so it forces a recreation. Tracked per sink (not just the widest) so a reshard
    //! of a non-widest sink is not missed.
    THashMap<TSinkId, i64> LastSinkChannelCounts;

    REGISTER_YSON_STRUCT(TUniversalComputationControllerPartitioningState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUniversalComputationControllerPartitioningState);

////////////////////////////////////////////////////////////////////////////////

class TUniversalComputationController
    : public TComputationControllerBase
{
private:
    struct TExtendedParameters
        : public virtual TComputationControllerBase::TParameters
    {
        REGISTER_YSON_STRUCT(TExtendedParameters);

        static void Register(TRegistrar registrar);
    };

    struct TExtendedDynamicParameters
        : public TComputationControllerBase::TDynamicParameters
    {
        std::optional<int> DesiredPartitionCount;
        std::optional<int> MinPartitionCount;
        std::optional<int> MaxPartitionCount;
        std::optional<int> SinkChannelMultiplier;
        std::optional<double> DesiredAveragePartitionCpuLoad;
        std::optional<double> DesiredAveragePartitionMemoryUsed;
        std::optional<double> DesiredAveragePartitionMessagesPerSecond;
        std::optional<double> DesiredAveragePartitionBytesPerSecond;
        std::optional<double> DesiredAveragePartitionTimerCount;
        std::optional<double> AllowedPartitionCountDeviation;
        std::optional<TDuration> PartitionCountDoubleDelay;
        std::optional<TDuration> PartitionCountHalfDelay;

        REGISTER_YSON_STRUCT(TExtendedDynamicParameters);

        static void Register(TRegistrar registrar);
    };

public:
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TExtendedDynamicParameters);

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

    TUniversalComputationController(
        TComputationControllerContextPtr context,
        TDynamicComputationControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Sync() final;
    void Commit() final;

    void UpdateWatermarkState(TWatermarkStatePtr watermarkState) final;
    // thread safe
    TWatermarkStatePtr GetWatermarkState();

    bool IsFullCoverage(
        const std::vector<TPartitionId>& computationPartitions,
        const TFlowViewPtr& flowView) final;

    void DoPartitioning(
        const std::vector<TPartitionId>& computationPartitions,
        const TFlowViewPtr& flowView) final;

    //! Peak-hold envelope follower used to damp partition-count reductions: the value may grow
    //! instantly (attack) but shrinks only exponentially, halving the remaining gap to the target
    //! every |releaseHalfDelay|. Passing a zero half-delay disables smoothing (returns the target
    //! verbatim). Public for testing.
    static double ApplyPeakHoldRelease(double previous, double target, TDuration elapsed, TDuration releaseHalfDelay);

protected:
    THashMap<std::string, std::vector<TNodeTraverseDataPtr>> GetNodesByAvailabilityGroup(
        const THashMap<TPartitionId, TNodeTraverseDataPtr>& traverseData,
        const TFlowViewPtr& flowView) final;
    std::optional<TNodeTraverseDataPtr> GetFuturePartitionsNodeTraverseData(const TFlowViewPtr& flowView) final;

private:
    static THashMap<TStreamId, ISourceControllerPtr> CreateSources(
        const TComputationControllerContextPtr& context,
        const TComputationSpecPtr& spec,
        const TDynamicComputationSpecPtr& dynamicSpec);
    static THashMap<TSinkId, ISinkControllerPtr> CreateSinks(
        const TComputationControllerContextPtr& context,
        const TComputationSpecPtr& spec,
        const TDynamicComputationSpecPtr& dynamicSpec);

    struct TInputAutoPartitioningContext;
    void InputAutoPartitioningCollectData(TInputAutoPartitioningContext& context) const;
    void InputAutoPartitioningCalculateOptimalCount(TInputAutoPartitioningContext& context);
    void InputAutoPartitioningBuildRanges(TInputAutoPartitioningContext& context) const;
    void InputAutoPartitioningTryRebalance(TInputAutoPartitioningContext& context) const;
    std::optional<THashMap<TKey, NYTree::IMapNodePtr>> GetSourcePartitionKeys() const;
    void ProcessSourcePartitionStatuses(const THashMap<TPartitionId, TKey>& keyPartitions, const TFlowViewPtr& flowView);

    struct TGroupedPartitions
    {
        THashMap<TPartitionId, TKeyRange> RangePartitions;
        THashMap<TPartitionId, TKey> KeyPartitions;
        THashSet<TPartitionId> BadPartitions;
        THashSet<TPartitionId> InterruptingPartitions;
    };

    TGroupedPartitions GroupPartitions(
        const std::vector<TPartitionId>& computationPartitions,
        const TFlowViewPtr& flowView) const;

    bool UsesRangePartitioning() const;

private:
    TAtomicIntrusivePtr<TWatermarkState> WatermarkState_;
    THashMap<TStreamId, ISourceControllerPtr> Sources_;
    THashMap<TSinkId, ISinkControllerPtr> Sinks_;
    TInstant LastRepartitionTime_ = TInstant::Now();
    TInstant LastCommonRepartitioningInstant_ = TInstant::Zero();

    struct TCriterionEmaState
    {
        double ProposedCount{};
        TInstant UpdatedAt;
    };

    // Per-criterion peak-hold smoothing of the proposed partition count so that a transient metric
    // dip shrinks partitions only with the partition_count_half_delay release half-delay (fast growth,
    // slow reduction). See YTFLOWSUPPORT-113. In-memory, reset on leader change like LastRepartitionTime_.
    // Timestamps are per criterion: a criterion may be skipped for a while (too few statuses), and
    // its decay must account for the whole skipped interval.
    THashMap<std::string, TCriterionEmaState> CriterionProposedCountEma_;

    //! Persisted partitioning state; notably the last observed per-sink target-queue partition
    //! counts used to decide producer-id regeneration. Persisted so it survives failover/recreation.
    TMutableStateClient<TUniversalComputationControllerPartitioningState> PartitioningState_;
};

DEFINE_REFCOUNTED_TYPE(TUniversalComputationController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
