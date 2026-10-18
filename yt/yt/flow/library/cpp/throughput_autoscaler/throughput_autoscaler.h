#pragma once

#include "computation_warmup.h"
#include "demand.h"
#include "feedback_policy.h"
#include "processing_observation.h"
#include "steady_demand_history.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TThroughputPipelineObservation
{
    TInstant Now;
    TThroughputSpecPtr Config = New<TThroughputSpec>();
    THashMap<TComputationId, TThroughputComputationObservation> Computations;
};

//! Latest controller-visible feedback for one computation.
struct TThroughputAutoscalerTelemetry
{
    i64 PartitionCount = 0;
    std::optional<i64> MaxPartitionCount;
    std::optional<double> Demand;
    std::optional<double> SteadyDemand;
    double PeakSteadyDemand = 0;
    std::optional<double> Processed;
    std::optional<double> Capacity;
    std::optional<i64> TargetPartitionCount;
    std::optional<TThroughputFeedbackBaseline> Anchor;
    std::optional<TThroughputExplorationState> Exploration;
    std::optional<i64> ExplorationPartitionCountLimit;
    EThroughputSearchPhase Phase = EThroughputSearchPhase::Idle;
    TDuration RetryWarmupPeriod;
    bool CapacityDegraded = false;
    bool WarmupReady = false;
    bool Repartitioning = false;
};

////////////////////////////////////////////////////////////////////////////////

//! Computes partition-count proposals from pipeline observations.
class TThroughputAutoscaler
{
public:
    struct TComputation
    {
        TComputationSpecPtr Spec;
        //! Null for source or other non-range partitioning.
        TPartitioningSpecPtr PartitioningSpec;
    };

    using TComputations = THashMap<TComputationId, TComputation>;

    void InitializeComputation(
        const TComputationId& computationId,
        TThroughputFeedbackState state,
        NYson::TYsonString demandHistory = {});
    const TThroughputFeedbackState& GetFeedbackState(const TComputationId& computationId) const;

    const NYson::TYsonString& SaveDemandHistory(const TComputationId& computationId) const;

    void BeginIteration();
    void Commit();
    void Reconfigure(TComputations computations, const TThroughputSpecPtr& config);
    TThroughputMeasurementRequests ComputeMeasurementRequests() const;
    void Evaluate(TThroughputPipelineObservation sample);
    TError CheckComputationStatus(const TComputationId& computationId) const;

    std::optional<TThroughputAutoscalerDecision> FindDecision(
        const TComputationId& computationId) const;

    void AcceptPartitionCount(const TComputationId& computationId, i64 currentCount, i64 selectedCount);

    std::optional<TThroughputAutoscalerTelemetry> FindTelemetry(
        const TComputationId& computationId) const;

private:
    struct TScaleDownRecommendation
    {
        i64 PartitionCount = 0;
        double Capacity = 0;
        TInstant MeasuredThrough;
    };

    struct TRuntimeState
    {
        i64 ObservedPartitionCount = 0;
        TInstant ObservedReadyAt;
        std::optional<TInstant> ShortageSince;
        std::optional<TScaleDownRecommendation> ScaleDownRecommendation;
        TThroughputComputationWarmup Warmup;
    };

    struct TPreparedComputation
    {
        TThroughputComputationObservation Observation;
        bool WarmupReady = false;
        bool UseRecoveryMeasurement = false;
        TThroughputDemand Demand;
        double PeakSteadyDemand = 0;
    };

    struct TDecisionState
    {
        TInstant CurrentTime;
        std::optional<TInstant> LastScaleDownBatchTime;
        THashMap<TComputationId, TThroughputFeedbackPolicy> Policies;
        THashMap<TComputationId, TRuntimeState> Computations;
    };

    // Committed measurements survive a rejected decision; history is not part of its rollback copy.
    THashMap<TComputationId, TSteadyDemandHistory> DemandHistories_;

    TDecisionState State_;
    std::optional<TDecisionState> PreviousState_;
    TComputations Computations_;
    THashMap<TComputationId, TPreparedComputation> Prepared_;
    THashMap<TComputationId, TThroughputAutoscalerDecision> Decisions_;

    void PropagateDemand(const TThroughputSpecPtr& config);
    void EvaluateComputation(
        const TComputationId& computationId,
        const TThroughputSpecPtr& config,
        TInstant now,
        bool allowOrdinaryScaleDown);
    static bool IsScalable(const TComputation& computation);
    bool HasScalableComputation() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
