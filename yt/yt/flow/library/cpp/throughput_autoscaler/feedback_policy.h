#pragma once

#include "feedback_state.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Inputs for a computation partition-count decision.
struct TThroughputPartitioningObservation
{
    i64 PartitionCount = 0;
    i64 MinPartitionCount = 1;
    i64 MaxPartitionCount = TPartitioningSpec::DefaultMaxPartitionCount;
    double Capacity = 0;
    double Processed = 0;
    std::optional<double> ProcessedCredit;
    double BusyPartitionShare = 0;
    double SteadyDemand = 0;
    double PeakSteadyDemand = 0;
    double Demand = 0;
    double NormalBacklog = 0;
    double ExcessBacklog = 0;
    bool AllowScaleUp = true;
    bool ResumeAfterBackoff = false;

    bool HasShortage() const;
};

class TThroughputFeedbackPolicy
{
public:
    TThroughputFeedbackPolicy(
        TThroughputFeedbackState state,
        TThroughputSpecPtr config,
        TPartitioningSpecPtr partitioningSpec,
        NLogging::TLogger logger);

    void Reconfigure(TThroughputSpecPtr config, TPartitioningSpecPtr partitioningSpec);
    const TThroughputFeedbackState& GetState() const;
    i64 ComputeExplorationPartitionCountLimit() const;
    bool ApplyResetGeneration();
    TDuration GetMeasurementPeriod() const;
    TDuration GetWarmupPeriod() const;
    bool CanRecoverWithoutBackoff(const TThroughputPartitioningObservation& observation) const;
    TThroughputAutoscalerDecision SuggestPartitionCount(const TThroughputPartitioningObservation& observation);
    std::optional<TThroughputAutoscalerDecision> RevalidateScaleDown(const TThroughputPartitioningObservation& observation) const;
    void AcceptPartitionCount(i64 currentCount, i64 selectedCount, std::optional<TThroughputAutoscalerDecision> proposal);
    void AcknowledgeAppliedPartitionCount(i64 count);

private:
    TThroughputFeedbackState State_;
    TThroughputSpecPtr Config_;
    TPartitioningSpecPtr PartitioningSpec_;
    NLogging::TLogger Logger_;

    void FinishSearch();
    void RecordAnchor(const TThroughputPartitioningObservation& observation);
    TThroughputAutoscalerDecision ProposePartitionCount(
        const TThroughputPartitioningObservation& observation,
        const NLogging::TLogger& logger,
        i64 target,
        EThroughputPartitioningAction action = EThroughputPartitioningAction::Regular);
    TThroughputAutoscalerDecision ReturnToAnchor(
        const TThroughputPartitioningObservation& observation,
        const NLogging::TLogger& logger);
    std::optional<TThroughputAutoscalerDecision> ReconcilePartitionCount(
        const TThroughputPartitioningObservation& observation,
        const NLogging::TLogger& logger);
    std::optional<TThroughputAutoscalerDecision> ObserveCapacity(
        const TThroughputPartitioningObservation& observation,
        const NLogging::TLogger& logger);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
