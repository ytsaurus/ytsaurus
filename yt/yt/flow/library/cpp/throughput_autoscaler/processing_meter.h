#pragma once

#include "processing_observation.h"
#include "processing_observation_window.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Collects committed processing windows and stream observations for current jobs.
class TProcessingMeter
{
public:
    using TComputations = TThroughputMeasurementRequests;
    using TObservations = THashMap<TComputationId, TThroughputComputationObservation>;

    TProcessingMeter() = default;
    TProcessingMeter(const TProcessingMeter&) = delete;
    TProcessingMeter& operator=(const TProcessingMeter&) = delete;

    TObservations Collect(
        const TFlowStatePtr& committedState,
        const TFlowViewPtr& flowView,
        const TThroughputSpecPtr& config,
        const TComputations& computations);

private:
    struct TComputationHistory
    {
        TInstant ReadyAt;
        THashMap<TPartitionId, TProcessingObservationWindow> Jobs;
    };

    THashMap<TComputationId, TComputationHistory> Histories_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
