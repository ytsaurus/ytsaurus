#pragma once

#include "public.h"

#include <util/generic/array_ref.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TThroughputProcessingMeasurement
{
    TInstant Begin;
    TInstant End;
    std::optional<double> Processed;
    std::optional<double> ProcessedCredit;
    TInstant ProcessedCreditBegin;
    TInstant ProcessedCreditEnd;
    std::optional<double> Capacity;
    double BusyPartitionShare = 0;
    double InputWaitingShare = 0;
    double OutputWaitingShare = 0;
};

//! Combines paired per-job observations; every current job must be represented.
std::optional<TThroughputProcessingMeasurement> MeasureThroughputProcessing(
    const TThroughputSpecPtr& config,
    TConstArrayRef<const TProcessingObservationWindow*> observations,
    TInstant now,
    TDuration period,
    TInstant readyAt = TInstant::Zero());

//! Mean duration of nonempty epochs, or the latest duration for older workers.
TDuration ComputeThroughputBatchDuration(TConstArrayRef<const TProcessingObservationWindow*> observations);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
