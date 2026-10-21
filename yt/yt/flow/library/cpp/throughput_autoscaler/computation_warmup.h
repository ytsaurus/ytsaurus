#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Requires a complete measured interval from every current job before each evaluation.
class TThroughputComputationWarmup
{
public:
    struct TObservation
    {
        TInstant Now;
        TInstant CohortReadyAt;
        TInstant MeasurementBegin;
        TInstant MeasurementEnd;
        TDuration Period;
    };

    [[nodiscard]] bool Observe(const TObservation& observation);
    void Reset(TInstant now);

private:
    TInstant NotBefore_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
