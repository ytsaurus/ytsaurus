#include "computation_warmup.h"

#include <algorithm>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

bool TThroughputComputationWarmup::Observe(const TObservation& observation)
{
    const auto begin = std::max({NotBefore_, observation.CohortReadyAt, observation.MeasurementBegin});
    if (observation.MeasurementEnd > observation.Now || observation.MeasurementEnd < begin || observation.MeasurementEnd - begin < observation.Period) {
        return false;
    }
    NotBefore_ = observation.MeasurementEnd;
    return true;
}

void TThroughputComputationWarmup::Reset(TInstant now)
{
    NotBefore_ = now;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
