#pragma once

#include <util/datetime/base.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// An additive accumulator with exponential decay and no warm-up.
class TDecayedSum
{
public:
    // An observation loses a factor of e over |decayTime|.
    explicit TDecayedSum(TDuration decayTime);

    // Older observations are decayed to the latest observation time.
    void Add(double value, TInstant now);

    double GetLastValue() const;
    double GetDecayedValue(TInstant now) const;

private:
    const double DecayTimeSeconds_;
    double Value_ = 0;
    TInstant LastUpdateTime_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
