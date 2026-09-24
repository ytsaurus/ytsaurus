#include "decayed_sum.h"

#include <library/cpp/yt/assert/assert.h>

#include <cmath>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TDecayedSum::TDecayedSum(TDuration decayTime)
    : DecayTimeSeconds_(decayTime.SecondsFloat())
{
    YT_VERIFY(decayTime > TDuration::Zero());
}

void TDecayedSum::Add(double value, TInstant now)
{
    if (now < LastUpdateTime_) {
        Value_ += value * std::exp(-(LastUpdateTime_ - now).SecondsFloat() / DecayTimeSeconds_);
    } else {
        Value_ = GetDecayedValue(now) + value;
        LastUpdateTime_ = now;
    }
}

double TDecayedSum::GetLastValue() const
{
    return Value_;
}

double TDecayedSum::GetDecayedValue(TInstant now) const
{
    if (now <= LastUpdateTime_) {
        return Value_;
    }
    return Value_ * std::exp(-(now - LastUpdateTime_).SecondsFloat() / DecayTimeSeconds_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
