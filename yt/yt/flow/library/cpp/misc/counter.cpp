#include "counter.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TSimpleEmaCounter::TSimpleEmaCounter(TDuration window)
    : Counter_({window})
{ }

void TSimpleEmaCounter::Update(double total, TInstant now)
{
    Total_ = total;
    Counter_.Update(Total_, now);
}

void TSimpleEmaCounter::Inc(double count, TInstant now)
{
    Total_ += count;
    Counter_.Update(Total_, now);
}

std::optional<double> TSimpleEmaCounter::GetRate(TInstant now) const
{
    auto rate = Counter_.GetRate(0, now);
    return rate;
}

double TSimpleEmaCounter::GetTotal() const
{
    return Total_;
}

void TSimpleEmaCounter::SetWindow(TDuration window)
{
    Counter_.WindowDurations[0] = window;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
