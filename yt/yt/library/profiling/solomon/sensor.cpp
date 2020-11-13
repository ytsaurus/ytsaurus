#include "sensor.h"

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TSimpleCounter)
DEFINE_REFCOUNTED_TYPE(TSimpleGauge)
DEFINE_REFCOUNTED_TYPE(TSimpleSummary)
DEFINE_REFCOUNTED_TYPE(TSimpleTimer)

////////////////////////////////////////////////////////////////////////////////

void TSimpleGauge::Update(double value)
{
    Value_.store(value, std::memory_order_relaxed);
}

double TSimpleGauge::GetValue()
{
    return Value_.load(std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleCounter::Increment(i64 delta)
{
    YT_VERIFY(delta >= 0);
    Value_.fetch_add(delta, std::memory_order_relaxed);
}

i64 TSimpleCounter::GetValue()
{
    return Value_.load(std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleTimeCounter::Add(TDuration delta)
{
    Value_.fetch_add(delta.GetValue(), std::memory_order_relaxed);
}

TDuration TSimpleTimeCounter::GetValue()
{
    return TDuration::FromValue(Value_.load(std::memory_order_relaxed));
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleSummary::Record(double value)
{
    auto guard = Guard(Lock_);
    Value_.Record(value);
}

TSummarySnapshot<double> TSimpleSummary::GetValue()
{
    auto guard = Guard(Lock_);
    return Value_;
}

TSummarySnapshot<double> TSimpleSummary::GetValueAndReset()
{
    auto guard = Guard(Lock_);

    auto value = Value_;
    Value_ = {};
    return value;
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleTimer::Record(TDuration value)
{
    auto guard = Guard(Lock_);
    Value_.Record(value);
}

TSummarySnapshot<TDuration> TSimpleTimer::GetValue()
{
    auto guard = Guard(Lock_);
    return Value_;
}

TSummarySnapshot<TDuration> TSimpleTimer::GetValueAndReset()
{
    auto guard = Guard(Lock_);

    auto value = Value_;
    Value_ = {};
    return value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
