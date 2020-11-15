#include "sensor.h"

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TSimpleCounter)
DEFINE_REFCOUNTED_TYPE(TSimpleGauge)

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

template <class T>
void TSimpleSummary<T>::Record(T value)
{
    auto guard = Guard(Lock_);
    Value_.Record(value);
}

template <class T>
TSummarySnapshot<T> TSimpleSummary<T>::GetValue()
{
    auto guard = Guard(Lock_);
    return Value_;
}

template <class T>
TSummarySnapshot<T> TSimpleSummary<T>::GetValueAndReset()
{
    auto guard = Guard(Lock_);

    auto value = Value_;
    Value_ = {};
    return value;
}

template class TSimpleSummary<double>;
template class TSimpleSummary<TDuration>;

static_assert(sizeof(TSimpleSummary<double>) == 64);
static_assert(sizeof(TSimpleSummary<TDuration>) == 64);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
