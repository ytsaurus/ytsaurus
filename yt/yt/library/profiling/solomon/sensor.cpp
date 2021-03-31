#include "sensor.h"
#include "yt/yt/core/misc/assert.h"
#include <atomic>

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

void TSimpleGauge::Record(double /* value */)
{
    YT_UNIMPLEMENTED();
}

TSummarySnapshot<double> TSimpleGauge::GetSummary()
{
    TSummarySnapshot<double> summary;
    summary.Record(GetValue());
    return summary;
}

TSummarySnapshot<double> TSimpleGauge::GetSummaryAndReset()
{
    return GetSummary();
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleTimeGauge::Update(TDuration value)
{
    Value_.store(value.GetValue(), std::memory_order_relaxed);
}

TDuration TSimpleTimeGauge::GetValue()
{
    return TDuration::FromValue(Value_.load(std::memory_order_relaxed));
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
TSummarySnapshot<T> TSimpleSummary<T>::GetSummary()
{
    auto guard = Guard(Lock_);
    return Value_;
}

template <class T>
TSummarySnapshot<T> TSimpleSummary<T>::GetSummaryAndReset()
{
    auto guard = Guard(Lock_);

    auto value = Value_;
    Value_ = {};
    return value;
}

template class TSimpleSummary<double>;
template class TSimpleSummary<TDuration>;

////////////////////////////////////////////////////////////////////////////////
constexpr int MaxBinCount = 65;
static auto GenericBucketBounds() {
    std::array<ui64, MaxBinCount> result;

    for (int index = 0; index <= 6; ++index) {
        result[index] = 1ull << index;
    }

    for (int index = 7; index < 10; ++index) {
        result[index] = 1000ull >> (10 - index);
    }

    for (int index = 10; index < MaxBinCount; ++index) {
        result[index] = 1000 * result[index - 10];
    }

    return result;
}

static std::vector<TDuration> BucketBounds(const TSensorOptions& options)
{
    if (options.HistogramMin.Zero() && options.HistogramMax.Zero()) {
        return {};
    }

    std::vector<TDuration> bounds;
    for (auto bound : GenericBucketBounds()) {
        auto duration = TDuration::FromValue(bound);
        if (options.HistogramMin <= duration && duration <= options.HistogramMax) {
            bounds.push_back(duration);
        }
    }

    return bounds;
}

THistogram::THistogram(const TSensorOptions& options)
    : Bounds_(!options.HistogramBounds.empty() ? std::vector<TDuration>{options.HistogramBounds} : BucketBounds(options))
    , Buckets_(Bounds_.size() + 1)
{
    YT_VERIFY(!Bounds_.empty());
    YT_VERIFY(Bounds_.size() <= MaxBinCount);
}

void THistogram::Record(TDuration value)
{
    auto it = std::lower_bound(Bounds_.begin(), Bounds_.end(), value);
    Buckets_[it - Bounds_.begin()].fetch_add(1, std::memory_order_relaxed);
}

THistogramSnapshot THistogram::GetSnapshotAndReset()
{
    THistogramSnapshot snapshot;
    snapshot.Times = Bounds_;
    snapshot.Values.resize(Buckets_.size());

    for (size_t i = 0; i < Buckets_.size(); ++i) {
        snapshot.Values[i] = Buckets_[i].exchange(0, std::memory_order_relaxed);
    }

    return snapshot;
}

TSummarySnapshot<TDuration> THistogram::GetSummary()
{
    YT_UNIMPLEMENTED();
}

TSummarySnapshot<TDuration> THistogram::GetSummaryAndReset()
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
