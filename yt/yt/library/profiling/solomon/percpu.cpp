#include "percpu.h"
#include "yt/library/profiling/summary.h"

#include <yt/core/profiling/tscp.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

void TPerCpuCounter::Increment(i64 delta)
{
    auto tscp = TTscp::Get();
    Shards_[tscp.ProcessorId].Value.fetch_add(delta, std::memory_order_relaxed);
}

i64 TPerCpuCounter::GetValue()
{
    i64 total = 0;
    for (const auto& shard : Shards_) {
        total += shard.Value.load();
    }
    return total;
}

////////////////////////////////////////////////////////////////////////////////

void TPerCpuTimeCounter::Add(TDuration delta)
{
    auto tscp = TTscp::Get();
    Shards_[tscp.ProcessorId].Value.fetch_add(delta.GetValue(), std::memory_order_relaxed);
}

TDuration TPerCpuTimeCounter::GetValue()
{
    TDuration total = TDuration::Zero();
    for (const auto& shard : Shards_) {
        total += TDuration::FromValue(shard.Value.load());
    }
    return total;
}

////////////////////////////////////////////////////////////////////////////////

void TPerCpuGauge::Update(double value)
{
    auto tscp = TTscp::Get();
    Shards_[tscp.ProcessorId].Value.store(TWrite{value, tscp.Instant}, std::memory_order_relaxed);
}

double TPerCpuGauge::GetValue()
{
    double lastValue = 0.0;
    TCpuInstant maxTimestamp = 0;

    for (const auto& shard : Shards_) {
        auto write = shard.Value.load();

        if (write.Timestamp > maxTimestamp) {
            maxTimestamp = write.Timestamp;
            lastValue = write.Value;
        }
    }

    return lastValue;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TPerCpuSummary<T>::Record(T value)
{
    auto tscp = TTscp::Get();
    auto guard = Guard(Shards_[tscp.ProcessorId].Lock);
    Shards_[tscp.ProcessorId].Value.Record(value);
}

template <class T>
TSummarySnapshot<T> TPerCpuSummary<T>::GetValue()
{
    TSummarySnapshot<T> value;
    for (const auto& shard : Shards_) {
        auto guard = Guard(shard.Lock);
        value += shard.Value;
    }
    return value;
}

template <class T>
TSummarySnapshot<T> TPerCpuSummary<T>::GetValueAndReset()
{
    TSummarySnapshot<T> value;
    for (auto& shard : Shards_) {
        auto guard = Guard(shard.Lock);
        value += shard.Value;
        shard.Value = {};
    }
    return value;
}

template class TPerCpuSummary<double>;
template class TPerCpuSummary<TDuration>;

static_assert(sizeof(TPerCpuSummary<double>) == 64 + 64 * 64);
static_assert(sizeof(TPerCpuSummary<TDuration>) == 64 + 64 * 64);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
