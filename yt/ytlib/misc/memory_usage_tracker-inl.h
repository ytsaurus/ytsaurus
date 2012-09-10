#ifndef MEMORY_USAGE_TRACKER_INL_H_
#error "Direct inclusion of this file is not allowed, include memory_usage_tracker.h"
#endif
#undef MEMORY_USAGE_TRACKER_INL_H_

namespace NYT {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

template <class EMemoryConsumer>
TMemoryUsageTracker<EMemoryConsumer>::TMemoryUsageTracker(i64 totalMemory, Stroka profilingPath)
    : TotalMemory(totalMemory)
    , FreeMemory(totalMemory)
    , Profiler(profilingPath + "/memory_usage")
    , FreeMemoryCounter("/free", EAggregateMode::Min)
    , Logger("MemoryUsageTracker")
{
    FOREACH(auto value, EMemoryConsumer::GetDomainValues()) {
        // MemoryConsumer enum must be contigious, without gaps.
        YCHECK(value < EMemoryConsumer::GetDomainSize());
    }

    UsedMemory.resize(EMemoryConsumer::GetDomainSize(), 0);
    for (int i = 0; i < EMemoryConsumer::GetDomainSize(); ++i) {
        ConsumerCounters.push_back(TAggregateCounter(EMemoryConsumer(i).ToString()));
    }
}

template <class EMemoryConsumer>
i64 TMemoryUsageTracker<EMemoryConsumer>::GetFreeMemory() const
{
    return FreeMemory;
}

template <class EMemoryConsumer>
i64 TMemoryUsageTracker<EMemoryConsumer>::GetUsedMemory() const
{
    return TotalMemory - FreeMemory;
}

template <class EMemoryConsumer>
i64 TMemoryUsageTracker<EMemoryConsumer>::GetUsedMemory(EMemoryConsumer consumer) const
{
    return UsedMemory[consumer.ToValue()];
}

template <class EMemoryConsumer>
i64 TMemoryUsageTracker<EMemoryConsumer>::GetTotalMemory() const
{
    return TotalMemory;
}

template <class EMemoryConsumer>
void TMemoryUsageTracker<EMemoryConsumer>::Acquire(EMemoryConsumer consumer, i64 size)
{
    TGuard<TSpinLock> guard(SpinLock);
    DoAcquire(consumer, size);
    auto freeMemory = FreeMemory;
    guard.Release();

    if (freeMemory < 0) {
        LOG_ERROR("Overcommited memory size %"PRId64, freeMemory);
    }
}

template <class EMemoryConsumer>
void TMemoryUsageTracker<EMemoryConsumer>::DoAcquire(EMemoryConsumer consumer, i64 size)
{
    FreeMemory -= size;
    auto& usedMemory = UsedMemory[consumer.ToValue()];
    usedMemory += size;
    Profiler.Aggregate(FreeMemoryCounter, FreeMemory);
    Profiler.Aggregate(ConsumerCounters[consumer.ToValue()], usedMemory);
}

template <class EMemoryConsumer>
TError TMemoryUsageTracker<EMemoryConsumer>::TryAcquire(EMemoryConsumer consumer, i64 size)
{
    TGuard<TSpinLock> guard(SpinLock);
    if (size < FreeMemory) {
        DoAcquire(consumer, size);
        return TError();
    }

    auto freeMemory = FreeMemory;
    guard.Release();

    return TError("Not enough memory (FreeMemory: %"PRId64
        ", Requested: %"PRId64
        ", MemoryConsumer: %s",
        freeMemory,
        size,
        ~consumer.ToString());
}

template <class EMemoryConsumer>
void TMemoryUsageTracker<EMemoryConsumer>::Release(EMemoryConsumer consumer, i64 size)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto& usedMemory = UsedMemory[consumer.ToValue()];
    YCHECK(usedMemory >= size);
    usedMemory -= size;
    FreeMemory += size;
    YCHECK(FreeMemory <= TotalMemory);

    Profiler.Aggregate(FreeMemoryCounter, FreeMemory);
    Profiler.Aggregate(ConsumerCounters[consumer.ToValue()], usedMemory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT