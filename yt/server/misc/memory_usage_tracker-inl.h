#ifndef MEMORY_USAGE_TRACKER_INL_H_
#error "Direct inclusion of this file is not allowed, include memory_usage_tracker.h"
#endif
#undef MEMORY_USAGE_TRACKER_INL_H_

#include <core/misc/string.h>

namespace NYT {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

template <class EMemoryConsumer>
TMemoryUsageTracker<EMemoryConsumer>::TMemoryUsageTracker(
    i64 totalMemory,
    const Stroka& profilingPath)
    : TotalMemory(totalMemory)
    , FreeMemory(totalMemory)
    , Profiler(profilingPath + "/memory_usage")
    , FreeMemoryCounter("/free", EmptyTagIds, EAggregateMode::Min)
    , Logger("MemoryUsage")
{
    for (auto value : EMemoryConsumer::GetDomainValues()) {
        // EMemoryConsumer enum must be contiguous, without gaps.
        YCHECK(value < EMemoryConsumer::GetDomainSize());
    }

    UsedMemory.resize(EMemoryConsumer::GetDomainSize(), 0);
    for (int value = 0; value < EMemoryConsumer::GetDomainSize(); ++value) {
        ConsumerCounters.push_back(TAggregateCounter("/" + FormatEnum(EMemoryConsumer(value))));
    }
}

template <class EMemoryConsumer>
i64 TMemoryUsageTracker<EMemoryConsumer>::GetFree() const
{
    return FreeMemory;
}

template <class EMemoryConsumer>
i64 TMemoryUsageTracker<EMemoryConsumer>::GetUsed() const
{
    return TotalMemory - FreeMemory;
}

template <class EMemoryConsumer>
i64 TMemoryUsageTracker<EMemoryConsumer>::GetUsed(EMemoryConsumer consumer) const
{
    return UsedMemory[static_cast<int>(consumer)];
}

template <class EMemoryConsumer>
i64 TMemoryUsageTracker<EMemoryConsumer>::GetTotal() const
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
        LOG_ERROR("Memory overcommit by %v after %Qv request for %v",
            -freeMemory,
            consumer,
            size);
    }
}

template <class EMemoryConsumer>
void TMemoryUsageTracker<EMemoryConsumer>::DoAcquire(EMemoryConsumer consumer, i64 size)
{
    FreeMemory -= size;
    auto& usedMemory = UsedMemory[static_cast<int>(consumer)];
    usedMemory += size;
    Profiler.Aggregate(FreeMemoryCounter, FreeMemory);
    Profiler.Aggregate(ConsumerCounters[static_cast<int>(consumer)], usedMemory);
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

    return TError(
        "Not enough memory to serve %Qv request: free %v, requested %v",
        consumer,
        freeMemory,
        size);
}

template <class EMemoryConsumer>
void TMemoryUsageTracker<EMemoryConsumer>::Release(EMemoryConsumer consumer, i64 size)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto& usedMemory = UsedMemory[static_cast<int>(consumer)];
    YCHECK(usedMemory >= size);
    usedMemory -= size;
    FreeMemory += size;
    YCHECK(FreeMemory <= TotalMemory);

    Profiler.Aggregate(FreeMemoryCounter, FreeMemory);
    Profiler.Aggregate(ConsumerCounters[static_cast<int>(consumer)], usedMemory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
