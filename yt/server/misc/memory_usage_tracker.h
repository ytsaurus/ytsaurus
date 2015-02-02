#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/profiling/profiler.h>

#include <core/logging/log.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class EMemoryConsumer>
class TMemoryUsageTracker
{
public:
    explicit TMemoryUsageTracker(
        i64 totalMemory,
        const Stroka& profilingPath = "");

    i64 GetFree() const;
    i64 GetUsed() const;
    i64 GetUsed(EMemoryConsumer consumer) const;
    i64 GetTotal() const;

    // Always succeeds, can lead to an overcommit.
    void Acquire(EMemoryConsumer consumer, i64 size);
    TError TryAcquire(EMemoryConsumer consumer, i64 size);
    void Release(EMemoryConsumer consumer, i64 size);

private:
    void DoAcquire(EMemoryConsumer consumer, i64 size);

    TSpinLock SpinLock;

    i64 TotalMemory;
    i64 FreeMemory;

    TEnumIndexedVector<i64, EMemoryConsumer> UsedMemory;

    NProfiling::TProfiler Profiler;
    NProfiling::TAggregateCounter FreeMemoryCounter;

    TEnumIndexedVector<NProfiling::TAggregateCounter, EMemoryConsumer> ConsumerCounters;

    NLog::TLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MEMORY_USAGE_TRACKER_INL_H_
#include "memory_usage_tracker-inl.h"
#undef MEMORY_USAGE_TRACKER_INL_H_
