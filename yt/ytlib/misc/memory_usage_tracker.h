#pragma once

#include "common.h"
#include "error.h"

#include <ytlib/profiling/profiler.h>
#include <ytlib/logging/log.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class EMemoryConsumer>
class TMemoryUsageTracker
{
public:
    explicit TMemoryUsageTracker(i64 totalMemory, Stroka profilingPath="");

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

    std::vector<i64> UsedMemory;

    NProfiling::TProfiler Profiler;
    NProfiling::TAggregateCounter FreeMemoryCounter;

    std::vector<NProfiling::TAggregateCounter> ConsumerCounters;

    NLog::TLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MEMORY_USAGE_TRACKER_INL_H_
#include "memory_usage_tracker-inl.h"
#undef MEMORY_USAGE_TRACKER_INL_H_