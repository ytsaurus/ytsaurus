#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/profiling/profiler.h>

#include <core/logging/log.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class ECategory>
class TMemoryUsageTracker
{
public:
    TMemoryUsageTracker(
        i64 totalLimit,
        const std::vector<std::pair<ECategory, i64>>& limits,
        const NLogging::TLogger& logger = NLogging::TLogger(),
        const NProfiling::TProfiler& profiler = NProfiling::TProfiler());

    i64 GetTotalLimit() const;
    i64 GetTotalUsed() const;
    i64 GetTotalFree() const;
    bool IsTotalExceeded() const;

    i64 GetLimit(ECategory category) const;
    i64 GetUsed(ECategory category) const;
    i64 GetFree(ECategory category) const;
    bool IsExceeded(ECategory category) const;

    // Always succeeds, can lead to an overcommit.
    void Acquire(ECategory category, i64 size);
    TError TryAcquire(ECategory category, i64 size);
    void Release(ECategory category, i64 size);

private:
    TSpinLock SpinLock_;

    const i64 TotalLimit_;

    NProfiling::TAggregateCounter TotalUsedCounter_;
    NProfiling::TAggregateCounter TotalFreeCounter_;

    struct TCategory
    {
        i64 Limit = std::numeric_limits<i64>::max();
        NProfiling::TAggregateCounter UsedCounter;
    };

    TEnumIndexedVector<TCategory, ECategory> Categories_;

    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;


    void DoAcquire(ECategory category, i64 size);

};

////////////////////////////////////////////////////////////////////////////////

template <class ECategory>
class TMemoryUsageTrackerGuard
    : private TNonCopyable
{
public:
    TMemoryUsageTrackerGuard();
    TMemoryUsageTrackerGuard(TMemoryUsageTrackerGuard&& other);
    ~TMemoryUsageTrackerGuard();

    TMemoryUsageTrackerGuard& operator=(TMemoryUsageTrackerGuard&& other);

    static TMemoryUsageTrackerGuard Acquire(TMemoryUsageTracker<ECategory>* tracker, ECategory category, i64 size);
    static TErrorOr<TMemoryUsageTrackerGuard> TryAcquire(TMemoryUsageTracker<ECategory>* tracker, ECategory category, i64 size);

    template <class T>
    friend void swap(TMemoryUsageTrackerGuard<T>& lhs, TMemoryUsageTrackerGuard<T>& rhs);

    void Release();

    explicit operator bool() const;

private:
    TMemoryUsageTracker<ECategory>* Tracker_;
    ECategory Category_;
    i64 Size_;

    void MoveFrom(TMemoryUsageTrackerGuard&& other);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MEMORY_USAGE_TRACKER_INL_H_
#include "memory_usage_tracker-inl.h"
#undef MEMORY_USAGE_TRACKER_INL_H_
