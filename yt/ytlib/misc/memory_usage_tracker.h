#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/error.h>

#include <yt/core/profiling/profiler.h>

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

    void SetTotalLimit(i64 newLimit);
    void SetCategoryLimit(ECategory category, i64 newLimit);

    // Always succeeds, can lead to an overcommit.
    void Acquire(ECategory category, i64 size);
    TError TryAcquire(ECategory category, i64 size);
    void Release(ECategory category, i64 size);

private:
    TSpinLock SpinLock_;

    i64 TotalLimit_;

    NProfiling::TAggregateGauge TotalUsedCounter_;
    NProfiling::TAggregateGauge TotalFreeCounter_;

    struct TCategory
    {
        i64 Limit = std::numeric_limits<i64>::max();
        NProfiling::TAggregateGauge UsedCounter;
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
    TMemoryUsageTrackerGuard() = default;
    TMemoryUsageTrackerGuard(const TMemoryUsageTrackerGuard& other) = delete;
    TMemoryUsageTrackerGuard(TMemoryUsageTrackerGuard&& other);
    ~TMemoryUsageTrackerGuard();

    TMemoryUsageTrackerGuard& operator=(const TMemoryUsageTrackerGuard& other) = delete;
    TMemoryUsageTrackerGuard& operator=(TMemoryUsageTrackerGuard&& other);

    static TMemoryUsageTrackerGuard Acquire(
        TMemoryUsageTracker<ECategory>* tracker,
        ECategory category,
        i64 size,
        i64 granularity = 1);
    static TErrorOr<TMemoryUsageTrackerGuard> TryAcquire(
        TMemoryUsageTracker<ECategory>* tracker,
        ECategory category,
        i64 size,
        i64 granularity = 1);

    template <class T>
    friend void swap(TMemoryUsageTrackerGuard<T>& lhs, TMemoryUsageTrackerGuard<T>& rhs);

    void Release();

    explicit operator bool() const;

    i64 GetSize() const;
    void SetSize(i64 size);
    void UpdateSize(i64 sizeDelta);

private:
    TMemoryUsageTracker<ECategory>* Tracker_ = nullptr;
    ECategory Category_;
    i64 Size_ = 0;
    i64 AcquiredSize_ = 0;
    i64 Granularity_ = 0;

    void MoveFrom(TMemoryUsageTrackerGuard&& other);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MEMORY_USAGE_TRACKER_INL_H_
#include "memory_usage_tracker-inl.h"
#undef MEMORY_USAGE_TRACKER_INL_H_
