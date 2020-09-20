#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/memory_usage_tracker.h>
#include <yt/core/misc/error.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/profile_manager.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class ECategory, class TPoolTag>
class TMemoryUsageTracker
    : public TRefCounted
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

    i64 GetLimit(ECategory category, const std::optional<TPoolTag>& poolTag = {}) const;
    i64 GetUsed(ECategory category, const std::optional<TPoolTag>& poolTag = {}) const;
    i64 GetFree(ECategory category, const std::optional<TPoolTag>& poolTag = {}) const;
    bool IsExceeded(ECategory category, const std::optional<TPoolTag>& poolTag = {}) const;

    void SetTotalLimit(i64 newLimit);
    void SetCategoryLimit(ECategory category, i64 newLimit);
    void SetPoolWeight(const TPoolTag& poolTag, i64 newWeight);

    // Always succeeds, may lead to an overcommit.
    void Acquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {});
    TError TryAcquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {});
    void Release(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {});

private:
    TAdaptiveLock SpinLock_;
    TAdaptiveLock PoolMapSpinLock_;

    i64 TotalLimit_;

    NProfiling::TAtomicGauge TotalUsedGauge_;
    NProfiling::TAtomicGauge TotalFreeGauge_;

    struct TCategory
    {
        i64 Limit = std::numeric_limits<i64>::max();
        NProfiling::TAtomicGauge UsedGauge;
    };

    TEnumIndexedVector<ECategory, TCategory> Categories_;

    inline static const NProfiling::TEnumMemberTagCache<ECategory> CategoryTagCache_{"category"};

    struct TPool
    {
        i64 Weight = 0;
        TEnumIndexedVector<ECategory, NProfiling::TAtomicGauge> Used;
    };

    THashMap<TPoolTag, TPool> Pools_;
    i64 TotalPoolWeight_ = 0;

    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;

    NConcurrency::TPeriodicExecutorPtr PeriodicUpdater_;

    void DoAcquire(ECategory category, i64 size, TPool* pool);

    void UpdateMetrics();

    TPool* FindPool(const TPoolTag& poolTag);
    const TPool* FindPool(const TPoolTag& poolTag) const;
    TPool* GetOrRegisterPool(const TPoolTag& poolTag);

    i64 GetPoolLimit(const TPool* pool, i64 categoryLimit) const;
};

////////////////////////////////////////////////////////////////////////////////

template <class ECategory, class TPoolTag>
class TMemoryUsageTrackerGuard
    : private TNonCopyable
{
public:
    using TMemoryUsageTracker = NYT::TMemoryUsageTracker<ECategory>;
    using TMemoryUsageTrackerPtr = TIntrusivePtr<TMemoryUsageTracker>;

    TMemoryUsageTrackerGuard() = default;
    TMemoryUsageTrackerGuard(const TMemoryUsageTrackerGuard& other) = delete;
    TMemoryUsageTrackerGuard(TMemoryUsageTrackerGuard&& other);
    ~TMemoryUsageTrackerGuard();

    TMemoryUsageTrackerGuard& operator=(const TMemoryUsageTrackerGuard& other) = delete;
    TMemoryUsageTrackerGuard& operator=(TMemoryUsageTrackerGuard&& other);

    static TMemoryUsageTrackerGuard Acquire(
        TMemoryUsageTrackerPtr tracker,
        ECategory category,
        i64 size,
        std::optional<TPoolTag> poolTag = {},
        i64 granularity = 1);
    static TErrorOr<TMemoryUsageTrackerGuard> TryAcquire(
        TMemoryUsageTrackerPtr tracker,
        ECategory category,
        i64 size,
        std::optional<TPoolTag> poolTag = {},
        i64 granularity = 1);

    template <class T>
    friend void swap(TMemoryUsageTrackerGuard<T>& lhs, TMemoryUsageTrackerGuard<T>& rhs);

    void Release();

    explicit operator bool() const;

    i64 GetSize() const;
    void SetSize(i64 size);
    void UpdateSize(i64 sizeDelta);

private:
    TMemoryUsageTrackerPtr Tracker_;
    ECategory Category_;
    std::optional<TPoolTag> PoolTag_;
    i64 Size_ = 0;
    i64 AcquiredSize_ = 0;
    i64 Granularity_ = 0;

    void MoveFrom(TMemoryUsageTrackerGuard&& other);
};

////////////////////////////////////////////////////////////////////////////////

template <class ECategory>
class TTypedMemoryTracker
    : public IMemoryUsageTracker
{
public:
    TTypedMemoryTracker(
        TIntrusivePtr<TMemoryUsageTracker<ECategory>> memoryTracker,
        ECategory category);

    virtual TError TryAcquire(size_t size) override;
    virtual void Release(size_t size) override;

private:
    const TIntrusivePtr<TMemoryUsageTracker<ECategory>> MemoryTracker_;
    const ECategory Category_;
};

template <class ECategory>
IMemoryUsageTrackerPtr CreateMemoryTrackerForCategory(
    TIntrusivePtr<TMemoryUsageTracker<ECategory>> memoryTracker,
    ECategory category)
{
    return New<TTypedMemoryTracker<ECategory>>(
        std::move(memoryTracker),
        category);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MEMORY_USAGE_TRACKER_INL_H_
#include "memory_usage_tracker-inl.h"
#undef MEMORY_USAGE_TRACKER_INL_H_
