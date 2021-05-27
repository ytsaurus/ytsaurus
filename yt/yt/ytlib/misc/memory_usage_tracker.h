#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/profiling/profiler.h>
#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

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
        const NLogging::TLogger& logger = {},
        const NProfiling::TProfiler& profiler = {});

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
    TError TryChange(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {});
    void Release(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {});
    i64 UpdateUsage(ECategory category, i64 newUsage);

    IMemoryUsageTrackerPtr WithCategory(
        ECategory category,
        std::optional<TPoolTag> poolTag = {});

private:
    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    YT_DECLARE_SPINLOCK(TAdaptiveLock, PoolMapSpinLock_);

    std::atomic<i64> TotalLimit_;

    std::atomic<i64> TotalUsed_ = 0;
    std::atomic<i64> TotalFree_ = 0;

    struct TCategory
    {
        std::atomic<i64> Limit = std::numeric_limits<i64>::max();
        std::atomic<i64> Used = 0;
    };

    TEnumIndexedVector<ECategory, TCategory> Categories_;

    struct TPool
        : public TRefCounted
    {
        TPoolTag Tag;
        std::atomic<i64> Weight = 0;
        TEnumIndexedVector<ECategory, std::atomic<i64>> Used;

        TPool() = default;

        TPool(const TPool& other)
            : Weight(other.Weight.load())
            , Used(other.Used)
        { }

        TPool& operator=(const TPool& other)
        {
            Weight = other.Weight.load();
            Used = other.Used;
            return *this;
        }
    };

    THashMap<TPoolTag, TIntrusivePtr<TPool>> Pools_;
    std::atomic<i64> TotalPoolWeight_ = 0;

    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;

    i64 DoGetLimit(ECategory category) const;
    i64 DoGetLimit(ECategory category, const TPool* pool) const;
    i64 DoGetUsed(ECategory category) const;
    i64 DoGetUsed(ECategory category, const TPool* pool) const;
    i64 DoGetFree(ECategory category) const;
    i64 DoGetFree(ECategory category, const TPool* pool) const;

    TError DoTryAcquire(ECategory category, i64 size, TPool* pool);
    void DoAcquire(ECategory category, i64 size, TPool* pool);
    void DoRelease(ECategory category, i64 size, TPool* pool);

    void UpdateMetrics();

    TPool* FindPool(const TPoolTag& poolTag);
    const TPool* FindPool(const TPoolTag& poolTag) const;
    TPool* GetOrRegisterPool(const TPoolTag& poolTag);
    TPool* GetOrRegisterPool(const std::optional<TPoolTag>& poolTag);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MEMORY_USAGE_TRACKER_INL_H_
#include "memory_usage_tracker-inl.h"
#undef MEMORY_USAGE_TRACKER_INL_H_
