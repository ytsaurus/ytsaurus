#ifndef MEMORY_USAGE_TRACKER_INL_H_
#error "Direct inclusion of this file is not allowed, include memory_usage_tracker.h"
#endif

#include <core/concurrency/thread_affinity.h>

#include <core/profiling/profile_manager.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class ECategory>
TMemoryUsageTracker<ECategory>::TMemoryUsageTracker(
    i64 totalLimit,
    const std::vector<std::pair<ECategory, i64>>& limits,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : TotalLimit_(totalLimit)
    , TotalUsedCounter_("/total_used", NProfiling::EmptyTagIds, NProfiling::EAggregateMode::Max)
    , TotalFreeCounter_("/total_free", NProfiling::EmptyTagIds, NProfiling::EAggregateMode::Min)
    , Logger(logger)
    , Profiler(profiler)
{
    Profiler.Update(TotalFreeCounter_, totalLimit);

    auto* profileManager = NProfiling::TProfileManager::Get();
    for (auto value : TEnumTraits<ECategory>::GetDomainValues()) {
        auto tagId = profileManager->RegisterTag("category", value);
        Categories_[value].UsedCounter = NProfiling::TAggregateCounter(
            "/used",
            {tagId},
            NProfiling::EAggregateMode::Max);
    }

    for (const auto& pair : limits) {
        Categories_[pair.first].Limit = pair.second;
    }
}

template <class ECategory>
i64 TMemoryUsageTracker<ECategory>::GetTotalLimit() const
{
    return TotalLimit_;
}

template <class ECategory>
i64 TMemoryUsageTracker<ECategory>::GetTotalUsed() const
{
    return TotalUsedCounter_.Current;
}

template <class ECategory>
i64 TMemoryUsageTracker<ECategory>::GetTotalFree() const
{
    return std::max(
        GetTotalLimit() - GetTotalUsed(),
        static_cast<i64>(0));
}

template <class ECategory>
bool TMemoryUsageTracker<ECategory>::IsTotalExceeded() const
{
    return TotalUsedCounter_.Current > TotalLimit_;
}

template <class ECategory>
i64 TMemoryUsageTracker<ECategory>::GetLimit(ECategory category) const
{
    return Categories_[category].Limit;
}

template <class ECategory>
i64 TMemoryUsageTracker<ECategory>::GetUsed(ECategory category) const
{
    return Categories_[category].UsedCounter.Current;
}

template <class ECategory>
i64 TMemoryUsageTracker<ECategory>::GetFree(ECategory category) const
{
    return std::max(
        std::min(GetLimit(category) - GetUsed(category), GetTotalFree()),
        static_cast<i64>(0));
}

template <class ECategory>
bool TMemoryUsageTracker<ECategory>::IsExceeded(ECategory category) const
{
    if (IsTotalExceeded()) {
        return true;
    }
    const auto& data = Categories_[category];
    return data.UsedCounter.Current > data.Limit;
}

template <class ECategory>
void TMemoryUsageTracker<ECategory>::Acquire(ECategory category, i64 size)
{
    TGuard<TSpinLock> guard(SpinLock_);

    DoAcquire(category, size);

    if (TotalFreeCounter_.Current < 0) {
        LOG_WARNING("Total memory overcommit by %v after %Qlv request for %v",
            -TotalFreeCounter_.Current,
            category,
            size);
    }

    const auto& data = Categories_[category];
    if (data.UsedCounter.Current > data.Limit) {
        LOG_WARNING("Per-category memory overcommit by %v after %Qlv request for %v",
            data.UsedCounter.Current - data.Limit,
            category,
            size);
    }
}

template <class ECategory>
TError TMemoryUsageTracker<ECategory>::TryAcquire(ECategory category, i64 size)
{
    TGuard<TSpinLock> guard(SpinLock_);

    i64 free = GetFree(category);
    if (size > GetFree(category)) {
        return TError(
            "Not enough memory to serve %Qlv request: free %v, requested %v",
            category,
            free,
            size);
    }

    DoAcquire(category, size);
    return TError();
}

template <class ECategory>
void TMemoryUsageTracker<ECategory>::DoAcquire(ECategory category, i64 size)
{
    YCHECK(size >= 0);

    VERIFY_SPINLOCK_AFFINITY(SpinLock_);
    Profiler.Increment(TotalUsedCounter_, +size);
    Profiler.Increment(TotalFreeCounter_, -size);
    Profiler.Increment(Categories_[category].UsedCounter, +size);
}

template <class ECategory>
void TMemoryUsageTracker<ECategory>::Release(ECategory category, i64 size)
{
    YCHECK(size >= 0);

    TGuard<TSpinLock> guard(SpinLock_);
    Profiler.Increment(TotalUsedCounter_, -size);
    Profiler.Increment(TotalFreeCounter_, +size);
    Profiler.Increment(Categories_[category].UsedCounter, -size);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
