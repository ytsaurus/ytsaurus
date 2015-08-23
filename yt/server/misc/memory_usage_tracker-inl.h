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

template <class ECategory>
TMemoryUsageTrackerGuard<ECategory>::TMemoryUsageTrackerGuard()
    : Tracker_(nullptr)
    , Size_(0)
{ }

template <class ECategory>
TMemoryUsageTrackerGuard<ECategory>::TMemoryUsageTrackerGuard(TMemoryUsageTrackerGuard&& other)
{
    MoveFrom(std::move(other));
}

template <class ECategory>
TMemoryUsageTrackerGuard<ECategory>::~TMemoryUsageTrackerGuard()
{
    Release();
}

template <class ECategory>
TMemoryUsageTrackerGuard<ECategory>& TMemoryUsageTrackerGuard<ECategory>::operator=(TMemoryUsageTrackerGuard&& other)
{
    if (this != &other) {
        Release();
        MoveFrom(std::move(other));
    }
    return *this;
}

template <class ECategory>
void TMemoryUsageTrackerGuard<ECategory>::MoveFrom(TMemoryUsageTrackerGuard&& other)
{
    Tracker_ = other.Tracker_;
    Category_ = other.Category_;
    Size_ = other.Size_;

    other.Tracker_ = nullptr;
    other.Size_ = 0;
}

template <class ECategory>
void swap(TMemoryUsageTrackerGuard<ECategory>& lhs, TMemoryUsageTrackerGuard<ECategory>& rhs)
{
    std::swap(lhs.Tracker_, rhs.Tracker_);
}

template <class ECategory>
TMemoryUsageTrackerGuard<ECategory> TMemoryUsageTrackerGuard<ECategory>::Acquire(
    TMemoryUsageTracker<ECategory>* tracker,
    ECategory category,
    i64 size)
{
    TMemoryUsageTrackerGuard guard;
    tracker->Acquire(category, size);
    guard.Tracker_ = tracker;
    guard.Category_ = category;
    guard.Size_ = size;
    return guard;
}

template <class ECategory>
TErrorOr<TMemoryUsageTrackerGuard<ECategory>> TMemoryUsageTrackerGuard<ECategory>::TryAcquire(
    TMemoryUsageTracker<ECategory>* tracker,
    ECategory category,
    i64 size)
{
    auto error = tracker->TryAcquire(category, size);
    if (!error.IsOK()) {
        return error;
    }
    TMemoryUsageTrackerGuard guard;
    guard.Tracker_ = tracker;
    guard.Category_ = category;
    guard.Size_ = size;
    return std::move(guard);
}

template <class ECategory>
void TMemoryUsageTrackerGuard<ECategory>::Release()
{
    if (Tracker_) {
        Tracker_->Release(Category_, Size_);
        Tracker_ = nullptr;
        Size_ = 0;
    }
}

template <class ECategory>
TMemoryUsageTrackerGuard<ECategory>::operator bool() const
{
    return Tracker_ != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
