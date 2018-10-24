#pragma once
#ifndef MEMORY_USAGE_TRACKER_INL_H_
#error "Direct inclusion of this file is not allowed, include memory_usage_tracker.h"
// For the sake of sane code completion
#include "memory_usage_tracker.h"
#endif

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/profiling/profile_manager.h>

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
        Categories_[value].UsedCounter = NProfiling::TAggregateGauge(
            "/used",
            {tagId},
            NProfiling::EAggregateMode::Max);
    }

    for (const auto& pair : limits) {
        Categories_[pair.first].Limit = pair.second;
    }

    PeriodicUpdater_ = New<NConcurrency::TPeriodicExecutor>(
        NProfiling::TProfileManager::Get()->GetInvoker(),
        BIND(&TMemoryUsageTracker::UpdateMetrics, MakeWeak(this)),
        TDuration::Seconds(1));
    PeriodicUpdater_->Start();
}

template <class ECategory>
i64 TMemoryUsageTracker<ECategory>::GetTotalLimit() const
{
    return TotalLimit_;
}

template <class ECategory>
i64 TMemoryUsageTracker<ECategory>::GetTotalUsed() const
{
    return TotalUsedCounter_.GetCurrent();
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
    return TotalUsedCounter_.GetCurrent() > TotalLimit_;
}

template <class ECategory>
i64 TMemoryUsageTracker<ECategory>::GetLimit(ECategory category) const
{
    return Categories_[category].Limit;
}

template <class ECategory>
i64 TMemoryUsageTracker<ECategory>::GetUsed(ECategory category) const
{
    return Categories_[category].UsedCounter.GetCurrent();
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
    return data.UsedCounter.GetCurrent() > data.Limit;
}

template <class ECategory>
void TMemoryUsageTracker<ECategory>::SetTotalLimit(i64 newLimit)
{
    auto guard = Guard(SpinLock_);
    TotalLimit_ = newLimit;
}

template <class ECategory>
void TMemoryUsageTracker<ECategory>::SetCategoryLimit(ECategory category, i64 newLimit)
{
    auto guard = Guard(SpinLock_);
    Categories_[category].Limit = newLimit;
}

template <class ECategory>
void TMemoryUsageTracker<ECategory>::Acquire(ECategory category, i64 size)
{
    TGuard<TSpinLock> guard(SpinLock_);

    DoAcquire(category, size);

    auto currentFree = TotalFreeCounter_.GetCurrent();
    if (currentFree < 0) {
        LOG_WARNING("Total memory overcommit by %v after %Qlv request for %v",
            -currentFree,
            category,
            size);
    }

    const auto& data = Categories_[category];
    auto currentUsed = data.UsedCounter.GetCurrent();
    if (currentUsed > data.Limit) {
        LOG_WARNING("Per-category memory overcommit by %v after %Qlv request for %v",
            currentUsed - data.Limit,
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

template <class ECategory>
void TMemoryUsageTracker<ECategory>::UpdateMetrics()
{
    Profiler.Increment(TotalUsedCounter_, 0);
    Profiler.Increment(TotalFreeCounter_, 0);

    for (auto& category : Categories_) {
        Profiler.Increment(category.UsedCounter, 0);
    }
}

template <class ECategory>
Y_FORCE_INLINE void Ref(TMemoryUsageTracker<ECategory>* obj)
{
    obj->Ref();
}

template <class ECategory>
Y_FORCE_INLINE void Ref(const TMemoryUsageTracker<ECategory>* obj)
{
    obj->Ref();
}

template <class ECategory>
Y_FORCE_INLINE void Unref(TMemoryUsageTracker<ECategory>* obj)
{
    obj->Unref();
}

template <class ECategory>
Y_FORCE_INLINE void Unref(const TMemoryUsageTracker<ECategory>* obj)
{
    obj->Unref();
}

////////////////////////////////////////////////////////////////////////////////

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
    AcquiredSize_ = other.AcquiredSize_;
    Granularity_ = other.Granularity_;

    other.Tracker_ = nullptr;
    other.Size_ = 0;
    other.AcquiredSize_ = 0;
    other.Granularity_ = 0;
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
    i64 size,
    i64 granularity)
{
    TMemoryUsageTrackerGuard guard;
    guard.Tracker_ = tracker;
    guard.Category_ = category;
    guard.Size_ = size;
    guard.Granularity_ = granularity;
    if (size >= granularity) {
        guard.AcquiredSize_ = size;
        tracker->Acquire(category, size);
    }
    return guard;
}

template <class ECategory>
TErrorOr<TMemoryUsageTrackerGuard<ECategory>> TMemoryUsageTrackerGuard<ECategory>::TryAcquire(
    TMemoryUsageTracker<ECategory>* tracker,
    ECategory category,
    i64 size,
    i64 granularity)
{
    auto error = tracker->TryAcquire(category, size);
    if (!error.IsOK()) {
        return error;
    }
    TMemoryUsageTrackerGuard guard;
    guard.Tracker_ = tracker;
    guard.Category_ = category;
    guard.Size_ = size;
    guard.AcquiredSize_ = size;
    guard.Granularity_ = granularity;
    return std::move(guard);
}

template <class ECategory>
void TMemoryUsageTrackerGuard<ECategory>::Release()
{
    if (Tracker_) {
        Tracker_->Release(Category_, AcquiredSize_);
        Tracker_ = nullptr;
        Size_ = 0;
        AcquiredSize_ = 0;
        Granularity_ = 0;
    }
}

template <class ECategory>
TMemoryUsageTrackerGuard<ECategory>::operator bool() const
{
    return Tracker_ != nullptr;
}

template <class ECategory>
i64 TMemoryUsageTrackerGuard<ECategory>::GetSize() const
{
    return Size_;
}

template <class ECategory>
void TMemoryUsageTrackerGuard<ECategory>::SetSize(i64 size)
{
    Y_ASSERT(Tracker_);
    Y_ASSERT(Size_ >= 0);
    Y_ASSERT(size >= 0);
    Size_ = size;
    if (std::abs(Size_ - AcquiredSize_) >= Granularity_) {
        if (Size_ > AcquiredSize_) {
            Tracker_->Acquire(Category_, Size_ - AcquiredSize_);
        } else {
            Tracker_->Release(Category_, AcquiredSize_ - Size_);
        }
        AcquiredSize_ = Size_;
    }
}

template <class ECategory>
void TMemoryUsageTrackerGuard<ECategory>::UpdateSize(i64 sizeDelta)
{
    Y_ASSERT(Tracker_);
    SetSize(Size_ + sizeDelta);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
