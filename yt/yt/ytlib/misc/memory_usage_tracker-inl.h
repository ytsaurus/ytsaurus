#pragma once
#ifndef MEMORY_USAGE_TRACKER_INL_H_
#error "Direct inclusion of this file is not allowed, include memory_usage_tracker.h"
// For the sake of sane code completion.
#include "memory_usage_tracker.h"
#endif

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class ECategory, class TPoolTag>
TMemoryUsageTracker<ECategory, TPoolTag>::TMemoryUsageTracker(
    i64 totalLimit,
    const std::vector<std::pair<ECategory, i64>>& limits,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : TotalLimit_(totalLimit)
    , TotalUsedGauge_("/total_used")
    , TotalFreeGauge_("/total_free")
    , Logger(logger)
    , Profiler(profiler)
{
    Profiler.Update(TotalFreeGauge_, totalLimit);

    for (auto category : TEnumTraits<ECategory>::GetDomainValues()) {
        Categories_[category].TagIdList = {CategoryTagCache_.GetTag(category)};
        Categories_[category].UsedGauge = NProfiling::TAtomicGauge(
            "/used",
            Categories_[category].TagIdList);
    }

    for (auto [category, limit] : limits) {
        Categories_[category].Limit = limit;
    }

    PeriodicUpdater_ = New<NConcurrency::TPeriodicExecutor>(
        NProfiling::TProfileManager::Get()->GetInvoker(),
        BIND(&TMemoryUsageTracker::UpdateMetrics, MakeWeak(this)),
        TDuration::Seconds(1));
    PeriodicUpdater_->Start();
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetTotalLimit() const
{
    return TotalLimit_;
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetTotalUsed() const
{
    return TotalUsedGauge_.GetCurrent();
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetTotalFree() const
{
    return std::max(
        GetTotalLimit() - GetTotalUsed(),
        static_cast<i64>(0));
}

template <class ECategory, class TPoolTag>
bool TMemoryUsageTracker<ECategory, TPoolTag>::IsTotalExceeded() const
{
    return TotalUsedGauge_.GetCurrent() > TotalLimit_;
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetLimit(ECategory category, const std::optional<TPoolTag>& poolTag) const
{
    auto categoryLimit = Categories_[category].Limit.load();

    if (poolTag) {
        TGuard guard(SpinLock_);

        auto* pool = FindPool(*poolTag);
        return GetPoolLimit(pool, categoryLimit);
    }

    return categoryLimit;
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetUsed(ECategory category, const std::optional<TPoolTag>& poolTag) const
{
    if (poolTag) {
        TGuard guard(SpinLock_);

        auto* pool = FindPool(*poolTag);
        return pool ? pool->Used[category].GetCurrent() : 0;
    } else {
        return Categories_[category].UsedGauge.GetCurrent();
    }
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetFree(ECategory category, const std::optional<TPoolTag>& poolTag) const
{
    i64 categoryLimit = GetLimit(category);
    i64 result = std::min(categoryLimit - GetUsed(category), GetTotalFree());

    if (poolTag) {
        TGuard guard(SpinLock_);

        if (auto* pool = FindPool(*poolTag)) {
            result = std::min(
                result,
                GetPoolLimit(pool, categoryLimit) - pool->Used[category].GetCurrent());
        } else {
            result = 0;
        }
    }

    return std::max<i64>(result, 0);
}

template <class ECategory, class TPoolTag>
bool TMemoryUsageTracker<ECategory, TPoolTag>::IsExceeded(ECategory category, const std::optional<TPoolTag>& poolTag) const
{
    if (IsTotalExceeded()) {
        return true;
    }

    const auto& data = Categories_[category];
    i64 categoryLimit = data.Limit.load();
    if (data.UsedGauge.GetCurrent() > categoryLimit) {
        return true;
    }

    if (poolTag) {
        TGuard guard(SpinLock_);

        if (auto* pool = FindPool(*poolTag)) {
            if (pool->Used[category].GetCurrent() > GetPoolLimit(pool, categoryLimit)) {
                return true;
            }
        }
    }

    return false;
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::SetTotalLimit(i64 newLimit)
{
    auto guard = Guard(SpinLock_);
    TotalLimit_ = newLimit;
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::SetCategoryLimit(ECategory category, i64 newLimit)
{
    auto guard = Guard(SpinLock_);
    Categories_[category].Limit = newLimit;
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::SetPoolWeight(const TPoolTag& poolTag, i64 newWeight)
{
    TGuard guard(SpinLock_);

    auto* pool = GetOrRegisterPool(poolTag);
    TotalPoolWeight_ += newWeight - pool->Weight;
    pool->Weight = newWeight;
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::Acquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag)
{
    TGuard guard(SpinLock_);

    auto* pool = poolTag ? GetOrRegisterPool(*poolTag) : nullptr;

    DoAcquire(category, size, pool);

    auto currentFree = TotalFreeGauge_.GetCurrent();
    if (currentFree < 0) {
        YT_LOG_WARNING("Total memory overcommit detected (Debt: %v, RequestCategory: %v, RequestSize: %v)",
            -currentFree,
            category,
            size);
    }

    const auto& data = Categories_[category];
    auto currentUsed = data.UsedGauge.GetCurrent();
    if (currentUsed > data.Limit) {
        YT_LOG_WARNING("Per-category memory overcommit detected (Debt: %v, RequestCategory: %v, RequestSize: %v)",
            currentUsed - data.Limit,
            category,
            size);
    }

    if (pool) {
        auto poolUsed = pool->Used[category].GetCurrent();
        auto poolLimit = GetPoolLimit(pool, data.Limit);
        if (poolUsed > poolLimit) {
            YT_LOG_WARNING("Per-pool memory overcommit detected (Debt: %v, RequestCategory: %v, PoolTag: %v, RequestSize: %v)",
                poolUsed - poolLimit,
                category,
                *poolTag,
                size);
        }
    }
}

template <class ECategory, class TPoolTag>
TError TMemoryUsageTracker<ECategory, TPoolTag>::TryAcquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag)
{
    TGuard guard(SpinLock_);

    i64 free = GetFree(category);
    if (size > GetFree(category)) {
        return TError(
            "Not enough memory to serve %Qlv acquisition request",
            category)
            << TErrorAttribute("bytes_free", free)
            << TErrorAttribute("bytes_requested", size);
    }

    auto* pool = poolTag ? GetOrRegisterPool(*poolTag) : nullptr;

    if (pool) {
        i64 free = GetPoolLimit(pool, GetLimit(category)) - pool->Used[category].GetCurrent();
        if (size > free) {
            return TError(
                "Not enough memory to serve %Qlv request in pool %v: free %v, requested %v",
                category,
                *poolTag,
                free,
                size);
        }
    }

    DoAcquire(category, size, pool);
    return TError();
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::DoAcquire(ECategory category, i64 size, TPool* pool)
{
    YT_VERIFY(size >= 0);

    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    Profiler.Increment(TotalUsedGauge_, +size);
    Profiler.Increment(TotalFreeGauge_, -size);
    Profiler.Increment(Categories_[category].UsedGauge, +size);

    if (pool) {
        Profiler.Increment(pool->Used[category], +size);
    }
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::Release(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag)
{
    YT_VERIFY(size >= 0);

    TGuard guard(SpinLock_);

    Profiler.Increment(TotalUsedGauge_, -size);
    Profiler.Increment(TotalFreeGauge_, +size);
    Profiler.Increment(Categories_[category].UsedGauge, -size);

    if (poolTag) {
        auto pool = GetOrRegisterPool(*poolTag);
        Profiler.Increment(pool->Used[category], -size);
    }
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::UpdateMetrics()
{
    Profiler.Increment(TotalUsedGauge_, 0);
    Profiler.Increment(TotalFreeGauge_, 0);

    for (auto& category : Categories_) {
        Profiler.Increment(category.UsedGauge, 0);
        Profiler.Enqueue(
            "/limit",
            std::min(category.Limit.load(), GetTotalLimit()),
            NProfiling::EMetricType::Gauge,
            category.TagIdList);
    }

    TGuard guard(PoolMapSpinLock_);
    for (auto& [poolTag, pool] : Pools_) {
        for (auto category : TEnumTraits<ECategory>::GetDomainValues()) {
            auto& usedCounter = pool.Used[category];
            if (usedCounter.GetCurrent() > 0) {
                Profiler.Increment(usedCounter, 0);
                Profiler.Enqueue(
                    "/pool_limit",
                    GetPoolLimit(&pool, Categories_[category].Limit.load()),
                    NProfiling::EMetricType::Gauge,
                    pool.TagIdLists[category]);
            }
        }
    }
}

template <class ECategory, class TPoolTag>
typename TMemoryUsageTracker<ECategory, TPoolTag>::TPool*
TMemoryUsageTracker<ECategory, TPoolTag>::GetOrRegisterPool(const TPoolTag& poolTag)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (auto it = Pools_.find(poolTag); it != Pools_.end()) {
        return &it->second;
    }

    TGuard guard(PoolMapSpinLock_);

    auto it = Pools_.emplace(poolTag, TPool{}).first;

    for (auto category : TEnumTraits<ECategory>::GetDomainValues()) {
        const auto& profileManager = NProfiling::TProfileManager::Get();
        it->second.TagIdLists[category] = {
            CategoryTagCache_.GetTag(category),
            profileManager->RegisterTag("pool", poolTag)
        };
        it->second.Used[category] = NProfiling::TAtomicGauge(
            "/pool_used",
            it->second.TagIdLists[category]);
    }

    return &it->second;
}

template <class ECategory, class TPoolTag>
typename TMemoryUsageTracker<ECategory, TPoolTag>::TPool*
TMemoryUsageTracker<ECategory, TPoolTag>::FindPool(const TPoolTag& poolTag)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    auto it = Pools_.find(poolTag);
    return it != Pools_.end() ? &it->second : nullptr;
}

template <class ECategory, class TPoolTag>
const typename TMemoryUsageTracker<ECategory, TPoolTag>::TPool*
TMemoryUsageTracker<ECategory, TPoolTag>::FindPool(const TPoolTag& poolTag) const
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    auto it = Pools_.find(poolTag);
    return it != Pools_.end() ? &it->second : nullptr;
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetPoolLimit(const TPool* pool, i64 categoryLimit) const
{
    categoryLimit = std::min(categoryLimit, GetTotalLimit());

    auto totalPoolWeight = TotalPoolWeight_.load();

    return (pool && totalPoolWeight > 0)
        ? static_cast<i64>(1.0 * categoryLimit * pool->Weight / totalPoolWeight)
        : 0;
}


template <class ECategory, class TPoolTag>
Y_FORCE_INLINE void Ref(TMemoryUsageTracker<ECategory, TPoolTag>* obj)
{
    obj->Ref();
}

template <class ECategory, class TPoolTag>
Y_FORCE_INLINE void Ref(const TMemoryUsageTracker<ECategory, TPoolTag>* obj)
{
    obj->Ref();
}

template <class ECategory, class TPoolTag>
Y_FORCE_INLINE void Unref(TMemoryUsageTracker<ECategory, TPoolTag>* obj)
{
    obj->Unref();
}

template <class ECategory, class TPoolTag>
Y_FORCE_INLINE void Unref(const TMemoryUsageTracker<ECategory, TPoolTag>* obj)
{
    obj->Unref();
}

////////////////////////////////////////////////////////////////////////////////

template <class ECategory, class TPoolTag>
TMemoryUsageTrackerGuard<ECategory, TPoolTag>::TMemoryUsageTrackerGuard(TMemoryUsageTrackerGuard&& other)
{
    MoveFrom(std::move(other));
}

template <class ECategory, class TPoolTag>
TMemoryUsageTrackerGuard<ECategory, TPoolTag>::~TMemoryUsageTrackerGuard()
{
    Release();
}

template <class ECategory, class TPoolTag>
TMemoryUsageTrackerGuard<ECategory, TPoolTag>& TMemoryUsageTrackerGuard<ECategory, TPoolTag>::operator=(TMemoryUsageTrackerGuard&& other)
{
    if (this != &other) {
        Release();
        MoveFrom(std::move(other));
    }
    return *this;
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTrackerGuard<ECategory, TPoolTag>::MoveFrom(TMemoryUsageTrackerGuard&& other)
{
    Tracker_ = other.Tracker_;
    Category_ = other.Category_;
    PoolTag_ = std::move(other.PoolTag_);
    Size_ = other.Size_;
    AcquiredSize_ = other.AcquiredSize_;
    Granularity_ = other.Granularity_;

    other.Tracker_ = nullptr;
    other.Size_ = 0;
    other.AcquiredSize_ = 0;
    other.Granularity_ = 0;
}

template <class ECategory, class TPoolTag>
void swap(TMemoryUsageTrackerGuard<ECategory, TPoolTag>& lhs, TMemoryUsageTrackerGuard<ECategory>& rhs)
{
    std::swap(lhs.Tracker_, rhs.Tracker_);
}

template <class ECategory, class TPoolTag>
TMemoryUsageTrackerGuard<ECategory, TPoolTag> TMemoryUsageTrackerGuard<ECategory, TPoolTag>::Acquire(
    TMemoryUsageTrackerPtr tracker,
    ECategory category,
    i64 size,
    std::optional<TPoolTag> poolTag,
    i64 granularity)
{
    YT_VERIFY(size >= 0);
    TMemoryUsageTrackerGuard guard;
    guard.Tracker_ = tracker;
    guard.Category_ = category;
    guard.PoolTag_ = std::move(poolTag);
    guard.Size_ = size;
    guard.Granularity_ = granularity;
    if (size >= granularity) {
        guard.AcquiredSize_ = size;
        tracker->Acquire(category, size, poolTag);
    }
    return guard;
}

template <class ECategory, class TPoolTag>
TErrorOr<TMemoryUsageTrackerGuard<ECategory, TPoolTag>> TMemoryUsageTrackerGuard<ECategory, TPoolTag>::TryAcquire(
    TMemoryUsageTrackerPtr tracker,
    ECategory category,
    i64 size,
    std::optional<TPoolTag> poolTag,
    i64 granularity)
{
    YT_VERIFY(size >= 0);
    auto error = tracker->TryAcquire(category, size, poolTag);
    if (!error.IsOK()) {
        return error;
    }
    TMemoryUsageTrackerGuard guard;
    guard.Tracker_ = tracker;
    guard.Category_ = category;
    guard.PoolTag_ = std::move(poolTag);
    guard.Size_ = size;
    guard.AcquiredSize_ = size;
    guard.Granularity_ = granularity;
    return std::move(guard);
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTrackerGuard<ECategory, TPoolTag>::Release()
{
    if (Tracker_) {
        Tracker_->Release(Category_, AcquiredSize_, PoolTag_);
        Tracker_ = nullptr;
        Size_ = 0;
        AcquiredSize_ = 0;
        Granularity_ = 0;
    }
}

template <class ECategory, class TPoolTag>
TMemoryUsageTrackerGuard<ECategory, TPoolTag>::operator bool() const
{
    return Tracker_.operator bool();
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTrackerGuard<ECategory, TPoolTag>::GetSize() const
{
    return Size_;
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTrackerGuard<ECategory, TPoolTag>::SetSize(i64 size)
{
    YT_VERIFY(Tracker_);
    YT_VERIFY(size >= 0);
    Size_ = size;
    if (std::abs(Size_ - AcquiredSize_) >= Granularity_) {
        if (Size_ > AcquiredSize_) {
            Tracker_->Acquire(Category_, Size_ - AcquiredSize_, PoolTag_);
        } else {
            Tracker_->Release(Category_, AcquiredSize_ - Size_, PoolTag_);
        }
        AcquiredSize_ = Size_;
    }
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTrackerGuard<ECategory, TPoolTag>::UpdateSize(i64 sizeDelta)
{
    SetSize(Size_ + sizeDelta);
}

////////////////////////////////////////////////////////////////////////////////

template <class ECategory>
TTypedMemoryTracker<ECategory>::TTypedMemoryTracker(
    TIntrusivePtr<TMemoryUsageTracker<ECategory>> memoryTracker,
    ECategory category)
    : MemoryTracker_(std::move(memoryTracker))
    , Category_(category)
{ }

template <class ECategory>
TError TTypedMemoryTracker<ECategory>::TryAcquire(size_t size)
{
    return MemoryTracker_->TryAcquire(Category_, size);
}

template <class ECategory>
void TTypedMemoryTracker<ECategory>::Release(size_t size)
{
    MemoryTracker_->Release(Category_, size);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
