#pragma once
#ifndef MEMORY_USAGE_TRACKER_INL_H_
#error "Direct inclusion of this file is not allowed, include memory_usage_tracker.h"
// For the sake of sane code completion.
#include "memory_usage_tracker.h"
#endif

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <algorithm>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class ECategory, class TPoolTag>
class TTypedMemoryTracker
    : public IMemoryUsageTracker
{
public:
    TTypedMemoryTracker(
        TIntrusivePtr<TMemoryUsageTracker<ECategory, TPoolTag>> memoryTracker,
        ECategory category,
        std::optional<TPoolTag> poolTag)
        : MemoryTracker_(std::move(memoryTracker))
        , Category_(category)
        , PoolTag_(std::move(poolTag))
    { }

    TError TryAcquire(i64 size) override
    {
        return MemoryTracker_->TryAcquire(Category_, size, PoolTag_);
    }

    TError TryChange(i64 size) override
    {
        return MemoryTracker_->TryChange(Category_, size, PoolTag_);
    }

    void Acquire(i64 size) override
    {
        MemoryTracker_->Acquire(Category_, size, PoolTag_);
    }

    void Release(i64 size) override
    {
        MemoryTracker_->Release(Category_, size, PoolTag_);
    }

    void SetLimit(i64 size) override
    {
        MemoryTracker_->SetCategoryLimit(Category_, size);
    }

private:
    const TIntrusivePtr<TMemoryUsageTracker<ECategory, TPoolTag>> MemoryTracker_;
    const ECategory Category_;
    const std::optional<TPoolTag> PoolTag_;
};

////////////////////////////////////////////////////////////////////////////////

template <class ECategory, class TPoolTag>
TMemoryUsageTracker<ECategory, TPoolTag>::TMemoryUsageTracker(
    i64 totalLimit,
    const std::vector<std::pair<ECategory, i64>>& limits,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : TotalLimit_(totalLimit)
    , TotalFree_(totalLimit)
    , Logger(logger)
    , Profiler_(profiler.WithSparse())
{
    profiler.AddFuncGauge("/total_limit", MakeStrong(this), [this] {
        return GetTotalLimit();
    });
    profiler.AddFuncGauge("/total_used", MakeStrong(this), [this] {
        return GetTotalUsed();
    });
    profiler.AddFuncGauge("/total_free", MakeStrong(this), [this] {
        return GetTotalFree();
    });

    for (auto category : TEnumTraits<ECategory>::GetDomainValues()) {
        auto categoryProfiler = profiler.WithTag("category", FormatEnum(category));

        categoryProfiler.AddFuncGauge("/used", MakeStrong(this), [this, category] {
            return DoGetUsed(category);
        });
        categoryProfiler.AddFuncGauge("/limit", MakeStrong(this), [this, category] {
            return DoGetLimit(category);
        });
    }

    for (auto [category, limit] : limits) {
        YT_VERIFY(limit >= 0);
        Categories_[category].Limit.store(limit);
    }
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetTotalLimit() const
{
    return TotalLimit_.load();
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetTotalUsed() const
{
    return TotalUsed_.load();
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
    return GetTotalUsed() > GetTotalLimit();
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetExplicitLimit(ECategory category) const
{
    return Categories_[category].Limit.load();
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetLimit(
    ECategory category,
    const std::optional<TPoolTag>& poolTag) const
{
    if (!poolTag) {
        return DoGetLimit(category);
    }

    auto guard = Guard(SpinLock_);

    auto* pool = FindPool(*poolTag);

    return DoGetLimit(category, pool);
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::DoGetLimit(ECategory category) const
{
    return std::min(Categories_[category].Limit.load(), GetTotalLimit());
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::DoGetLimit(ECategory category, const TPool* pool) const
{
    auto result = DoGetLimit(category);

    if (!pool) {
        return result;
    }

    auto totalPoolWeight = TotalPoolWeight_.load();

    if (totalPoolWeight <= 0) {
        return 0;
    }

    auto fpResult = 1.0 * result * pool->Weight / totalPoolWeight;
    return fpResult >= std::numeric_limits<i64>::max() ? std::numeric_limits<i64>::max() : static_cast<i64>(fpResult);
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetUsed(ECategory category, const std::optional<TPoolTag>& poolTag) const
{
    if (!poolTag) {
        return DoGetUsed(category);
    }

    auto guard = Guard(SpinLock_);

    auto* pool = FindPool(*poolTag);

    if (!pool) {
        return 0;
    }

    return DoGetUsed(category, pool);
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::DoGetUsed(ECategory category) const
{
    return Categories_[category].Used.load();
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::DoGetUsed(ECategory category, const TPool* pool) const
{
    return pool->Used[category].load();
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::GetFree(ECategory category, const std::optional<TPoolTag>& poolTag) const
{
    auto freeMemory = std::max(static_cast<i64>(0), DoGetFree(category));

    if (!poolTag) {
        return freeMemory;
    }

    auto guard = Guard(SpinLock_);

    auto* pool = FindPool(*poolTag);

    if (!pool) {
        return 0;
    }

    auto poolFreeMemory = std::max(static_cast<i64>(0), DoGetFree(category, pool));
    return std::min(freeMemory, poolFreeMemory);
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::DoGetFree(ECategory category) const
{
    auto limit = DoGetLimit(category);
    auto used = DoGetUsed(category);
    return std::min(limit - used, GetTotalFree());
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::DoGetFree(ECategory category, const TPool* pool) const
{
    auto limit = DoGetLimit(category, pool);
    auto used = DoGetUsed(category, pool);
    return std::min(limit - used, GetTotalFree());
}

template <class ECategory, class TPoolTag>
bool TMemoryUsageTracker<ECategory, TPoolTag>::IsExceeded(ECategory category, const std::optional<TPoolTag>& poolTag) const
{
    if (IsTotalExceeded()) {
        return true;
    }

    if (DoGetUsed(category) > DoGetLimit(category)) {
        return true;
    }

    if (!poolTag) {
        return false;
    }

    auto guard = Guard(SpinLock_);

    auto* pool = FindPool(*poolTag);

    if (!pool) {
        return false;
    }

    return DoGetUsed(category, pool) > DoGetLimit(category, pool);
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::SetTotalLimit(i64 newLimit)
{
    YT_VERIFY(newLimit >= 0);

    auto guard = Guard(SpinLock_);

    TotalLimit_.store(newLimit);
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::SetCategoryLimit(ECategory category, i64 newLimit)
{
    YT_VERIFY(newLimit >= 0);

    auto guard = Guard(SpinLock_);

    Categories_[category].Limit.store(newLimit);
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::SetPoolWeight(const TPoolTag& poolTag, i64 newWeight)
{
    YT_VERIFY(newWeight >= 0);

    auto guard = Guard(SpinLock_);

    auto* pool = GetOrRegisterPool(poolTag);
    TotalPoolWeight_ += newWeight - pool->Weight;
    pool->Weight = newWeight;
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::Acquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag)
{
    auto guard = Guard(SpinLock_);

    auto* pool = GetOrRegisterPool(poolTag);

    DoAcquire(category, size, pool);

    auto currentFree = TotalFree_.load();
    if (currentFree < 0) {
        YT_LOG_WARNING("Total memory overcommit detected (Debt: %v, RequestCategory: %v, RequestSize: %v)",
            -currentFree,
            category,
            size);
    }

    const auto& data = Categories_[category];
    auto currentUsed = data.Used.load();
    if (currentUsed > data.Limit) {
        YT_LOG_WARNING("Per-category memory overcommit detected (Debt: %v, RequestCategory: %v, RequestSize: %v)",
            currentUsed - data.Limit,
            category,
            size);
    }

    if (pool) {
        auto poolUsed = DoGetUsed(category, pool);
        auto poolLimit = DoGetLimit(category, pool);
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
    auto guard = Guard(SpinLock_);

    auto* pool = GetOrRegisterPool(poolTag);

    return DoTryAcquire(category, size, pool);
}

template <class ECategory, class TPoolTag>
TError TMemoryUsageTracker<ECategory, TPoolTag>::TryChange(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag)
{
    YT_VERIFY(size >= 0);

    auto guard = Guard(SpinLock_);

    auto* pool = GetOrRegisterPool(poolTag);

    auto currentSize = DoGetUsed(category, pool);
    if (size > currentSize) {
        return DoTryAcquire(category, size - currentSize, pool);
    } else if (size < currentSize) {
        DoRelease(category, currentSize - size, pool);
    }
    return {};
}

template <class ECategory, class TPoolTag>
TError TMemoryUsageTracker<ECategory, TPoolTag>::DoTryAcquire(ECategory category, i64 size, TPool* pool)
{
    YT_VERIFY(size >= 0);
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    auto freeMemory = DoGetFree(category);
    if (size > freeMemory) {
        return TError(
            "Not enough memory to serve %Qlv acquisition request",
            category)
            << TErrorAttribute("bytes_free", freeMemory)
            << TErrorAttribute("bytes_requested", size);
    }

    if (pool) {
        auto poolFreeMemory = DoGetFree(category, pool);
        if (size > poolFreeMemory) {
            return TError(
                "Not enough memory to serve %Qlv request in pool %Qv",
                category,
                pool->Tag)
                << TErrorAttribute("bytes_free", poolFreeMemory)
                << TErrorAttribute("bytes_requested", size);
        }
    }

    DoAcquire(category, size, pool);

    return {};
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::DoAcquire(ECategory category, i64 size, TPool* pool)
{
    YT_VERIFY(size >= 0);
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    TotalUsed_ += size;
    TotalFree_ -= size;
    Categories_[category].Used += size;

    if (pool) {
        pool->Used[category] += size;
    }
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::DoRelease(ECategory category, i64 size, TPool* pool)
{
    YT_VERIFY(size >= 0);
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    TotalUsed_ -= size;
    TotalFree_ += size;
    Categories_[category].Used -= size;

    if (pool) {
        pool->Used[category] -= size;
    }
}

template <class ECategory, class TPoolTag>
void TMemoryUsageTracker<ECategory, TPoolTag>::Release(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag)
{
    auto guard = Guard(SpinLock_);

    auto* pool = GetOrRegisterPool(poolTag);

    DoRelease(category, size, pool);
}

template <class ECategory, class TPoolTag>
i64 TMemoryUsageTracker<ECategory, TPoolTag>::UpdateUsage(ECategory category, i64 newUsage)
{
    auto oldUsage = GetUsed(category);
    if (oldUsage < newUsage) {
        Acquire(category, newUsage - oldUsage);
    } else {
        Release(category, oldUsage - newUsage);
    }
    return oldUsage;
}

template <class ECategory, class TPoolTag>
IMemoryUsageTrackerPtr TMemoryUsageTracker<ECategory, TPoolTag>::WithCategory(
    ECategory category,
    std::optional<TPoolTag> poolTag)
{
    return New<TTypedMemoryTracker<ECategory, TPoolTag>>(
        this,
        category,
        std::move(poolTag));
}

template <class ECategory, class TPoolTag>
typename TMemoryUsageTracker<ECategory, TPoolTag>::TPool*
TMemoryUsageTracker<ECategory, TPoolTag>::GetOrRegisterPool(const TPoolTag& poolTag)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (auto it = Pools_.find(poolTag); it != Pools_.end()) {
        return it->second.Get();
    }

    auto guard = Guard(PoolMapSpinLock_);

    auto pool = New<TPool>();
    pool->Tag = poolTag;
    for (auto category : TEnumTraits<ECategory>::GetDomainValues()) {
        pool->Used[category].store(0);

        auto categoryProfiler = Profiler_
            .WithTag("category", FormatEnum(category))
            .WithTag("pool", ToString(poolTag));

        categoryProfiler.AddFuncGauge("/pool_used", pool, [pool = pool.Get(), category] {
            return pool->Used[category].load();
        });

        categoryProfiler.AddFuncGauge("/pool_limit", pool, [this, pool = pool.Get(), this_ = MakeStrong(this), category] {
            return DoGetLimit(category, pool);
        });
    }

    Pools_.emplace(poolTag, pool);
    return pool.Get();
}

template <class ECategory, class TPoolTag>
typename TMemoryUsageTracker<ECategory, TPoolTag>::TPool*
TMemoryUsageTracker<ECategory, TPoolTag>::GetOrRegisterPool(const std::optional<TPoolTag>& poolTag)
{
    return poolTag ? GetOrRegisterPool(*poolTag) : nullptr;
}

template <class ECategory, class TPoolTag>
typename TMemoryUsageTracker<ECategory, TPoolTag>::TPool*
TMemoryUsageTracker<ECategory, TPoolTag>::FindPool(const TPoolTag& poolTag)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    auto it = Pools_.find(poolTag);
    return it != Pools_.end() ? it->second.Get() : nullptr;
}

template <class ECategory, class TPoolTag>
const typename TMemoryUsageTracker<ECategory, TPoolTag>::TPool*
TMemoryUsageTracker<ECategory, TPoolTag>::FindPool(const TPoolTag& poolTag) const
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    auto it = Pools_.find(poolTag);
    return it != Pools_.end() ? it->second.Get() : nullptr;
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

} // namespace NYT
