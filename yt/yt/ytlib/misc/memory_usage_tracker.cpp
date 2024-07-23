#include "memory_usage_tracker.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <algorithm>

namespace NYT {

using namespace NLogging;
using namespace NProfiling;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

using ECategory = INodeMemoryTracker::ECategory;
using TPoolTag = INodeMemoryTracker::TPoolTag;

/////////////////////////////////////////////////////////////////////////////

constexpr int ReferenceAddressMapShardCount = 256;
constexpr int ReferenceAddressExpectedAlignmentLog = 4;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNodeMemoryTracker)

class TNodeMemoryTracker
    : public INodeMemoryTracker
{
public:
    TNodeMemoryTracker(
        i64 totalLimit,
        const std::vector<std::pair<ECategory, i64>>& limits,
        const TLogger& logger,
        const TProfiler& profiler);

    i64 GetTotalLimit() const override;
    i64 GetTotalUsed() const override;
    i64 GetTotalFree() const override;
    bool IsTotalExceeded() const override;

    i64 GetExplicitLimit(ECategory category) const override;
    i64 GetLimit(ECategory category, const std::optional<TPoolTag>& poolTag = {}) const override;
    i64 GetUsed(ECategory category, const std::optional<TPoolTag>& poolTag = {}) const override;
    i64 GetFree(ECategory category, const std::optional<TPoolTag>& poolTag = {}) const override;
    bool IsExceeded(ECategory category, const std::optional<TPoolTag>& poolTag = {}) const override;

    void SetTotalLimit(i64 newLimit) override;
    void SetCategoryLimit(ECategory category, i64 newLimit) override;
    void SetPoolWeight(const TPoolTag& poolTag, i64 newWeight) override;

    // Always succeeds, may lead to an overcommit.
    bool Acquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {}) override;
    TError TryAcquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {}) override;
    TError TryChange(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {}) override;
    void Release(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {}) override;
    i64 UpdateUsage(ECategory category, i64 newUsage) override;

    IMemoryUsageTrackerPtr WithCategory(
        ECategory category,
        std::optional<TPoolTag> poolTag = {}) override;

    void ClearTrackers() override;

    TSharedRef Track(TSharedRef reference, EMemoryCategory category, bool keepExistingTracking) override;
    TErrorOr<TSharedRef> TryTrack(
        TSharedRef reference,
        EMemoryCategory category,
        bool keepExistingTracking) override;

private:
    class TTrackedReferenceHolder
        : public TSharedRangeHolder
    {
    public:
        TTrackedReferenceHolder(
            TNodeMemoryTrackerPtr tracker,
            TSharedRef underlying,
            EMemoryCategory category)
            : Tracker_(std::move(tracker))
            , Underlying_(std::move(underlying))
            , Category_(category)
        { }

        ~TTrackedReferenceHolder() override
        {
            Tracker_->RemoveStateOrDecreaseUsageConter(Underlying_, Category_);
        }

        // TSharedRangeHolder overrides.
        TSharedRangeHolderPtr Clone(const TSharedRangeHolderCloneOptions& options) override
        {
            if (options.KeepMemoryReferenceTracking) {
                return this;
            }
            return Underlying_.GetHolder()->Clone(options);
        }

        std::optional<size_t> GetTotalByteSize() const override
        {
            return Underlying_.GetHolder()->GetTotalByteSize();
        }

    private:
        const TNodeMemoryTrackerPtr Tracker_;
        const TSharedRef Underlying_;
        const EMemoryCategory Category_;
    };

    const TLogger Logger;
    const TProfiler Profiler_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, SpinLock_);

    std::atomic<i64> TotalLimit_;

    std::atomic<i64> TotalUsed_ = 0;
    std::atomic<i64> TotalFree_ = 0;

    struct TCategory
    {
        std::atomic<i64> Limit = std::numeric_limits<i64>::max();
        std::atomic<i64> Used = 0;
    };

    TEnumIndexedArray<ECategory, TCategory> Categories_;

    struct TPool
        : public TRefCounted
        , public TNonCopyable
    {
        TPoolTag Tag;
        std::atomic<i64> Weight = 0;
        TEnumIndexedArray<ECategory, std::atomic<i64>> Used;

        TPool() = default;
    };

    THashMap<TPoolTag, TIntrusivePtr<TPool>> Pools_;
    std::atomic<i64> TotalPoolWeight_ = 0;

    struct TState
    {
        TRef Reference;
        THashMap<EMemoryCategory, i64> CategoryToUsage;
        TMemoryUsageTrackerGuard MemoryGuard;
    };

    using TReferenceKey = std::pair<uintptr_t, size_t>;

    struct TReferenceAddressMapShard
    {
        THashMap<TReferenceKey, TState> Map;
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock);
    };

    std::array<TReferenceAddressMapShard, ReferenceAddressMapShardCount> ReferenceAddressToState_;

    TEnumIndexedArray<EMemoryCategory, IMemoryUsageTrackerPtr> CategoryTrackers_;
    THashMap<TPoolTag, TEnumIndexedArray<EMemoryCategory, IMemoryUsageTrackerPtr>> PoolTrackers_;

    void InitCategoryTrackers();

    i64 DoGetLimit(ECategory category) const;
    i64 DoGetLimit(ECategory category, const TPool* pool) const;
    i64 DoGetUsed(ECategory category) const;
    i64 DoGetUsed(ECategory category, const TPool* pool) const;
    i64 DoGetFree(ECategory category) const;
    i64 DoGetFree(ECategory category, const TPool* pool) const;

    TError DoTryAcquire(ECategory category, i64 size, TPool* pool);
    void DoAcquire(ECategory category, i64 size, TPool* pool);
    void DoRelease(ECategory category, i64 size, TPool* pool);

    TPool* FindPool(const TPoolTag& poolTag);
    const TPool* FindPool(const TPoolTag& poolTag) const;
    TPool* GetOrRegisterPool(const TPoolTag& poolTag);
    TPool* GetOrRegisterPool(const std::optional<TPoolTag>& poolTag);

    TReferenceKey GetReferenceKey(TRef ref);
    TReferenceAddressMapShard& GetReferenceAddressMapShard(TReferenceKey key);
    TError TryCreateStateOrIncrementUsageCounter(TRef rawReference, EMemoryCategory category, bool allowOvercommit);
    void RemoveStateOrDecreaseUsageConter(TRef rawReference, EMemoryCategory category);
    TError TryChangeCategoryUsage(TState* state, EMemoryCategory category, i64 delta, bool allowOvercommit);
    std::optional<EMemoryCategory> GetCategoryByUsage(const THashMap<EMemoryCategory, i64>& usage);
    TErrorOr<TSharedRef> DoTryTrackMemory(
        TSharedRef reference,
        EMemoryCategory category,
        bool keepExistingTracking,
        bool allowOvercommit);
};

////////////////////////////////////////////////////////////////////////////////

class TMemoryUsageTracker
    : public IMemoryUsageTracker
{
public:
    TMemoryUsageTracker(
        INodeMemoryTrackerPtr memoryTracker,
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

    bool Acquire(i64 size) override
    {
        return MemoryTracker_->Acquire(Category_, size, PoolTag_);
    }

    void Release(i64 size) override
    {
        MemoryTracker_->Release(Category_, size, PoolTag_);
    }

    void SetLimit(i64 size) override
    {
        MemoryTracker_->SetCategoryLimit(Category_, size);
    }

    i64 GetLimit() const override
    {
        return MemoryTracker_->GetLimit(Category_, PoolTag_);
    }

    i64 GetUsed() const override
    {
        return MemoryTracker_->GetUsed(Category_, PoolTag_);
    }

    i64 GetFree() const override
    {
        return MemoryTracker_->GetFree(Category_, PoolTag_);
    }

    bool IsExceeded() const override
    {
        return MemoryTracker_->IsExceeded(Category_, PoolTag_);
    }

    TSharedRef Track(
        TSharedRef reference,
        bool keepHolder) override
    {
        return MemoryTracker_->Track(std::move(reference), Category_, keepHolder);
    }

    virtual TErrorOr<TSharedRef> TryTrack(
        TSharedRef reference,
        bool keepHolder) override
    {
        return MemoryTracker_->TryTrack(std::move(reference), Category_, keepHolder);
    }

private:
    const INodeMemoryTrackerPtr MemoryTracker_;
    const ECategory Category_;
    const std::optional<TPoolTag> PoolTag_;
};

////////////////////////////////////////////////////////////////////////////////

TNodeMemoryTracker::TNodeMemoryTracker(
    i64 totalLimit,
    const std::vector<std::pair<ECategory, i64>>& limits,
    const TLogger& logger,
    const TProfiler& profiler)
    : Logger(logger)
    , Profiler_(profiler.WithSparse())
    , TotalLimit_(totalLimit)
    , TotalFree_(totalLimit)
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

    InitCategoryTrackers();
}

void TNodeMemoryTracker::InitCategoryTrackers()
{
    for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        CategoryTrackers_[category] = New<TMemoryUsageTracker>(this, category, std::nullopt);
    }
}

void TNodeMemoryTracker::ClearTrackers()
{
    for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        CategoryTrackers_[category] = nullptr;
    }

    for (auto& it : PoolTrackers_) {
        for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
            it.second[category] = nullptr;
        }
    }
}

i64 TNodeMemoryTracker::GetTotalLimit() const
{
    return TotalLimit_.load();
}

i64 TNodeMemoryTracker::GetTotalUsed() const
{
    return TotalUsed_.load();
}

i64 TNodeMemoryTracker::GetTotalFree() const
{
    return std::max(
        GetTotalLimit() - GetTotalUsed(),
        static_cast<i64>(0));
}

bool TNodeMemoryTracker::IsTotalExceeded() const
{
    return GetTotalUsed() > GetTotalLimit();
}

i64 TNodeMemoryTracker::GetExplicitLimit(ECategory category) const
{
    return Categories_[category].Limit.load();
}

i64 TNodeMemoryTracker::GetLimit(
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

i64 TNodeMemoryTracker::DoGetLimit(ECategory category) const
{
    return std::min(Categories_[category].Limit.load(), GetTotalLimit());
}

i64 TNodeMemoryTracker::DoGetLimit(ECategory category, const TPool* pool) const
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

i64 TNodeMemoryTracker::GetUsed(ECategory category, const std::optional<TPoolTag>& poolTag) const
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

i64 TNodeMemoryTracker::DoGetUsed(ECategory category) const
{
    return Categories_[category].Used.load();
}

i64 TNodeMemoryTracker::DoGetUsed(ECategory category, const TPool* pool) const
{
    return pool->Used[category].load();
}

i64 TNodeMemoryTracker::GetFree(ECategory category, const std::optional<TPoolTag>& poolTag) const
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

i64 TNodeMemoryTracker::DoGetFree(ECategory category) const
{
    auto limit = DoGetLimit(category);
    auto used = DoGetUsed(category);
    return std::min(limit - used, GetTotalFree());
}

i64 TNodeMemoryTracker::DoGetFree(ECategory category, const TPool* pool) const
{
    auto limit = DoGetLimit(category, pool);
    auto used = DoGetUsed(category, pool);
    return std::min(limit - used, GetTotalFree());
}

bool TNodeMemoryTracker::IsExceeded(ECategory category, const std::optional<TPoolTag>& poolTag) const
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

void TNodeMemoryTracker::SetTotalLimit(i64 newLimit)
{
    YT_VERIFY(newLimit >= 0);

    auto guard = Guard(SpinLock_);

    auto delta = newLimit - TotalLimit_.load();

    TotalLimit_.store(newLimit);
    TotalFree_ += delta;
}

void TNodeMemoryTracker::SetCategoryLimit(ECategory category, i64 newLimit)
{
    YT_VERIFY(newLimit >= 0);

    auto guard = Guard(SpinLock_);

    Categories_[category].Limit.store(newLimit);
}

void TNodeMemoryTracker::SetPoolWeight(const TPoolTag& poolTag, i64 newWeight)
{
    YT_VERIFY(newWeight >= 0);

    auto guard = Guard(SpinLock_);

    auto* pool = GetOrRegisterPool(poolTag);
    TotalPoolWeight_ += newWeight - pool->Weight;
    pool->Weight = newWeight;
}

bool TNodeMemoryTracker::Acquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag)
{
    auto guard = Guard(SpinLock_);

    auto* pool = GetOrRegisterPool(poolTag);

    DoAcquire(category, size, pool);

    bool overcommitted = false;

    auto currentFree = TotalFree_.load();
    if (currentFree < 0) {
        overcommitted = true;

        YT_LOG_WARNING("Total memory overcommit detected (Debt: %v, RequestCategory: %v, RequestSize: %v)",
            -currentFree,
            category,
            size);
    }

    if (pool) {
        auto poolUsed = DoGetUsed(category, pool);
        auto poolLimit = DoGetLimit(category, pool);
        if (poolUsed > poolLimit) {
            overcommitted = true;

            YT_LOG_WARNING("Per-pool memory overcommit detected (Debt: %v, RequestCategory: %v, PoolTag: %v, RequestSize: %v)",
                poolUsed - poolLimit,
                category,
                *poolTag,
                size);
        }
    }

    return !overcommitted;
}

TError TNodeMemoryTracker::TryAcquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag)
{
    auto guard = Guard(SpinLock_);

    auto* pool = GetOrRegisterPool(poolTag);

    return DoTryAcquire(category, size, pool);
}

TError TNodeMemoryTracker::TryChange(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag)
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

TError TNodeMemoryTracker::DoTryAcquire(ECategory category, i64 size, TPool* pool)
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

void TNodeMemoryTracker::DoAcquire(ECategory category, i64 size, TPool* pool)
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

void TNodeMemoryTracker::DoRelease(ECategory category, i64 size, TPool* pool)
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

void TNodeMemoryTracker::Release(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag)
{
    auto guard = Guard(SpinLock_);

    auto* pool = GetOrRegisterPool(poolTag);

    DoRelease(category, size, pool);
}

i64 TNodeMemoryTracker::UpdateUsage(ECategory category, i64 newUsage)
{
    auto oldUsage = GetUsed(category);
    if (oldUsage < newUsage) {
        Acquire(category, newUsage - oldUsage);
    } else {
        Release(category, oldUsage - newUsage);
    }
    return oldUsage;
}

IMemoryUsageTrackerPtr TNodeMemoryTracker::WithCategory(
    ECategory category,
    std::optional<TPoolTag> poolTag)
{
    if (poolTag) {
        auto guard = Guard(SpinLock_);

        auto it = PoolTrackers_.find(poolTag.value());

        if (it.IsEnd()) {
            TEnumIndexedArray<EMemoryCategory, IMemoryUsageTrackerPtr> trackers;
            auto tracker = New<TMemoryUsageTracker>(
                this,
                category,
                std::move(poolTag));
            trackers[category] = tracker;
            PoolTrackers_.insert({poolTag.value(), std::move(trackers)});
            return tracker;
        } else {
            auto& trackers = it->second;

            if (auto tracker = trackers[category]) {
                return tracker;
            } else {
                tracker = New<TMemoryUsageTracker>(
                    this,
                    category,
                    std::move(poolTag));
                trackers[category] = tracker;
                return tracker;
            }
        }
    } else {
        auto tracker = CategoryTrackers_[category];

        YT_VERIFY(tracker != nullptr);

        return tracker;
    }
}

TNodeMemoryTracker::TReferenceKey TNodeMemoryTracker::GetReferenceKey(TRef ref)
{
    YT_VERIFY(ref);
    return TReferenceKey(reinterpret_cast<uintptr_t>(ref.Begin()), ref.Size());
}

TNodeMemoryTracker::TReferenceAddressMapShard& TNodeMemoryTracker::GetReferenceAddressMapShard(TReferenceKey key)
{
    return ReferenceAddressToState_[(key.first >> ReferenceAddressExpectedAlignmentLog) % ReferenceAddressMapShardCount];
}

TSharedRef TNodeMemoryTracker::Track(
    TSharedRef reference,
    EMemoryCategory category,
    bool keepExistingTracking)
{
    auto refOrError = DoTryTrackMemory(
        std::move(reference),
        category,
        keepExistingTracking,
        /*allowOvercommit*/ true);

    YT_VERIFY(refOrError.IsOK());
    return refOrError.Value();
}

TErrorOr<TSharedRef> TNodeMemoryTracker::TryTrack(
    TSharedRef reference,
    EMemoryCategory category,
    bool keepExistingTracking)
{
    return DoTryTrackMemory(
        std::move(reference),
        category,
        keepExistingTracking,
        /*allowOvercommit*/ false);
}

TErrorOr<TSharedRef> TNodeMemoryTracker::DoTryTrackMemory(
    TSharedRef reference,
    EMemoryCategory category,
    bool keepExistingTracking,
    bool allowOvercommit)
{
    if (!reference) {
        return reference;
    }

    auto rawReference = TRef(reference);
    const auto& holder = reference.GetHolder();

    // Reference could be without a holder, e.g. empty reference.
    if (!holder) {
        YT_VERIFY(reference.Begin() == TRef::MakeEmpty().Begin());
        return reference;
    }

    auto error = TryCreateStateOrIncrementUsageCounter(rawReference, category, allowOvercommit);
    if (!error.IsOK()) {
        return error;
    }

    auto underlyingHolder = holder->Clone({.KeepMemoryReferenceTracking = keepExistingTracking});
    auto underlyingReference = TSharedRef(rawReference, std::move(underlyingHolder));
    return TSharedRef(
        rawReference,
        New<TTrackedReferenceHolder>(this, std::move(underlyingReference), category));
}

TError TNodeMemoryTracker::TryCreateStateOrIncrementUsageCounter(
    TRef rawReference,
    EMemoryCategory category,
    bool allowOvercommit)
{
    auto key = GetReferenceKey(rawReference);
    auto& shard = GetReferenceAddressMapShard(key);

    auto guard = Guard(shard.SpinLock);

    if (auto it = shard.Map.find(key); it != shard.Map.end()) {
        return TryChangeCategoryUsage(&it->second, category, /*delta*/ 1, allowOvercommit);
    }

    auto it = EmplaceOrCrash(shard.Map, key, TState{.Reference = rawReference});
    return TryChangeCategoryUsage(&it->second, category, /*delta*/ 1, allowOvercommit);
}

void TNodeMemoryTracker::RemoveStateOrDecreaseUsageConter(TRef rawReference, EMemoryCategory category)
{
    auto key = GetReferenceKey(rawReference);
    auto& shard = GetReferenceAddressMapShard(key);
    auto guard = Guard(shard.SpinLock);

    auto it = GetIteratorOrCrash(shard.Map, key);
    auto& state = it->second;

    // Overcommit is not expected when while state is removing, because the counter is not incremented.
    YT_VERIFY(TryChangeCategoryUsage(&state, category, /*delta*/ -1, /*allowOvercommit*/ true)
        .IsOK());

    if (state.CategoryToUsage.empty()) {
        shard.Map.erase(it);
    }
}

TError TNodeMemoryTracker::TryChangeCategoryUsage(
    TState* state,
    EMemoryCategory category,
    i64 delta,
    bool allowOvercommit)
{
    auto oldCategory = GetCategoryByUsage(state->CategoryToUsage);

    if (state->CategoryToUsage.contains(category) &&
        state->CategoryToUsage[category] + delta != 0)
    {
        state->CategoryToUsage[category] += delta;
        return TError();
    }

    state->CategoryToUsage[category] += delta;

    if (state->CategoryToUsage[category] == 0) {
        state->CategoryToUsage.erase(category);
    }

    auto newCategory = GetCategoryByUsage(state->CategoryToUsage);
    if (!newCategory) {
        state->MemoryGuard.Release();
        return TError();
    }

    if ((oldCategory && newCategory && *oldCategory != *newCategory) || !oldCategory) {
        if (allowOvercommit) {
            state->MemoryGuard = TMemoryUsageTrackerGuard::Acquire(
                WithCategory(*newCategory),
                static_cast<i64>(state->Reference.Size()));
        } else {
            auto guardOrError = TMemoryUsageTrackerGuard::TryAcquire(
                WithCategory(*newCategory),
                static_cast<i64>(state->Reference.Size()));
            if (!guardOrError.IsOK()) {
                return guardOrError;
            }
            state->MemoryGuard = std::move(guardOrError.Value());
        }
    }

    return TError();
}

std::optional<EMemoryCategory> TNodeMemoryTracker::GetCategoryByUsage(const THashMap<EMemoryCategory, i64>& usage)
{
    auto anotherCategory = [&] (EMemoryCategory skipCategory) {
        for (auto& [category, _]: usage) {
            if (category != skipCategory) {
                return category;
            }
        }
        YT_ABORT();
    };

    if (usage.empty()) {
        return std::nullopt;
    }

    if (usage.size() == 1) {
        return usage.begin()->first;
    }

    if (usage.size() == 2) {
        if (usage.contains(EMemoryCategory::Unknown)) {
            return anotherCategory(EMemoryCategory::Unknown);
        }
        if (usage.contains(EMemoryCategory::BlockCache)) {
            return anotherCategory(EMemoryCategory::BlockCache);
        }
        return EMemoryCategory::Mixed;
    }

    if (usage.size() == 3) {
        if (usage.contains(EMemoryCategory::Unknown) && usage.contains(EMemoryCategory::BlockCache)) {
            for (auto [category, _]: usage) {
                if (category != EMemoryCategory::Unknown && category != EMemoryCategory::BlockCache) {
                    return category;
                }
            }
        }
        return EMemoryCategory::Mixed;
    }

    return EMemoryCategory::Mixed;
}

typename TNodeMemoryTracker::TPool*
TNodeMemoryTracker::GetOrRegisterPool(const TPoolTag& poolTag)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (auto it = Pools_.find(poolTag); it != Pools_.end()) {
        return it->second.Get();
    }

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

typename TNodeMemoryTracker::TPool*
TNodeMemoryTracker::GetOrRegisterPool(const std::optional<TPoolTag>& poolTag)
{
    return poolTag ? GetOrRegisterPool(*poolTag) : nullptr;
}

typename TNodeMemoryTracker::TPool*
TNodeMemoryTracker::FindPool(const TPoolTag& poolTag)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    auto it = Pools_.find(poolTag);
    return it != Pools_.end() ? it->second.Get() : nullptr;
}

const typename TNodeMemoryTracker::TPool*
TNodeMemoryTracker::FindPool(const TPoolTag& poolTag) const
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    auto it = Pools_.find(poolTag);
    return it != Pools_.end() ? it->second.Get() : nullptr;
}

Y_FORCE_INLINE void Ref(TNodeMemoryTracker* obj)
{
    obj->Ref();
}

Y_FORCE_INLINE void Ref(const TNodeMemoryTracker* obj)
{
    obj->Ref();
}

Y_FORCE_INLINE void Unref(TNodeMemoryTracker* obj)
{
    obj->Unref();
}

Y_FORCE_INLINE void Unref(const TNodeMemoryTracker* obj)
{
    obj->Unref();
}

////////////////////////////////////////////////////////////////////////////////

IMemoryUsageTrackerPtr WithCategory(
    const INodeMemoryTrackerPtr& memoryTracker,
    EMemoryCategory category,
    std::optional<INodeMemoryTracker::TPoolTag> poolTag)
{
    if (!memoryTracker) {
        return {};
    }

    return memoryTracker->WithCategory(category, std::move(poolTag));
}

////////////////////////////////////////////////////////////////////////////////

INodeMemoryTrackerPtr CreateNodeMemoryTracker(
    i64 totalLimit,
    const std::vector<std::pair<ECategory, i64>>& limits,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
{
    return New<TNodeMemoryTracker>(
        totalLimit,
        limits,
        logger,
        profiler);
}

/////////////////////////////////////////////////////////////////////////////

class TDelayedReferenceHolder
    : public TSharedRangeHolder
{
public:
    TDelayedReferenceHolder(
        TSharedRef underlying,
        TDuration delayBeforeFree,
        IInvokerPtr dtorInvoker)
        : Underlying_(std::move(underlying))
        , DelayBeforeFree_(delayBeforeFree)
        , DtorInvoker_(std::move(dtorInvoker))
    { }

    TSharedRangeHolderPtr Clone(const TSharedRangeHolderCloneOptions& options) override
    {
        if (options.KeepMemoryReferenceTracking) {
            return this;
        }
        return Underlying_.GetHolder()->Clone(options);
    }

    std::optional<size_t> GetTotalByteSize() const override
    {
        return Underlying_.GetHolder()->GetTotalByteSize();
    }

    ~TDelayedReferenceHolder()
    {
        NConcurrency::TDelayedExecutor::Submit(
            BIND([] (TSharedRef reference) {
                reference.ReleaseHolder();
            }, Passed(std::move(Underlying_))),
            DelayBeforeFree_,
            DtorInvoker_);
    }

private:
    TSharedRef Underlying_;
    const TDuration DelayBeforeFree_;
    const IInvokerPtr DtorInvoker_;
};

////////////////////////////////////////////////////////////////////////////////

TSharedRef WrapWithDelayedReferenceHolder(
    TSharedRef reference,
    TDuration delayBeforeFree,
    IInvokerPtr dtorInvoker)
{
    YT_VERIFY(dtorInvoker);

    auto underlyingHolder = reference.GetHolder();
    auto underlyingReference = TSharedRef(reference, std::move(underlyingHolder));
    return TSharedRef(
        reference,
        New<TDelayedReferenceHolder>(std::move(underlyingReference), delayBeforeFree, dtorInvoker));
}

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TSharedRef> TryTrackMemory(
    const INodeMemoryTrackerPtr& tracker,
    EMemoryCategory category,
    TSharedRef reference,
    bool keepExistingTracking)
{
    if (!tracker) {
        return reference;
    }
    return TryTrackMemory(
        tracker->WithCategory(category),
        std::move(reference),
        keepExistingTracking);
}

TSharedRef TrackMemory(
    const INodeMemoryTrackerPtr& tracker,
    EMemoryCategory category,
    TSharedRef reference,
    bool keepExistingTracking)
{
    if (!tracker) {
        return reference;
    }
    return TrackMemory(
        tracker->WithCategory(category),
        std::move(reference),
        keepExistingTracking);
}

TSharedRefArray TrackMemory(
    const INodeMemoryTrackerPtr& tracker,
    EMemoryCategory category,
    TSharedRefArray array,
    bool keepExistingTracking)
{
    if (!tracker) {
        return array;
    }
    return TrackMemory(
        tracker->WithCategory(category),
        std::move(array),
        keepExistingTracking);
}

////////////////////////////////////////////////////////////////////////////////

class TReservingMemoryTracker
    : public IReservingMemoryUsageTracker
{
public:
    TReservingMemoryTracker(
        IMemoryUsageTrackerPtr underlying,
        TCounter memoryUsageCounter)
        : Underlying_(std::move(underlying))
        , MemoryUsageCounter_(std::move(memoryUsageCounter))
    { }

    ~TReservingMemoryTracker()
    {
        Underlying_->Release(UnderlyingAllocatedSize_);
        MemoryUsageCounter_.Increment(-UnderlyingAllocatedSize_);
    }

    TError TryAcquire(i64 size) override
    {
        YT_VERIFY(size >= 0);

        auto guard = Guard(SpinLock_);
        i64 reservedAmount = UnderlyingAllocatedSize_ - AllocatedSize_;
        if (auto toAquire = size - reservedAmount; toAquire > 0) {
            auto acquireResult = Underlying_->TryAcquire(toAquire);
            if (!acquireResult.IsOK()) {
                return acquireResult;
            }
            UnderlyingAllocatedSize_ += toAquire;
            MemoryUsageCounter_.Increment(toAquire);
        }

        AllocatedSize_ += size;

        return {};
    }

    TError TryChange(i64 /*size*/) override
    {
        return TError("Setting is not supported for reserve memory tracker");
    }

    bool Acquire(i64 size) override
    {
        YT_VERIFY(size >= 0);

        auto guard = Guard(SpinLock_);
        i64 reservedAmount = UnderlyingAllocatedSize_ - AllocatedSize_;
        bool result = true;
        if (auto toAquire = size - reservedAmount; toAquire > 0) {
            result = Underlying_->Acquire(toAquire);
            UnderlyingAllocatedSize_ += toAquire;
            MemoryUsageCounter_.Increment(toAquire);
        }

        AllocatedSize_ += size;
        return result;
    }

    void Release(i64 size) override
    {
        YT_VERIFY(size >= 0);

        auto guard = Guard(SpinLock_);
        AllocatedSize_ -= size;
    }

    void SetLimit(i64 size) override
    {
        Underlying_->SetLimit(size);
    }

    i64 GetLimit() const override
    {
        return Underlying_->GetLimit();
    }

    i64 GetUsed() const override
    {
        return Underlying_->GetUsed();
    }

    i64 GetFree() const override
    {
        auto guard = Guard(SpinLock_);
        return Underlying_->GetFree() + UnderlyingAllocatedSize_ - AllocatedSize_;
    }

    bool IsExceeded() const override
    {
        return Underlying_->IsExceeded();
    }

    TError TryReserve(i64 size) override
    {
        auto guard = Guard(SpinLock_);
        auto reserveResult = Underlying_->TryAcquire(size);
        if (reserveResult.IsOK()) {
            UnderlyingAllocatedSize_ += size;
            MemoryUsageCounter_.Increment(size);
        }

        return reserveResult;
    }

    TSharedRef Track(TSharedRef reference, bool keepHolder) override
    {
        return Underlying_->Track(std::move(reference), keepHolder);
    }

    TErrorOr<TSharedRef> TryTrack(TSharedRef reference, bool keepHolder) override
    {
        return Underlying_->TryTrack(std::move(reference), keepHolder);
    }

    void ReleaseUnusedReservation() override
    {
        auto guard = Guard(SpinLock_);
        if (auto releaseAmount = UnderlyingAllocatedSize_ - AllocatedSize_; releaseAmount > 0) {
            Underlying_->Release(releaseAmount);
            MemoryUsageCounter_.Increment(-releaseAmount);
            UnderlyingAllocatedSize_ -= releaseAmount;
        }
    }

private:
    const IMemoryUsageTrackerPtr Underlying_;
    const TCounter MemoryUsageCounter_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    // Amount we allocated from Underlying_.
    i64 UnderlyingAllocatedSize_ = 0;
    // Amount that was allocated by users (UnderlyingAllocatedSize_ >= AllocatedSize_).
    i64 AllocatedSize_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TReservingMemoryTracker)

////////////////////////////////////////////////////////////////////////////////

IReservingMemoryUsageTrackerPtr CreateResevingMemoryUsageTracker(
    IMemoryUsageTrackerPtr underlying,
    TCounter memoryUsageCounter)
{
    return New<TReservingMemoryTracker>(std::move(underlying), std::move(memoryUsageCounter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
