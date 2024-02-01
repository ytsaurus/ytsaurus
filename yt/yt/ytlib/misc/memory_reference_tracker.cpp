#include "memory_reference_tracker.h"

#include "config.h"

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/memory_reference_tracker.h>

#include <library/cpp/yt/threading/public.h>

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

constexpr int ReferenceAddressMapShardCount = 256;
constexpr int ReferenceAddressExpectedAlignmentLog = 4;

/////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNodeMemoryReferenceTracker)

class TNodeMemoryReferenceTracker
    : public INodeMemoryReferenceTracker
{
public:
    explicit TNodeMemoryReferenceTracker(INodeMemoryTrackerPtr memoryTracker)
        : MemoryTracker_(std::move(memoryTracker))
    {
        InitCategoryTrackers();
    }

    const IMemoryReferenceTrackerPtr& WithCategory(EMemoryCategory category) override
    {
        YT_VERIFY(category != EMemoryCategory::Mixed);

        return CategoryTrackers_[category];
    }

    void Reconfigure(TNodeMemoryReferenceTrackerConfigPtr config) override
    {
        Enabled_ = config->EnableMemoryReferenceTracker;
    }

private:
    class TMemoryReferenceTracker
        : public IMemoryReferenceTracker
    {
    public:
        TMemoryReferenceTracker(
            TNodeMemoryReferenceTrackerPtr owner,
            EMemoryCategory category)
            : Owner_(std::move(owner))
            , Category_(category)
        { }

        TSharedRef Track(TSharedRef reference, bool keepHolder = false) override
        {
            if (!reference) {
                return reference;
            }

            if (auto owner = Owner_.Lock()) {
                return owner->Track(std::move(reference), Category_, keepHolder);
            }

            return reference;
        }

    private:
        const TWeakPtr<TNodeMemoryReferenceTracker> Owner_;
        const EMemoryCategory Category_;
    };

    class TTrackedReferenceHolder
        : public TSharedRangeHolder
    {
    public:
        TTrackedReferenceHolder(
            TNodeMemoryReferenceTrackerPtr tracker,
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
        const TNodeMemoryReferenceTrackerPtr Tracker_;
        const TSharedRef Underlying_;
        const EMemoryCategory Category_;
    };

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

    const INodeMemoryTrackerPtr MemoryTracker_;

    std::array<TReferenceAddressMapShard, ReferenceAddressMapShardCount> ReferenceAddressToState_;

    TEnumIndexedArray<EMemoryCategory, IMemoryReferenceTrackerPtr> CategoryTrackers_;

    std::atomic<bool> Enabled_ = true;

    void InitCategoryTrackers()
    {
        for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
            CategoryTrackers_[category] = New<TMemoryReferenceTracker>(this, category);
        }
    }

    TReferenceKey GetReferenceKey(TRef ref)
    {
        YT_VERIFY(ref);
        return TReferenceKey(reinterpret_cast<uintptr_t>(ref.Begin()), ref.Size());
    }

    TReferenceAddressMapShard& GetReferenceAddressMapShard(TReferenceKey key)
    {
        return ReferenceAddressToState_[(key.first >> ReferenceAddressExpectedAlignmentLog) % ReferenceAddressMapShardCount];
    }

    TSharedRef Track(TSharedRef reference, EMemoryCategory category, bool keepExistingTracking)
    {
        if (!Enabled_.load()) {
            return reference;
        }

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

        CreateStateOrIncrementUsageCounter(rawReference, category);

        auto underlyingHolder = holder->Clone({.KeepMemoryReferenceTracking = keepExistingTracking});
        auto underlyingReference = TSharedRef(rawReference, std::move(underlyingHolder));
        return TSharedRef(
            rawReference,
            New<TTrackedReferenceHolder>(this, std::move(underlyingReference), category));
    }

    void CreateStateOrIncrementUsageCounter(TRef rawReference, EMemoryCategory category)
    {
        auto key = GetReferenceKey(rawReference);
        auto& shard = GetReferenceAddressMapShard(key);

        auto guard = Guard(shard.SpinLock);

        if (auto it = shard.Map.find(key); it != shard.Map.end()) {
            ChangeCategoryUsage(&it->second, category, 1);
            return;
        }

        auto it = EmplaceOrCrash(shard.Map, key, TState{.Reference = rawReference});
        ChangeCategoryUsage(&it->second, category, 1);
    }

    void RemoveStateOrDecreaseUsageConter(TRef rawReference, EMemoryCategory category)
    {
        auto key = GetReferenceKey(rawReference);
        auto& shard = GetReferenceAddressMapShard(key);
        auto guard = Guard(shard.SpinLock);

        auto it = GetIteratorOrCrash(shard.Map, key);
        auto& state = it->second;

        ChangeCategoryUsage(&state, category, -1);
        if (state.CategoryToUsage.empty()) {
            shard.Map.erase(it);
        }
    }

    void ChangeCategoryUsage(TState* state, EMemoryCategory category, i64 delta)
    {
        auto oldCategory = GetCategoryByUsage(state->CategoryToUsage);

        if (state->CategoryToUsage.contains(category) &&
            state->CategoryToUsage[category] + delta != 0)
        {
            state->CategoryToUsage[category] += delta;
            return;
        }

        state->CategoryToUsage[category] += delta;

        if (state->CategoryToUsage[category] == 0) {
            state->CategoryToUsage.erase(category);
        }

        auto newCategory = GetCategoryByUsage(state->CategoryToUsage);
        if (!newCategory) {
            state->MemoryGuard.Release();
            return;
        }

        if ((oldCategory && newCategory && *oldCategory != *newCategory) || !oldCategory) {
            state->MemoryGuard = TMemoryUsageTrackerGuard::Acquire(
                MemoryTracker_->WithCategory(*newCategory),
                static_cast<i64>(state->Reference.Size()));
        }
    }

    static std::optional<EMemoryCategory> GetCategoryByUsage(const THashMap<EMemoryCategory, i64>& usage)
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
};

DEFINE_REFCOUNTED_TYPE(TNodeMemoryReferenceTracker)

////////////////////////////////////////////////////////////////////////////////

TDelayedReferenceHolder::TDelayedReferenceHolder(
    TSharedRef underlying,
    TDuration delayBeforeFree,
    IInvokerPtr dtorInvoker)
    : Underlying_(std::move(underlying))
    , DelayBeforeFree_(delayBeforeFree)
    , DtorInvoker_(std::move(dtorInvoker))
{ }

TSharedRangeHolderPtr TDelayedReferenceHolder::Clone(const TSharedRangeHolderCloneOptions& options)
{
    if (options.KeepMemoryReferenceTracking) {
        return this;
    }
    return Underlying_.GetHolder()->Clone(options);
}

std::optional<size_t> TDelayedReferenceHolder::GetTotalByteSize() const
{
    return Underlying_.GetHolder()->GetTotalByteSize();
}

TDelayedReferenceHolder::~TDelayedReferenceHolder()
{
    NConcurrency::TDelayedExecutor::Submit(
        BIND([] (TSharedRef reference) {
            reference.ReleaseHolder();
        }, Passed(std::move(Underlying_))),
        DelayBeforeFree_,
        DtorInvoker_);
}

////////////////////////////////////////////////////////////////////////////////

INodeMemoryReferenceTrackerPtr CreateNodeMemoryReferenceTracker(INodeMemoryTrackerPtr tracker)
{
    return New<TNodeMemoryReferenceTracker>(std::move(tracker));
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef TrackMemory(
    const INodeMemoryReferenceTrackerPtr& tracker,
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
    const INodeMemoryReferenceTrackerPtr& tracker,
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

IMemoryReferenceTrackerPtr WithCategory(const INodeMemoryReferenceTrackerPtr& tracker, EMemoryCategory category)
{
    if (!tracker) {
        return {};
    }

    return tracker->WithCategory(category);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
