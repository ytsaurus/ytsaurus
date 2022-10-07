#include "memory_reference_tracker.h"

#include "config.h"

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/misc/memory_reference_tracker.h>

#include <library/cpp/yt/threading/public.h>

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

constexpr int ReferenceAddressMapShardCount = 256;

/////////////////////////////////////////////////////////////////////////////

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
            TIntrusivePtr<TNodeMemoryReferenceTracker> owner,
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

    class TReferenceHolder
        : public TSharedRangeHolder
    {
    public:
        TReferenceHolder(TIntrusivePtr<TNodeMemoryReferenceTracker> tracker, TRef rawReference, EMemoryCategory category)
            : Tracker_(std::move(tracker))
            , RawReference_(rawReference)
            , Category_(category)
        { }

        ~TReferenceHolder() override
        {
            Tracker_->RemoveStateOrDecreaseUsageConter(RawReference_, Category_);
        }

        TSharedRangeHolderPtr Clone(const TSharedRangeHolderCloneOptions& options) override
        {
            if (options.KeepMemoryReferenceTracking) {
                return this;
            }
            return nullptr;
        }

    private:
        const TIntrusivePtr<TNodeMemoryReferenceTracker> Tracker_;
        const TRef RawReference_;
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

    TEnumIndexedVector<EMemoryCategory, IMemoryReferenceTrackerPtr> CategoryTrackers_;

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
        return ReferenceAddressToState_[key.first % ReferenceAddressMapShardCount];
    }

    TSharedRef Track(TSharedRef reference, EMemoryCategory category, bool keepHolder)
    {
        if (!Enabled_.load()) {
            return reference;
        }

        auto rawReference = TRef(reference);
        const auto& holder = reference.GetHolder();
        auto options = TSharedRangeHolderCloneOptions{
            .KeepMemoryReferenceTracking = keepHolder
        };

        CreateStateOrIncrementUsageCounter(rawReference, category);

        return TSharedRef(rawReference, MakeCompositeSharedRangeHolder({
            New<TReferenceHolder>(this, rawReference, category),
            holder->Clone(options)
        }));
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
            if (state->MemoryGuard) {
                state->MemoryGuard.Release();
            }
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

////////////////////////////////////////////////////////////////////////////////

INodeMemoryReferenceTrackerPtr CreateNodeMemoryReferenceTracker(INodeMemoryTrackerPtr tracker)
{
    return New<TNodeMemoryReferenceTracker>(std::move(tracker));
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef TrackMemoryReference(
    const INodeMemoryReferenceTrackerPtr& tracker,
    EMemoryCategory category,
    TSharedRef reference,
    bool keepHolder)
{
    return tracker
        ? tracker->WithCategory(category)->Track(reference, keepHolder)
        : reference;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
