#include "block_tracker.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/chunk_client/block.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <library/cpp/yt/threading/public.h>

#include <unordered_map>
#include <utility>

namespace NYT {

using NChunkClient::TBlock;

/////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using TBlockId = uintptr_t;

TBlockId GetBlockId(TRef ref)
{
    YT_VERIFY(ref);
    return TBlockId(ref.Begin());
}

} // namespace NDetail

/////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! Calls IBlockTracker::OnUnregisterBlock on destruction
class TBlockHolder
    : public TRefCounted
{
public:
    TBlockHolder(IBlockTrackerPtr tracker, TRef block)
        : Tracker_(std::move(tracker))
        , Block_(block)
    { }

    ~TBlockHolder() override
    {
        Tracker_->OnUnregisterBlock(Block_);
    }

private:
    const IBlockTrackerPtr Tracker_;
    TRef Block_;
};

} // namespace NDetail

class TBlockTracker
    : public IBlockTracker
{
public:
    explicit TBlockTracker(INodeMemoryTrackerPtr memoryTracker)
        : MemoryTracker_(std::move(memoryTracker))
    { }

    TSharedRef RegisterBlock(TSharedRef block) override
    {
        if (!block) {
            return block;
        }

        auto blockId = NDetail::GetBlockId(block);
        auto blockRef = TRef(block);

        TSharedRef oldBlock;

        {
            // Fast path.

            auto guard = Guard(SpinLock_);

            auto it = BlockIdToState_.find(blockId);
            if (it != BlockIdToState_.end()) {
                auto holder = it->second.Holder.Lock();
                if (holder) {
                    // Old block.Data should be destructed outside of critical section.
                    oldBlock = std::move(block);
                    block = TSharedRef(static_cast<TRef>(oldBlock), std::move(holder));

                    guard.Release();
                    return block;
                }
            }
        }

        auto memoryGuard = TMemoryUsageTrackerGuard::Acquire(
            MemoryTracker_->WithCategory(EMemoryCategory::UnknownBlocks),
            static_cast<i64>(block.Size()));

        {
            // Slow path.

            auto guard = Guard(SpinLock_);

            auto it = BlockIdToState_.find(blockId);
            if (it != BlockIdToState_.end()) {
                auto& blockState = it->second;
                auto holder = blockState.Holder.Lock();

                // Old block.Data should be destructed outside of critical section.
                oldBlock = std::move(block);

                if (holder) {
                    block = TSharedRef(static_cast<TRef>(oldBlock), std::move(holder));
                } else {
                    holder = New<NDetail::TBlockHolder>(this, blockRef);
                    blockState.Holder = holder;
                    block = TSharedRef(blockRef, std::move(holder));
                }
            } else {
                auto& blockState = BlockIdToState_[blockId];

                blockState.Data = std::move(block);
                blockState.MemoryGuard = std::move(memoryGuard);

                auto holder = New<NDetail::TBlockHolder>(this, blockRef);
                blockState.Holder = holder;
                block = TSharedRef(blockRef, std::move(holder));
            }
        }

        return block;
    }

    void AcquireCategory(TRef ref, EMemoryCategory category) override
    {
        DoChangeCategoryUsage(ref, category, 1);
    }

    void ReleaseCategory(TRef ref, EMemoryCategory category) override
    {
        DoChangeCategoryUsage(ref, category, -1);
    }

    void OnUnregisterBlock(TRef ref) override
    {
        YT_VERIFY(ref);

        auto blockId = NDetail::GetBlockId(ref);

        auto guard = Guard(SpinLock_);

        auto it = BlockIdToState_.find(blockId);

        if (it == BlockIdToState_.end()) {
            return;
        }

        if (it->second.Holder.IsExpired()) {
            BlockIdToState_.erase(it);
        }
    }

private:
    struct TBlockState
    {
        TWeakPtr<TRefCounted> Holder;
        TSharedRef Data;
        THashMap<EMemoryCategory, i64> CategoryToUsage;
        TMemoryUsageTrackerGuard MemoryGuard;
    };

    const INodeMemoryTrackerPtr MemoryTracker_;
    THashMap<NDetail::TBlockId, TBlockState> BlockIdToState_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    void DoChangeCategoryUsage(TRef ref, EMemoryCategory category, i64 delta)
    {
        if (!ref) {
            return;
        }

        auto blockId = NDetail::GetBlockId(ref);

        auto guard = Guard(SpinLock_);

        auto& state = GetOrCrash(BlockIdToState_, blockId);
        auto oldBlockCategory = GetCategoryByUsage(state.CategoryToUsage);

        if (state.CategoryToUsage.contains(category) &&
            state.CategoryToUsage[category] + delta != 0)
        {
            state.CategoryToUsage[category] += delta;
            return;
        }

        auto& categoryUsage = state.CategoryToUsage[category];
        categoryUsage += delta;

        if (categoryUsage == 0) {
            state.CategoryToUsage.erase(category);
        }

        auto newBlockCategory = GetCategoryByUsage(state.CategoryToUsage);
        if (oldBlockCategory != newBlockCategory) {
            state.MemoryGuard = TMemoryUsageTrackerGuard::Acquire(
                MemoryTracker_->WithCategory(newBlockCategory),
                static_cast<i64>(ref.Size()));
        }
    }

    static EMemoryCategory GetCategoryByUsage(const THashMap<EMemoryCategory, i64>& usage)
    {
        if (usage.empty()) {
            return EMemoryCategory::UnknownBlocks;
        }
        if (usage.size() == 1) {
            return usage.begin()->first;
        }

        if (usage.contains(EMemoryCategory::BlockCache)) {
            if (usage.size() == 2) {
                for (auto& [category, _]: usage) {
                    if (category != EMemoryCategory::BlockCache) {
                        return category;
                    }
                }
            } else {
                return EMemoryCategory::MixedBlocks;
            }
        }

        return EMemoryCategory::MixedBlocks;
    }
};

IBlockTrackerPtr CreateBlockTracker(INodeMemoryTrackerPtr tracker)
{
    YT_VERIFY(tracker);
    return New<TBlockTracker>(std::move(tracker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
