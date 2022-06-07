#include "block_tracker.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/chunk_client/block.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <library/cpp/yt/threading/public.h>

#include <unordered_map>
#include <utility>

namespace NYT {

using NChunkClient::TBlock;
using NNodeTrackerClient::EMemoryCategory;

/////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using TBlockId = uintptr_t;

TBlockId GetBlockId(TRef ref)
{
    YT_VERIFY(ref);
    return TBlockId(ref.Begin());
}

TBlockId GetBlockId(const TBlock &block)
{
    return GetBlockId(block.Data);
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

    NChunkClient::TBlock RegisterBlock(NChunkClient::TBlock block) override
    {
        if (!block.Data) {
            return block;
        }

        auto blockId = NDetail::GetBlockId(block);
        auto blockRef = TRef(block.Data);

        TSharedRef oldBlock;

        {
            // Fast path.

            auto guard = Guard(SpinLock_);

            if (BlockIdToState_.contains(blockId)) {
                auto holder = BlockIdToState_[blockId].Holder.Lock();
                if (holder) {
                    // Old block.Data should be destructed outside of critical section
                    oldBlock = std::move(block.Data);
                    block.Data = TSharedRef(static_cast<TRef>(oldBlock), std::move(holder));

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

            if (BlockIdToState_.contains(blockId)) {
                auto holder = BlockIdToState_[blockId].Holder.Lock();

                // Old block.Data should be destructed outside of critical section
                oldBlock = std::move(block.Data);

                if (holder) {
                    block.Data = TSharedRef(static_cast<TRef>(oldBlock), std::move(holder));
                } else {
                    holder = New<NDetail::TBlockHolder>(this, blockRef);
                    BlockIdToState_[blockId].Holder = holder;
                    block.Data = TSharedRef(blockRef, std::move(holder));
                }
            } else {
                auto& blockState = BlockIdToState_[blockId];

                blockState.Data = std::move(block.Data);
                blockState.MemoryGuard = std::move(memoryGuard);

                auto holder = New<NDetail::TBlockHolder>(this, blockRef);
                blockState.Holder = holder;
                block.Data = TSharedRef(blockRef, std::move(holder));
            }
        }

        return block;
    }

    void AcquireCategory(TRef ref, NNodeTrackerClient::EMemoryCategory category) override
    {
        DoChangeCategoryUsage(ref, category, 1);
    }

    void ReleaseCategory(TRef ref, NNodeTrackerClient::EMemoryCategory category) override
    {
        DoChangeCategoryUsage(ref, category, -1);
    }

    void OnUnregisterBlock(TRef ref) override
    {
        YT_VERIFY(ref);

        auto blockId = NDetail::GetBlockId(ref);

        auto guard = Guard(SpinLock_);
        if (!BlockIdToState_.contains(blockId)) {
            return;
        }

        if (BlockIdToState_[blockId].Holder.IsExpired()) {
            BlockIdToState_.erase(blockId);
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

    THashMap<NDetail::TBlockId, TBlockState> BlockIdToState_;
    const INodeMemoryTrackerPtr MemoryTracker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    void DoChangeCategoryUsage(TRef ref, NNodeTrackerClient::EMemoryCategory category, i64 delta)
    {
        if (!ref) {
            return;
        }

        auto blockId = NDetail::GetBlockId(ref);

        auto guard = Guard(SpinLock_);

        YT_ASSERT(BlockIdToState_.contains(blockId));
        auto& state = BlockIdToState_[blockId];

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

/////////////////////////////////////////////////////////////////////////////

namespace NDetail {

class TCategoryUsageGuard
    : public TRefCounted
    , public TNonCopyable
{
public:
    explicit TCategoryUsageGuard(
        IBlockTrackerPtr tracker,
        TSharedRef holder,
        NNodeTrackerClient::EMemoryCategory category)
        : Tracker_(std::move(tracker))
        , Holder_(std::move(holder))
        , Category_(category)
    {
        YT_VERIFY(Tracker_);
        YT_VERIFY(Holder_);

        Tracker_->AcquireCategory(Holder_, Category_);
    }

    ~TCategoryUsageGuard() override
    {
        Tracker_->ReleaseCategory(Holder_, Category_);
    }

private:
    const IBlockTrackerPtr Tracker_;
    const TSharedRef Holder_;
    const NNodeTrackerClient::EMemoryCategory Category_;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TBlock ResetCategory(
    NChunkClient::TBlock block,
    IBlockTrackerPtr tracker,
    std::optional<NNodeTrackerClient::EMemoryCategory> category)
{
    if (!tracker) {
        return block;
    }

    if (!block.Data) {
        return block;
    }

    block = tracker->RegisterBlock(std::move(block));

    if (category) {
        TRef ref = block.Data;
        block.Data = TSharedRef(
            ref,
            New<NDetail::TCategoryUsageGuard>(
                std::move(tracker),
                std::move(block.Data),
                *category));
    }

    return block;
}

TSharedRef ResetCategory(
    TSharedRef block,
    IBlockTrackerPtr tracker,
    std::optional<NNodeTrackerClient::EMemoryCategory> category)
{
    return ResetCategory(
        TBlock(std::move(block)),
        std::move(tracker),
        category)
        .Data;
}

/////////////////////////////////////////////////////////////////////////////

NChunkClient::TBlock AttachCategory(
    NChunkClient::TBlock block,
    IBlockTrackerPtr tracker,
    std::optional<NNodeTrackerClient::EMemoryCategory> category)
{
    block.Data = AttachCategory(std::move(block.Data), std::move(tracker), category);

    return block;
}

TSharedRef AttachCategory(
    TSharedRef block,
    IBlockTrackerPtr tracker,
    std::optional<NNodeTrackerClient::EMemoryCategory> category)
{
    TSharedRef blockWithCategory = ResetCategory(block, std::move(tracker), category);

    TRef ref = block;
    return TSharedRef(
        ref,
        MakeCompositeHolder(
            std::move(block),
            std::move(blockWithCategory)));
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
