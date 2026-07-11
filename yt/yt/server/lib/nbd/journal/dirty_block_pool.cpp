#include "dirty_block_pool.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/assert/assert.h>

#include <deque>

namespace NYT::NNbd::NJournal {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TDirtyBlock::TDirtyBlock(int blockIndex, TSharedRef payload)
    : BlockIndex(blockIndex)
    , Payload(std::move(payload))
{ }

////////////////////////////////////////////////////////////////////////////////

class TDirtyBlockPool
    : public IDirtyBlockPool
{
public:
    explicit TDirtyBlockPool(int capacityCount)
        : CapacityCount_(capacityCount)
        , BlockRing_(capacityCount)
    {
        YT_VERIFY(capacityCount > 0);
    }

    int GetCapacity() const override
    {
        return CapacityCount_;
    }

    int GetSize() const override
    {
        auto guard = Guard(Lock_);
        return GetSizeLocked();
    }

    TFuture<std::vector<TDirtyBlockId>> Put(TRange<TDirtyBlockPtr> blocks) override
    {
        if (blocks.empty()) {
            return MakeFuture(std::vector<TDirtyBlockId>{});
        }

        auto guard = Guard(Lock_);

        // Accept as many blocks as currently fit (the caller resubmits the rest). Only when
        // nothing fits do we wait for space; puts already waiting keep their place in line.
        if (PendingPuts_.empty()) {
            int count = Min<int>(GetFreeCountLocked(), std::ssize(blocks));
            if (count > 0) {
                return MakeFuture(PutLocked(blocks.Slice(0, count)));
            }
        }

        PendingPuts_.push_back({
            .Blocks = std::vector(blocks.begin(), blocks.end()),
        });
        return PendingPuts_.back().Promise.ToFuture();
    }

    TDirtyBlockPtr Find(TDirtyBlockId blockId, int blockIndex) override
    {
        // Lock-free: load the slot and check whether it still holds the requested block.
        auto block = BlockRing_[GetSlot(blockId.Underlying())].Acquire();
        return block && block->BlockIndex == blockIndex
            ? block
            : nullptr;
    }

    TBeginDrainResult BeginDrain(int maxBlockCount) override
    {
        auto guard = Guard(Lock_);
        // Hand out the oldest blocks without removing them; the paired EndDrain evicts them. Only
        // one drain is outstanding at a time, so this always returns the current head range.
        int blockCount = Min(maxBlockCount, GetSizeLocked());
        TBeginDrainResult result;
        result.reserve(blockCount);
        for (int index = 0; index < blockCount; ++index) {
            result.push_back(BlockRing_[GetSlot(HeadIndex_ + index)].Acquire());
        }
        return result;
    }

    void EndDrain(const TBeginDrainResult& result) override
    {
        std::vector<TPendingPut> readyPuts;
        {
            auto guard = Guard(Lock_);

            int drainedBlockCount = std::ssize(result);
            YT_VERIFY(drainedBlockCount <= GetSizeLocked());

            // Evict the drained head blocks and advance the head, freeing their slots.
            for (int index = 0; index < drainedBlockCount; ++index) {
                BlockRing_[GetSlot(HeadIndex_ + index)].Reset();
            }
            HeadIndex_ += drainedBlockCount;

            // The freed space unblocks waiting puts, oldest first; each accepts as much as
            // fits (possibly a prefix) and resolves.
            while (!PendingPuts_.empty() && GetFreeCountLocked() > 0) {
                auto pendingPut = std::move(PendingPuts_.front());
                PendingPuts_.pop_front();
                int blockCount = Min<int>(GetFreeCountLocked(), std::ssize(pendingPut.Blocks));
                pendingPut.BlockIds = PutLocked(TRange(pendingPut.Blocks).Slice(0, blockCount));
                readyPuts.push_back(std::move(pendingPut));
            }
        }

        // Fulfill the unblocked puts outside the lock.
        for (auto& pendingPut : readyPuts) {
            pendingPut.Promise.Set(std::move(pendingPut.BlockIds));
        }
    }

private:
    const int CapacityCount_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);

    // Absolute positions of the dirty window [HeadIndex_, TailIndex_); the dirty count is
    // TailIndex_ - HeadIndex_ and never exceeds CapacityCount_.
    i64 HeadIndex_ = 0;
    i64 TailIndex_ = 0;

    std::vector<TAtomicIntrusivePtr<TDirtyBlock>> BlockRing_;

    struct TPendingPut
    {
        std::vector<TDirtyBlockPtr> Blocks;
        std::vector<TDirtyBlockId> BlockIds;
        const TPromise<std::vector<TDirtyBlockId>> Promise =  NewPromise<std::vector<TDirtyBlockId>>();
    };

    std::deque<TPendingPut> PendingPuts_;

    int GetSizeLocked() const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);
        return static_cast<int>(TailIndex_ - HeadIndex_);
    }

    int GetFreeCountLocked() const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);
        return CapacityCount_ - GetSizeLocked();
    }

    int GetSlot(i64 position) const
    {
        return static_cast<int>(position % CapacityCount_);
    }

    //! Places |blocks| at the tail, assigning them consecutive ids. Assumes there is room and
    //! that Lock_ is held.
    std::vector<TDirtyBlockId> PutLocked(TRange<TDirtyBlockPtr> blocks)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);
        std::vector<TDirtyBlockId> blockIds;
        blockIds.reserve(blocks.size());
        for (const auto& block : blocks) {
            block->BlockId = TDirtyBlockId(TailIndex_);
            BlockRing_[GetSlot(TailIndex_)].Store(block);
            blockIds.push_back(block->BlockId);
            ++TailIndex_;
        }
        return blockIds;
    }
};

////////////////////////////////////////////////////////////////////////////////

IDirtyBlockPoolPtr CreateDirtyBlockPool(int capacityCount)
{
    return New<TDirtyBlockPool>(capacityCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
