#include "block_map.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/memory/new.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/error/error.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <atomic>
#include <deque>
#include <functional>
#include <optional>
#include <vector>

namespace NYT::NNbd::NJournal {

using namespace NMappedBlockIdLayout;

////////////////////////////////////////////////////////////////////////////////

bool IsStoredMappedBlockId(TMappedBlockId id)
{
    return ((id.Underlying() & TagMask) >> PayloadBits) == StoredTag;
}

TStoredBlockId ToStoredBlockId(TMappedBlockId id)
{
    YT_ASSERT(IsStoredMappedBlockId(id));
    return TStoredBlockId(id.Underlying() & PayloadMask);
}

TMappedBlockId ToMappedBlockId(TStoredBlockId id)
{
    YT_VERIFY((id.Underlying() & ~PayloadMask) == 0);
    return TMappedBlockId((StoredTag << PayloadBits) | id.Underlying());
}

bool IsDirtyMappedBlockId(TMappedBlockId id)
{
    return ((id.Underlying() & TagMask) >> PayloadBits) == DirtyTag;
}

TDirtyBlockId ToDirtyBlockId(TMappedBlockId id)
{
    YT_ASSERT(IsDirtyMappedBlockId(id));
    return TDirtyBlockId(id.Underlying() & PayloadMask);
}

TMappedBlockId ToMappedBlockId(TDirtyBlockId id)
{
    YT_VERIFY((id.Underlying() & ~PayloadMask) == 0);
    return TMappedBlockId((DirtyTag << PayloadBits) | id.Underlying());
}

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsCoW(TMappedBlockId id)
{
    return (id.Underlying() & CoWMask) != 0;
}

TMappedBlockId WithCoW(TMappedBlockId id)
{
    return TMappedBlockId(id.Underlying() | CoWMask);
}

TMappedBlockId WithoutCoW(TMappedBlockId id)
{
    return TMappedBlockId(id.Underlying() & ~CoWMask);
}

////////////////////////////////////////////////////////////////////////////////

//! Maps each block index to its mapped block id, kept in a vector of atomic slots (see
//! TMappedBlockId); a zero-initialized slot is empty.
class TBlockMap
    : public IBlockMap
{
public:
    explicit TBlockMap(int blockCount)
        : Slots_(blockCount)
    {
        YT_VERIFY(blockCount >= 0);
    }

    TMappedBlockId FindBlock(int blockIndex) override
    {
        auto& slot = GetSlot(blockIndex);
        return WithoutCoW(TMappedBlockId(slot.load(std::memory_order::acquire)));
    }

    void PutDirty(int blockIndex, TDirtyBlockId blockId) override
    {
        std::optional<TStoredBlockId> diedStoredBlockId;
        {
            auto guard = Guard(WriteLock_);

            auto& slot = GetSlot(blockIndex);
            auto oldId = TMappedBlockId(slot.load(std::memory_order::acquire));

            // The first write to a block makes it non-empty for good.
            if (oldId == EmptyMappedBlockId) {
                UsedBlockCount_.fetch_add(1, std::memory_order::relaxed);
            }

            auto bareOldId = WithoutCoW(oldId);
            if (IsStoredMappedBlockId(bareOldId)) {
                diedStoredBlockId = ToStoredBlockId(bareOldId);
            }

            auto newId = ToMappedBlockId(blockId);
            // While a snapshot scans, stash the pre-snapshot value the first time we overwrite a block
            // (the CoW bit means "already stashed") so TakeSnapshot can restore it.
            if (SnapshotState_ == ESnapshotState::CoWActive) {
                if (!IsCoW(oldId)) {
                    CoWBlocks_.emplace_back(blockIndex, oldId);
                }
                newId = WithCoW(newId);
            }
            slot.store(newId.Underlying(), std::memory_order::release);
        }

        // Fire outside WriteLock_ (subscribers may re-enter the map), strictly after the slot update.
        if (diedStoredBlockId) {
            StoredBlockDied_.Fire(*diedStoredBlockId);
        }
    }

    bool TryMakeClean(int blockIndex, TDirtyBlockId expectedBlockId, TStoredBlockId storedBlockId) override
    {
        auto adopted = TryAdoptStoredBlock(blockIndex, expectedBlockId, storedBlockId);

        // Fire outside WriteLock_ (subscribers may re-enter the map) but strictly after the slot
        // update, and regardless of whether the flush was adopted: a snapshot armed under the same
        // lock either sees the update in its scan or is subscribed by now, so it cannot miss this.
        BlockFlushObserved_.Fire(expectedBlockId, storedBlockId);

        // A flush that lost the last-write-wins race is never referenced, so its stored block is dead
        // on arrival.
        if (!adopted) {
            StoredBlockDied_.Fire(storedBlockId);
        }

        return adopted;
    }

    int GetUsedBlockCount() const override
    {
        return UsedBlockCount_.load(std::memory_order::acquire);
    }

    //! Takes a point-in-time cut without holding the lock across the whole scan, via copy-on-write:
    //! flip to CoWActive, scan the slots lock-free, and let concurrent writers stash the pre-snapshot
    //! value of each block they first touch. The scan may catch post-flip values; a cleanup pass then
    //! restores the stashed originals, yielding the map exactly as of the flip.
    TBlockMapSnapshot TakeSnapshot(std::function<void(int)> onScanned) override
    {
        // Arm copy-on-write. Only one snapshot at a time (the CoW bit and CoWBlocks_ are single-writer).
        {
            auto guard = Guard(WriteLock_);

            if (SnapshotState_ != ESnapshotState::None) {
                THROW_ERROR_EXCEPTION("Another snapshot is already in progress");
            }

            SnapshotState_ = ESnapshotState::CoWActive;
            YT_VERIFY(CoWBlocks_.empty());
        }

        auto disarmGuard = Finally([&] {
            auto guard = Guard(WriteLock_);

            SnapshotState_ = ESnapshotState::None;
            CoWBlocks_.clear();
        });

        // Scan every slot lock-free, recording its current value and where it landed in the result.
        TBlockMapSnapshot snapshot;
        snapshot.Blocks.reserve(GetUsedBlockCount());
        absl::flat_hash_map<int, int> blockIndexToPosition;
        blockIndexToPosition.reserve(snapshot.Blocks.capacity());
        for (int index = 0; index < std::ssize(Slots_); ++index) {
            if (onScanned) {
                onScanned(index);
            }
            auto& slot = GetSlot(index);
            auto id = TMappedBlockId(slot.load(std::memory_order::acquire));
            if (id == EmptyMappedBlockId) {
                continue;
            }
            EmplaceOrCrash(blockIndexToPosition, index, std::ssize(snapshot.Blocks));
            snapshot.Blocks.emplace_back(index, WithoutCoW(id));
        }

        // Disarm: past this barrier no writer stashes anything, so CoWBlocks_ is complete and stable.
        {
            auto guard = Guard(WriteLock_);

            YT_VERIFY(SnapshotState_ == ESnapshotState::CoWActive);
            SnapshotState_ = ESnapshotState::CoWCleanup;
        }

        // Restore each stashed block to its pre-flip value and clear its CoW bit (so the next snapshot
        // starts clean). A block absent from the scan was empty at the flip; its stash is Empty.
        for (auto [index, id] : CoWBlocks_) {
            auto& slot = GetSlot(index);
            slot.fetch_and(~CoWMask, std::memory_order::release);
            if (auto it = blockIndexToPosition.find(index); it != blockIndexToPosition.end()) {
                snapshot.Blocks[it->second].second = id;
            }
        }

        // Drop blocks that were empty at the flip (first written mid-scan).
        EraseIf(
            snapshot.Blocks,
            [] (const auto& indexAndId) { return indexAndId.second == EmptyMappedBlockId; });

        return snapshot;
    }

    void LoadSnapshot(const TBlockMapSnapshot& snapshot) override
    {
        YT_VERIFY(GetUsedBlockCount() == 0);
        UsedBlockCount_.store(std::ssize(snapshot.Blocks), std::memory_order::release);

        for (auto [index, id] : snapshot.Blocks) {
            YT_VERIFY(IsStoredMappedBlockId(id) && !IsCoW(id));
            auto& slot = GetSlot(index);
            YT_VERIFY(TMappedBlockId(slot.load(std::memory_order::acquire)) == EmptyMappedBlockId);
            slot.store(id.Underlying(), std::memory_order::release);
        }
    }

    DEFINE_SIGNAL_OVERRIDE(void(TDirtyBlockId dirtyBlockId, TStoredBlockId storedBlockId), BlockFlushObserved);
    DEFINE_SIGNAL_OVERRIDE(void(TStoredBlockId storedBlockId), StoredBlockDied);

private:
    std::vector<std::atomic<TMappedBlockId::TUnderlying>> Slots_;
    std::atomic<int> UsedBlockCount_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, WriteLock_);

    enum class ESnapshotState
    {
        None,       // no snapshot running
        CoWActive,  // scanning; writers stash pre-snapshot values
        CoWCleanup, // scan done; restoring stashed values, clearing CoW bits
    };

    ESnapshotState SnapshotState_ = ESnapshotState::None;

    // A deque (not a vector) so appends under WriteLock_ never trigger an O(n) reallocation-and-copy.
    std::deque<std::pair<int, TMappedBlockId>> CoWBlocks_;

    std::atomic<TMappedBlockId::TUnderlying>& GetSlot(int blockIndex)
    {
        YT_ASSERT(0 <= blockIndex && blockIndex < std::ssize(Slots_));
        return Slots_[blockIndex];
    }

    //! The locked half of #TryMakeClean; returns whether the flush was adopted.
    bool TryAdoptStoredBlock(int blockIndex, TDirtyBlockId expectedBlockId, TStoredBlockId storedBlockId)
    {
        auto guard = Guard(WriteLock_);

        auto& slot = GetSlot(blockIndex);
        auto oldId = TMappedBlockId(slot.load(std::memory_order::acquire));
        if (WithoutCoW(oldId) != ToMappedBlockId(expectedBlockId)) {
            return false;
        }

        auto newId = ToMappedBlockId(storedBlockId);
        // Stash the pre-snapshot value on first overwrite during a scan; see #PutDirty.
        if (SnapshotState_ == ESnapshotState::CoWActive) {
            if (!IsCoW(oldId)) {
                CoWBlocks_.emplace_back(blockIndex, oldId);
            }
            newId = WithCoW(newId);
        }
        slot.store(newId.Underlying(), std::memory_order::release);

        return true;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockMapPtr CreateBlockMap(int blockCount)
{
    return New<TBlockMap>(blockCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
