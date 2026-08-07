#include "block_map.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/memory/new.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/error/error.h>

#include <algorithm>
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

    TMappedBlockId FindBlock(int blockIndex) final
    {
        auto& slot = GetSlot(blockIndex);
        return WithoutCoW(TMappedBlockId(slot.load(std::memory_order::acquire)));
    }

    void PutBlock(int blockIndex, TDirtyBlockId blockId) final
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
            StoredBlockUnreferenced_.Fire(*diedStoredBlockId);
        }
    }

    bool TryPutBlock(int blockIndex, TMappedBlockId expectedBlockId, TStoredBlockId storedBlockId) final
    {
        bool succeeded;
        {
            auto guard = Guard(WriteLock_);

            auto& slot = GetSlot(blockIndex);
            auto oldId = TMappedBlockId(slot.load(std::memory_order::acquire));
            succeeded = WithoutCoW(oldId) == expectedBlockId;
            if (succeeded) {
                auto newId = ToMappedBlockId(storedBlockId);
                // Stash the pre-snapshot value on first overwrite during a scan; see #PutBlock.
                if (SnapshotState_ == ESnapshotState::CoWActive) {
                    if (!IsCoW(oldId)) {
                        CoWBlocks_.emplace_back(blockIndex, oldId);
                    }
                    newId = WithCoW(newId);
                }
                slot.store(newId.Underlying(), std::memory_order::release);
            }
        }

        // Fire outside WriteLock_ (subscribers may re-enter the map), strictly after the slot update. A
        // dirty expected id means this is a flush: report where it landed, succeeded or not, so a snapshot
        // armed under the same lock cannot miss it.
        if (IsDirtyMappedBlockId(expectedBlockId)) {
            BlockFlushObserved_.Fire(ToDirtyBlockId(expectedBlockId), storedBlockId);
        }

        if (succeeded) {
            // A superseded stored id is now unreferenced; a dirty one was never a stored block.
            if (IsStoredMappedBlockId(expectedBlockId)) {
                StoredBlockUnreferenced_.Fire(ToStoredBlockId(expectedBlockId));
            }
        } else {
            StoredBlockUnreferenced_.Fire(storedBlockId);
        }

        return succeeded;
    }

    int GetUsedBlockCount() const final
    {
        return UsedBlockCount_.load(std::memory_order::acquire);
    }

    //! Takes a point-in-time cut without holding the lock across the whole scan, via copy-on-write:
    //! flip to CoWActive, scan the slots lock-free, and let concurrent writers stash the pre-snapshot
    //! value of each block they first touch. The scan may catch post-flip values; a cleanup pass then
    //! restores the stashed originals, yielding the map exactly as of the flip.
    TBlockMapSnapshot TakeSnapshot(const std::function<void(int)>& onScanned) final
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

        // Scan every slot lock-free, recording its current value.
        TBlockMapSnapshot snapshot;
        snapshot.Blocks.reserve(GetUsedBlockCount());
        for (int index = 0; index < std::ssize(Slots_); ++index) {
            if (onScanned) {
                onScanned(index);
            }
            auto& slot = GetSlot(index);
            auto id = TMappedBlockId(slot.load(std::memory_order::acquire));
            if (id == EmptyMappedBlockId) {
                continue;
            }
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
            // The scan runs in ascending index order, so Blocks is sorted by index. Stashes are bounded
            // by the writes racing the scan, so the searches stay cheap.
            auto it = std::lower_bound(
                snapshot.Blocks.begin(),
                snapshot.Blocks.end(),
                index,
                [] (const auto& indexAndId, int index) { return indexAndId.first < index; });
            if (it != snapshot.Blocks.end() && it->first == index) {
                it->second = id;
            }
        }

        // Drop blocks that were empty at the flip (first written mid-scan).
        EraseIf(
            snapshot.Blocks,
            [] (const auto& indexAndId) { return indexAndId.second == EmptyMappedBlockId; });

        return snapshot;
    }

    void BeginLoadSnapshot() final
    {
        YT_VERIFY(!LoadingSnapshot_);
        LoadingSnapshot_ = true;
    }

    void LoadSnapshotPart(const TBlockMapSnapshot& snapshot) final
    {
        YT_VERIFY(LoadingSnapshot_);

        for (auto [index, id] : snapshot.Blocks) {
            YT_VERIFY(IsStoredMappedBlockId(id) && !IsCoW(id));
            auto& slot = GetSlot(index);
            // The blocks come from a snapshot table, which may name the same block twice; refuse rather
            // than crash the server on it.
            if (TMappedBlockId(slot.load(std::memory_order::acquire)) != EmptyMappedBlockId) {
                THROW_ERROR_EXCEPTION("Snapshot maps block %v more than once", index);
            }
            slot.store(id.Underlying(), std::memory_order::release);
            UsedBlockCount_.fetch_add(1, std::memory_order::relaxed);
        }
    }

    void EndLoadSnapshot() final
    {
        YT_VERIFY(LoadingSnapshot_);
        LoadingSnapshot_ = false;
    }

    void IterateBlocks(const std::function<void(int blockIndex, TMappedBlockId mappedId)>& onBlock) const final
    {
        for (int index = 0; index < std::ssize(Slots_); ++index) {
            auto id = WithoutCoW(TMappedBlockId(Slots_[index].load(std::memory_order::acquire)));
            if (id != EmptyMappedBlockId) {
                onBlock(index, id);
            }
        }
    }

    DEFINE_SIGNAL_OVERRIDE(void(TDirtyBlockId dirtyBlockId, TStoredBlockId storedBlockId), BlockFlushObserved);
    DEFINE_SIGNAL_OVERRIDE(void(TStoredBlockId storedBlockId), StoredBlockUnreferenced);

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

    bool LoadingSnapshot_ = false;

    // A deque (not a vector) so appends under WriteLock_ never trigger an O(n) reallocation-and-copy.
    std::deque<std::pair<int, TMappedBlockId>> CoWBlocks_;

    std::atomic<TMappedBlockId::TUnderlying>& GetSlot(int blockIndex)
    {
        YT_ASSERT(0 <= blockIndex && blockIndex < std::ssize(Slots_));
        return Slots_[blockIndex];
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
