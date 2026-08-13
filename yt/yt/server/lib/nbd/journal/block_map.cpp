#include "block_map.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/memory/new.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/error/error.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <atomic>
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
        std::optional<TStoredBlockId> unreferencedStoredBlockId;
        {
            auto guard = Guard(WriteLock_);

            auto& slot = GetSlot(blockIndex);
            auto oldId = TMappedBlockId(slot.load(std::memory_order::acquire));
            auto bareOldId = WithoutCoW(oldId);

            if (bareOldId == EmptyMappedBlockId) {
                UsedBlockCount_.fetch_add(1, std::memory_order::relaxed);
            }

            if (IsStoredMappedBlockId(bareOldId)) {
                unreferencedStoredBlockId = ToStoredBlockId(bareOldId);
            }

            auto newId = ToMappedBlockId(blockId);
            // While a snapshot scans, stash the pre-snapshot value the first time we overwrite a block
            // (the CoW bit means "already stashed") so a scan can read it back.
            if (SnapshotState_ == ESnapshotState::CoWActive) {
                if (!IsCoW(oldId)) {
                    EmplaceOrCrash(CoWBlocks_, blockIndex, oldId);
                }
                newId = WithCoW(newId);
            }
            slot.store(newId.Underlying(), std::memory_order::release);
        }

        // Fire outside WriteLock_ (subscribers may re-enter the map), strictly after the slot update.
        if (unreferencedStoredBlockId) {
            StoredBlockUnreferenced_.Fire(*unreferencedStoredBlockId);
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
                        EmplaceOrCrash(CoWBlocks_, blockIndex, oldId);
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

    bool DiscardBlock(int blockIndex) final
    {
        std::optional<TStoredBlockId> unreferencedStoredBlockId;
        {
            auto guard = Guard(WriteLock_);

            auto& slot = GetSlot(blockIndex);
            auto oldId = TMappedBlockId(slot.load(std::memory_order::acquire));
            auto bareOldId = WithoutCoW(oldId);
            if (bareOldId == EmptyMappedBlockId) {
                return false;
            }

            UsedBlockCount_.fetch_sub(1, std::memory_order::relaxed);
            if (IsStoredMappedBlockId(bareOldId)) {
                unreferencedStoredBlockId = ToStoredBlockId(bareOldId);
            }

            auto newId = EmptyMappedBlockId;
            if (SnapshotState_ == ESnapshotState::CoWActive) {
                if (!IsCoW(oldId)) {
                    EmplaceOrCrash(CoWBlocks_, blockIndex, oldId);
                }
                newId = WithCoW(newId);
            }
            slot.store(newId.Underlying(), std::memory_order::release);
        }

        if (unreferencedStoredBlockId) {
            StoredBlockUnreferenced_.Fire(*unreferencedStoredBlockId);
        }

        return true;
    }

    int GetBlockCount() const final
    {
        return std::ssize(Slots_);
    }

    int GetUsedBlockCount() const final
    {
        return UsedBlockCount_.load(std::memory_order::acquire);
    }

    void BeginSnapshot() final
    {
        auto guard = Guard(WriteLock_);

        if (SnapshotState_ != ESnapshotState::None) {
            THROW_ERROR_EXCEPTION("Another snapshot is already in progress");
        }

        SnapshotState_ = ESnapshotState::CoWActive;
        YT_VERIFY(CoWBlocks_.empty());
    }

    TBlockMapSnapshot ScanSnapshotPart(int beginBlockIndex, int endBlockIndex) final
    {
        YT_VERIFY(SnapshotState_ == ESnapshotState::CoWActive);
        YT_VERIFY(0 <= beginBlockIndex && beginBlockIndex <= endBlockIndex && endBlockIndex <= std::ssize(Slots_));

        // Positions in Blocks whose slot was overwritten since the flip, to be filled in from the stash.
        std::vector<int> stashedPositions;

        TBlockMapSnapshot snapshot;
        for (int index = beginBlockIndex; index < endBlockIndex; ++index) {
            auto id = TMappedBlockId(GetSlot(index).load(std::memory_order::acquire));
            if (IsCoW(id)) {
                stashedPositions.push_back(std::ssize(snapshot.Blocks));
            } else if (id == EmptyMappedBlockId) {
                // Untouched since the flip, and empty then too.
                continue;
            }
            snapshot.Blocks.emplace_back(index, id);
        }

        if (!stashedPositions.empty()) {
            {
                auto guard = Guard(WriteLock_);
                for (int position : stashedPositions) {
                    auto& [index, id] = snapshot.Blocks[position];
                    id = GetOrCrash(CoWBlocks_, index);
                }
            }

            // Drop those the stash says were empty at the flip.
            EraseIf(snapshot.Blocks, [] (const auto& indexAndId) { return indexAndId.second == EmptyMappedBlockId; });
        }

        return snapshot;
    }

    void EndSnapshot() final
    {
        // Past this barrier no writer stashes anything, so CoWBlocks_ is complete and stable.
        {
            auto guard = Guard(WriteLock_);

            YT_VERIFY(SnapshotState_ == ESnapshotState::CoWActive);
            SnapshotState_ = ESnapshotState::CoWCleanup;
        }

        // Clear the CoW bit of every stashed block, so the next snapshot starts clean.
        for (const auto& [index, id] : CoWBlocks_) {
            GetSlot(index).fetch_and(~CoWMask, std::memory_order::release);
        }

        {
            auto guard = Guard(WriteLock_);

            SnapshotState_ = ESnapshotState::None;
            CoWBlocks_.clear();
        }
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

    std::vector<std::pair<int, TStoredBlockId>> GetChunkBlocks(int chunkIndex) const final
    {
        YT_VERIFY(0 <= chunkIndex && chunkIndex < MaxChunksPerDevice);

        using namespace NStoredBlockIdLayout;

        auto slotMask = TagMask | (((1ULL << ChunkIndexBits) - 1) << (RecordIndexBits + BlockIndexBits));
        auto slotExpected = (StoredTag << PayloadBits) |
            (static_cast<ui64>(chunkIndex) << (RecordIndexBits + BlockIndexBits));

        std::vector<std::pair<int, TStoredBlockId>> blocks;
        for (int index = 0; index < std::ssize(Slots_); ++index) {
            auto slot = Slots_[index].load(std::memory_order::acquire);
            if ((slot & slotMask) == slotExpected) {
                blocks.emplace_back(index, ToStoredBlockId(TMappedBlockId(slot)));
            }
        }
        return blocks;
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

    //! Pre-snapshot value of every block overwritten since the snapshot was armed, so a scan can read
    //! back what a CoW-marked slot held at the flip. Guarded by WriteLock_.
    absl::flat_hash_map<int, TMappedBlockId> CoWBlocks_;

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
