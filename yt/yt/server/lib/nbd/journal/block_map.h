#pragma once

#include "private.h"

#include <yt/yt/core/actions/signal.h>

#include <functional>
#include <utility>
#include <vector>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! A TMappedBlockId packs a tag and a payload (see NMappedBlockIdLayout). These classify a mapped id by
//! tag and convert to/from the corresponding payload. Converting to a payload asserts the tag matches
//! (debug only); converting from one verifies the payload fits.
bool IsStoredMappedBlockId(TMappedBlockId id);
TStoredBlockId ToStoredBlockId(TMappedBlockId id);
TMappedBlockId ToMappedBlockId(TStoredBlockId id);

bool IsDirtyMappedBlockId(TMappedBlockId id);
TDirtyBlockId ToDirtyBlockId(TMappedBlockId id);
TMappedBlockId ToMappedBlockId(TDirtyBlockId id);

////////////////////////////////////////////////////////////////////////////////

//! A snapshot of the map's used blocks: a (block index, mapped block id) pair per non-empty block, in
//! ascending index order. A point-in-time cut of the whole map when produced by #TakeSnapshot, or one
//! batch of a larger snapshot when fed to #LoadSnapshotPart.
/*!
 *  A snapshot of a live (concurrently written) device (see #TakeSnapshot) may mix stored (clean) and
 *  dirty mapped ids; a snapshot handed to #LoadSnapshotPart must hold only stored (clean) mapped ids.
 */
struct TBlockMapSnapshot
{
    std::vector<std::pair<int, TMappedBlockId>> Blocks;
};

//! Maps each of a device's blocks to where its latest content lives: nowhere yet
//! (|EmptyMappedBlockId|), in the dirty block pool (a dirty mapped id), or in the block store
//! (a stored mapped id).
/*!
 *  Thread affinity: any.
 */
struct IBlockMap
    : public TRefCounted
{
    //! Returns the mapped id of the block at |blockIndex|: |EmptyMappedBlockId| if it has never been
    //! written, otherwise a dirty or stored mapped id (see #IsDirtyMappedBlockId/#IsStoredMappedBlockId).
    virtual TMappedBlockId FindBlock(int blockIndex) = 0;

    //! Records that the latest content of the block at |blockIndex| now sits in the dirty
    //! block pool under |blockId|, overwriting its previous mapping.
    virtual void PutBlock(int blockIndex, TDirtyBlockId blockId) = 0;

    //! Points the block at |blockIndex| at |storedBlockId|, a freshly stored copy of its content, but
    //! only if it still maps to |expectedBlockId| -- i.e. no newer write superseded it. Returns whether it did.
    /*!
     *  Guards last-write-wins for the two publishers of stored content: a flush (|expectedBlockId| a dirty id,
     *  as drained from the pool) and a compaction relocation (|expectedBlockId| a stored id).
     *
     *  A dirty |expectedBlockId| additionally fires #BlockFlushObserved (always, adopted or not) reporting where the
     *  flush landed. On success a superseded stored |expectedBlockId| becomes unreferenced (a dirty one was
     *  never a stored block); on failure the orphaned |storedBlockId| does (see #StoredBlockUnreferenced).
     */
    virtual bool TryPutBlock(int blockIndex, TMappedBlockId expectedBlockId, TStoredBlockId storedBlockId) = 0;

    //! Returns the number of blocks that have ever been written, i.e. are no longer empty.
    virtual int GetUsedBlockCount() const = 0;

    //! Snapshots every used block as a single point-in-time cut, concurrently with ongoing writes.
    /*!
     *  Empty blocks are omitted; used blocks are reported by their mapped id (stored if clean, dirty if
     *  still in the pool), in ascending block index order. At most one snapshot may run at a time.
     *
     *  Testing only: |onScanned|, if set, is invoked with each slot index before that slot is read, so a
     *  test can inject concurrent mutations at a chosen scan position.
     */
    virtual TBlockMapSnapshot TakeSnapshot(const std::function<void(int blockIndex)>& onScanned = {}) = 0;

    //! Brackets a batched load of a previously saved device snapshot. Only valid before the device
    //! serves any I/O.
    virtual void BeginLoadSnapshot() = 0;

    //! Loads one batch of a device snapshot into the map, publishing its blocks as flushed to the block
    //! store. Batches accumulate.
    /*!
     *  Every snapshot block must carry a stored (clean) mapped id, and each target slot must still be
     *  empty.
     */
    virtual void LoadSnapshotPart(const TBlockMapSnapshot& snapshot) = 0;

    //! Ends the batched load of a device snapshot.
    virtual void EndLoadSnapshot() = 0;

    //! Invokes |onBlock| with each used block's index and current mapped id, in ascending index order.
    /*!
     *  A racy lock-free scan: it reflects each slot at the moment it is visited.
     */
    virtual void IterateBlocks(const std::function<void(int blockIndex, TMappedBlockId mappedId)>& onBlock) const = 0;

    //! Fired once the map has processed the flush of a dirty block, reporting where its payload landed.
    DECLARE_INTERFACE_SIGNAL(void(TDirtyBlockId dirtyBlockId, TStoredBlockId storedBlockId), BlockFlushObserved);

    //! Fired when a stored block stops being referenced -- a newer write to the same device block
    //! superseded it, or its flush was never adopted (lost the last-write-wins race). Every stored block
    //! handed to the map is unreferenced exactly once. Lets the store free a chunk once none of its
    //! blocks are referenced.
    DECLARE_INTERFACE_SIGNAL(void(TStoredBlockId storedBlockId), StoredBlockUnreferenced);
};

DEFINE_REFCOUNTED_TYPE(IBlockMap)

////////////////////////////////////////////////////////////////////////////////

IBlockMapPtr CreateBlockMap(int blockCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
