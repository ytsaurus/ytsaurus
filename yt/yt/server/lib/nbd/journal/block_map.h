#pragma once

#include "private.h"

#include <yt/yt/core/actions/signal.h>

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

//! A run of the map's used blocks: a (block index, mapped block id) pair per non-empty block, in
//! ascending index order. Part of a point-in-time cut when produced by #ScanSnapshotPart, or one batch
//! of a device snapshot when fed to #LoadSnapshotPart.
/*!
 *  A cut of a live (concurrently written) device may mix stored (clean) and dirty mapped ids; a batch
 *  handed to #LoadSnapshotPart must hold only stored (clean) ones.
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
     *  Guards last-write-wins for the two publishers of stored content: a flush (|expectedBlockId| a dirty id)
     *  and a compaction relocation (|expectedBlockId| a stored id).
     *
     *  A dirty |expectedBlockId| additionally fires #BlockFlushObserved (always, adopted or not) reporting where the
     *  flush landed. On success a superseded stored |expectedBlockId| becomes unreferenced; on failure the orphaned
     *  |storedBlockId| does (see #StoredBlockUnreferenced).
     */
    virtual bool TryPutBlock(int blockIndex, TMappedBlockId expectedBlockId, TStoredBlockId storedBlockId) = 0;

    //! Resets the block at |blockIndex| to empty, so that it reads back as never written. Returns
    //! whether it was non-empty.
    /*!
     *  A superseded stored payload becomes unreferenced (see #StoredBlockUnreferenced). A dirty one is
     *  not withdrawn from the pool -- its flush simply finds the slot changed and is not adopted, which
     *  frees the block it wrote.
     */
    virtual bool DiscardBlock(int blockIndex) = 0;

    //! Returns the number of currently non-empty blocks.
    virtual int GetUsedBlockCount() const = 0;

    //! Returns the number of blocks the map covers, empty ones included.
    virtual int GetBlockCount() const = 0;

    //! Fixes the point in time a snapshot cuts at, concurrently with ongoing writes.
    /*!
     *  Arms copy-on-write: a writer stashes a block's pre-snapshot value the first time it overwrites
     *  it, so #ScanSnapshotPart can report that value however late it runs. At most one snapshot may be
     *  open at a time, and #EndSnapshot must close it.
     *
     *  The stash grows with the number of distinct blocks written while the snapshot is open, so a
     *  caller should not hold one open longer than it must.
     */
    virtual void BeginSnapshot() = 0;

    //! Returns the blocks of [|beginBlockIndex|, |endBlockIndex|) used as of #BeginSnapshot.
    /*!
     *  Every part reads the same cut however late it is taken.
     */
    virtual TBlockMapSnapshot ScanSnapshotPart(int beginBlockIndex, int endBlockIndex) = 0;

    //! Closes the snapshot: disarms copy-on-write and drops the stash.
    virtual void EndSnapshot() = 0;

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

    //! Returns the blocks whose content is stored in the chunk at |chunkIndex|, in ascending index order.
    /*!
     *  A racy lock-free scan: it reflects each slot at the moment it is visited.
     */
    virtual std::vector<std::pair<int, TStoredBlockId>> GetChunkBlocks(int chunkIndex) const = 0;

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
