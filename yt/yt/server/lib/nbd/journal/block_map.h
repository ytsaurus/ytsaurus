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

//! A point-in-time snapshot of every used block of the map: a (block index, mapped block id) pair per
//! non-empty block, in ascending index order.
/*!
 *  A snapshot of a live (concurrently written) device (see #TakeSnapshot) may mix stored (clean) and
 *  dirty mapped ids; a snapshot handed to #LoadSnapshot must hold only stored (clean) mapped ids.
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
    virtual void PutDirty(int blockIndex, TDirtyBlockId blockId) = 0;

    //! Publishes the block at |blockIndex| as flushed to the block store under |storedBlockId|, but
    //! only if it is still dirty under |expectedBlockId| — i.e. no newer write superseded it since it
    //! was drained.
    /*!
     *  Returns whether the transition happened. This guards last-write-wins: a flush that raced a
     *  newer write to the same block must not clobber it with the older, already-flushed content.
     */
    virtual bool TryMakeClean(int blockIndex, TDirtyBlockId expectedBlockId, TStoredBlockId storedBlockId) = 0;

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
    virtual TBlockMapSnapshot TakeSnapshot(std::function<void(int blockIndex)> onScanned = {}) = 0;

    //! Loads a previously saved device snapshot into the map, publishing every one of its blocks as
    //! flushed to the block store.
    /*!
     *  Every snapshot block must carry a stored (clean) mapped id. Only valid before the device serves
     *  any I/O; each target slot must still be empty.
     */
    virtual void LoadSnapshot(const TBlockMapSnapshot& snapshot) = 0;

    //! Fired once the map has processed the flush of a dirty block, reporting where its payload landed.
    DECLARE_INTERFACE_SIGNAL(void(TDirtyBlockId dirtyBlockId, TStoredBlockId storedBlockId), BlockFlushObserved);
};

DEFINE_REFCOUNTED_TYPE(IBlockMap)

////////////////////////////////////////////////////////////////////////////////

IBlockMapPtr CreateBlockMap(int blockCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
