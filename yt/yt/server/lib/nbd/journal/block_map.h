#pragma once

#include "private.h"

#include <variant>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! The state of a block that has never been written.
struct TEmptyBlock
{ };

//! Maps each of a device's blocks to where its latest content lives: nowhere yet
//! (|TEmptyBlock|), in the dirty block pool (|TDirtyBlockId|), or in the block store
//! (|TStoredBlockId|).
/*!
 *  Thread affinity: any.
 */
struct IBlockMap
    : public TRefCounted
{
    //! Returns the current state of the block at |blockIndex|.
    virtual std::variant<TEmptyBlock, TStoredBlockId, TDirtyBlockId> Find(int blockIndex) = 0;

    //! Records that the latest content of the block at |blockIndex| now sits in the dirty
    //! block pool under |blockId|, overwriting its previous state.
    virtual void PutDirty(int blockIndex, TDirtyBlockId blockId) = 0;

    //! Publishes the block at |blockIndex| as flushed to the block store under |storedBlockId|,
    //! but only if it is still dirty under |expectedBlockId| — i.e. no newer write superseded it
    //! since it was drained. Returns whether the transition happened.
    /*!
     *  This guards last-write-wins: a flush that raced a newer write to the same block must not
     *  clobber it with the older, already-flushed content.
     */
    virtual bool TryMakeClean(int blockIndex, TDirtyBlockId expectedBlockId, TStoredBlockId storedBlockId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBlockMap)

////////////////////////////////////////////////////////////////////////////////

IBlockMapPtr CreateBlockMap(int blockCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
