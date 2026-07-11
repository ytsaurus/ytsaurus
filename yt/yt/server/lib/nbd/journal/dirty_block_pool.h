#pragma once

#include "private.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! A single dirty (written but not yet flushed) block.
struct TDirtyBlock final
{
    TDirtyBlock(int blockIndex, TSharedRef payload);

    const int BlockIndex;
    const TSharedRef Payload;

    //! Filled in by the dirty block pool when it accepts the block (IDirtyBlockPool::Put); unset
    //! before that. Identifies the block's pool slot so later Find calls can locate it and detect
    //! that the slot has since been reused.
    TDirtyBlockId BlockId;
};

////////////////////////////////////////////////////////////////////////////////

//! A bounded, unordered pool of dirty blocks sitting between the write path (Put) and the
//! background flusher (BeginDrain/EndDrain), providing back-pressure once it fills up.
/*!
 *  Thread affinity: any.
 */
struct IDirtyBlockPool
    : public TRefCounted
{
    //! Maximum number of blocks the pool can hold; fixed at construction.
    virtual int GetCapacity() const = 0;

    //! Number of blocks currently occupying the pool (put but not yet retired by EndDrain).
    virtual int GetSize() const = 0;

    //! Adds |blocks| to the pool and returns the ids of the accepted blocks.
    /*!
     *  A put may be partial: only a prefix of |blocks| is accepted (as much as currently
     *  fits), and the caller is expected to resubmit the rest. When the pool is momentarily
     *  full, the returned future is set once space frees up and a non-empty prefix is
     *  accepted. Never fails.
     */
    virtual TFuture<std::vector<TDirtyBlockId>> Put(TRange<TDirtyBlockPtr> blocks) = 0;

    //! Returns the block put under |blockId|, or null if it is no longer in the pool.
    /*!
     *  A block is returned only if it is still present and its #BlockIndex matches
     *  |blockIndex|; otherwise (drained, evicted, or its slot reused) null is returned.
     */
    virtual TDirtyBlockPtr Find(TDirtyBlockId blockId, int blockIndex) = 0;

    using TBeginDrainResult = std::vector<TDirtyBlockPtr>;
    //! Returns up to |maxBlockCount| of the oldest blocks without removing them from the pool.
    /*!
     *  The returned blocks stay in the pool (and findable via #Find) until the matching #EndDrain,
     *  so reads still see them while their flush is in flight. Meant to be paired with #EndDrain by
     *  a single drainer with at most one drain outstanding. Each block carries its #BlockId, which
     *  the drainer needs to conditionally publish it as clean without clobbering a newer write.
     */
    virtual TBeginDrainResult BeginDrain(int maxBlockCount) = 0;

    //! Removes the blocks handed out by the preceding #BeginDrain, freeing their space so that
    //! waiting Puts can make progress.
    virtual void EndDrain(const TBeginDrainResult& result) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDirtyBlockPool)

////////////////////////////////////////////////////////////////////////////////

IDirtyBlockPoolPtr CreateDirtyBlockPool(int capacityCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
