#pragma once

#include "public.h"

#include <core/misc/async_cache.h>
#include <core/misc/ref.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Represents a cached block of chunk.
class TCachedBlock
    : public TAsyncCacheValueBase<TBlockId, TCachedBlock>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TSharedRef, Data);
    DEFINE_BYREF_RO_PROPERTY(TNullable<NNodeTrackerClient::TNodeDescriptor>, Source);

public:
    //! Constructs a new block from id and data.
    TCachedBlock(
        const TBlockId& blockId,
        const TSharedRef& data,
        const TNullable<NNodeTrackerClient::TNodeDescriptor>& source);

};

DEFINE_REFCOUNTED_TYPE(TCachedBlock)

////////////////////////////////////////////////////////////////////////////////

//! Manages chunk blocks stored at Data Node.
/*!
 *  \note
 *  Thread affinity: any
 */
class TBlockStore
    : public TRefCounted
{
public:
    TBlockStore(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    ~TBlockStore();

    //! Synchronously looks up a single block in the store's cache.
    TCachedBlockPtr FindBlock(const TBlockId& blockId);

    //! Asynchronously reads a range of blocks from the store.
    /*!
     *  If some unrecoverable IO error happens during retrieval then the latter error is returned.
     *
     *  The resulting list may contain less blocks than requested.
     *  If the whole chunk or some of its blocks does not exist then null TSharedRef element may be returned.
     *  An empty list indicates that the requested blocks are all out of range.
     *
     *  Note that blob chunks will indicate an error if an attempt is made to read a non-existing block.
     *  Journal chunks, however, will silently ignore it.
     */
    TFuture<std::vector<TSharedRef>> ReadBlocks(
        const TChunkId& chunkId,
        int firstBlockIndex,
        int blockCount,
        i64 priority,
        bool enableCaching);

    //! Asynchronously reads a set of blocks from the store.
    /*!
     *  Tries to group block reads into contiguous ranges.
     */
    TFuture<std::vector<TSharedRef>> ReadBlocks(
        const TChunkId& chunkId,
        const std::vector<int>& blockIndexes,
        i64 priority,
        bool enableCaching);

    //! Puts a block into the store.
    /*!
     *  The store may already have another copy of the same block.
     *  In this case the block content is checked for identity.
     */
    void PutBlock(
        const TBlockId& blockId,
        const TSharedRef& data,
        const TNullable<NNodeTrackerClient::TNodeDescriptor>& source);

    //! Gets a vector of all blocks stored in the cache. Thread-safe.
    std::vector<TCachedBlockPtr> GetAllBlocks() const;

    //! Returns the number of bytes that are scheduled for disk read IO.
    i64 GetPendingReadSize() const;

    //! Acquires a lock for the given number of bytes to be read.
    TPendingReadSizeGuard IncreasePendingReadSize(i64 delta);

private:
    class TImpl;

    friend class TPendingReadSizeGuard;

    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TBlockStore)

////////////////////////////////////////////////////////////////////////////////

class TPendingReadSizeGuard
{
public:
    TPendingReadSizeGuard() = default;
    TPendingReadSizeGuard(TPendingReadSizeGuard&& other) = default;
    ~TPendingReadSizeGuard();

    TPendingReadSizeGuard& operator = (TPendingReadSizeGuard&& other);

    explicit operator bool() const;
    i64 GetSize() const;

    friend void swap(TPendingReadSizeGuard& lhs, TPendingReadSizeGuard& rhs);

private:
    friend TBlockStore;

    TPendingReadSizeGuard(i64 size, TBlockStorePtr owner);
        
    i64 Size_ = 0;
    TBlockStorePtr Owner_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

