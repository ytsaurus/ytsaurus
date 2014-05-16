#pragma once

#include "public.h"

#include <core/misc/cache.h>
#include <core/misc/ref.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Represents a cached block of chunk.
class TCachedBlock
    : public TCacheValueBase<TBlockId, TCachedBlock>
{
public:
    //! Constructs a new block from id and data.
    TCachedBlock(
        const TBlockId& blockId,
        const TSharedRef& data,
        const TNullable<NNodeTrackerClient::TNodeDescriptor>& source);

    ~TCachedBlock();

    DEFINE_BYVAL_RO_PROPERTY(TSharedRef, Data);
    DEFINE_BYREF_RO_PROPERTY(TNullable<NNodeTrackerClient::TNodeDescriptor>, Source);
};

DEFINE_REFCOUNTED_TYPE(TCachedBlock)

////////////////////////////////////////////////////////////////////////////////

//! Manages cached blocks.
class TBlockStore
    : public TRefCounted
{
public:
    TBlockStore(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    void Initialize();

    ~TBlockStore();

    typedef TErrorOr<std::vector<TSharedRef>> TGetBlocksResult;

    //! Asynchronously retrives a range of blocks from the store.
    /*!
     * This call returns a promise to the list of requested blocks.
     *
     * Fetching an already-cached block is cheap (i.e. requires no context switch).
     * Fetching an uncached block enqueues a disk-read action to the appropriate IO queue.
     * 
     * If some block is missing then the corresponding entry is null.
     */
    TFuture<TGetBlocksResult> GetBlocks(
        const TChunkId& chunkId,
        int firstBlockIndex,
        int blockCount,
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

    //! Updates (increments or decrements) pending read size.
    void UpdatePendingReadSize(i64 delta);

    //! Returns a caching adapter.
    NChunkClient::IBlockCachePtr GetBlockCache();

private:
    class TStoreImpl;
    class TCacheImpl;
    class TGetBlocksSession;

    TIntrusivePtr<TStoreImpl> StoreImpl_;
    TIntrusivePtr<TCacheImpl> CacheImpl_;

};

DEFINE_REFCOUNTED_TYPE(TBlockStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

