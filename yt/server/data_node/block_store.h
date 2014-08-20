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

    typedef TErrorOr<TSharedRef> TGetBlockResult;
    typedef TFuture<TGetBlockResult> TAsyncGetBlockResult;

    typedef TErrorOr<std::vector<TSharedRef>> TGetBlocksResult;
    typedef TFuture<TGetBlocksResult> TAsyncGetBlocksResult;

    //! Asynchronously retrieves a block from the store.
    /*!
     *  Fetching an already-cached block is cheap (i.e. requires no context switch).
     *  Fetching an uncached block enqueues a disk-read action to the appropriate IO queue.
     * 
     *  If the requested block does not exist then an error is returned.
     *  However, if the requested chunk is missing then cache lookup is performed.
     *  In the latter case null may be returned for non-existing blocks.
     */
    TAsyncGetBlockResult GetBlock(
        const TChunkId& chunkId,
        int blockIndex,
        i64 priority,
        bool enableCaching);

    //! Asynchronously retrieves a range of blocks from the store.
    /*!
     *  The resulting list may contain less blocks than requested.
     *  An empty list indicates that the requested blocks are all out of range.
     */
    TAsyncGetBlocksResult GetBlocks(
        const TChunkId& chunkId,
        int firstBlockIndex,
        int blockCount,
        i64 priority);

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

    //! Returns a caching adapter.
    NChunkClient::IBlockCachePtr GetCompressedBlockCache();

private:
    class TStoreImpl;
    class TCacheImpl;
    class TGetBlocksSession;

    friend class TPendingReadSizeGuard;

    TIntrusivePtr<TStoreImpl> StoreImpl_;
    TIntrusivePtr<TCacheImpl> CacheImpl_;

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

private:
    friend TBlockStore;

    TPendingReadSizeGuard(i64 size, TBlockStorePtr owner);
        
    void Destroy();

    i64 Size_ = 0;
    TBlockStorePtr Owner_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

