#pragma once

#include "common.h"
#include "config.h"

#include <ytlib/misc/cache.h>
#include <ytlib/chunk_client/block_cache.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Represents a cached block of chunk.
class TCachedBlock
    : public TCacheValueBase<TBlockId, TCachedBlock>
{
public:
    typedef TIntrusivePtr<TCachedBlock> TPtr;

    //! Constructs a new block from id and data.
    TCachedBlock(
        const TBlockId& blockId,
        const TSharedRef& data,
        const Stroka& source);

    ~TCachedBlock();

    DEFINE_BYVAL_RO_PROPERTY(TSharedRef, Data);
    DEFINE_BYREF_RO_PROPERTY(Stroka, Source);
};

////////////////////////////////////////////////////////////////////////////////

class TReaderCache;
class TChunkRegistry;

//! Manages cached blocks.
class TBlockStore
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TBlockStore> TPtr;

    //! Constructs a store.
    TBlockStore(
        TChunkHolderConfig* config,
        TChunkRegistry* chunkRegistry,
        TReaderCache* readerCache);

    typedef TValueOrError<TCachedBlock::TPtr> TGetBlockResult;
    typedef TFuture<TGetBlockResult> TAsyncGetBlockResult;

    //! Gets (asynchronously) a block from the store.
    /*!
     * This call returns an async result that becomes set when the 
     * block is fetched. Fetching an already-cached block is cheap
     * (i.e. requires no context switch). Fetching an uncached block
     * enqueues a disk-read action to the appropriate IO queue.
     */
    TAsyncGetBlockResult::TPtr GetBlock(const TBlockId& blockId);

    //! Tries to find a block in the cache.
    /*!
     *  If the block is not available immediately, it returns NULL.
     *  No IO is queued.
     */
    TCachedBlock::TPtr FindBlock(const TBlockId& blockId);

    //! Puts a block into the store.
    /*!
     *  The store may already have another copy of the same block.
     *  In this case the block content is checked for identity.
     */
    TCachedBlock::TPtr PutBlock(
        const TBlockId& blockId,
        const TSharedRef& data,
        const Stroka& source);

    //! Gets a vector of all blocks stored in the cache. Thread-safe.
    yvector<TCachedBlock::TPtr> GetAllBlocks() const;

    //! Returns the number of bytes that are scheduled for disk read IO.
    i64 GetPendingReadSize() const;

    //! Returns a caching adapter.
    NChunkClient::IBlockCache* GetBlockCache();

private:
    class TStoreImpl;
    friend class TStoreImpl;

    class TCacheImpl;

    TIntrusivePtr<TStoreImpl> StoreImpl;
    TIntrusivePtr<TCacheImpl> CacheImpl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

