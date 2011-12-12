#pragma once

#include "common.h"

#include "../misc/cache.h"
#include "../chunk_client/common.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Represents a cached block of chunk.
class TCachedBlock
    : public TCacheValueBase<NChunkClient::TBlockId, TCachedBlock>
{
public:
    typedef TIntrusivePtr<TCachedBlock> TPtr;

    //! Constructs a new block from id and data.
    TCachedBlock(const NChunkClient::TBlockId& blockId, const TSharedRef& data);

    ~TCachedBlock();

    //! Returns block data.
    TSharedRef GetData() const;

private:
    TSharedRef Data;

};

////////////////////////////////////////////////////////////////////////////////

class TChunkStore;
class TReaderCache;

//! Manages cached blocks.
class TBlockStore
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TBlockStore> TPtr;

    //! Constructs a store.
    TBlockStore(
        const TChunkHolderConfig& config,
        TChunkStore* chunkStore,
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
    TAsyncGetBlockResult::TPtr GetBlock(const NChunkClient::TBlockId& blockId);

    //! Puts a block into the store.
    /*!
     *  The store may already have another copy of the same block.
     *  In this case the block content is checked for identity.
     */
    TCachedBlock::TPtr PutBlock(const NChunkClient::TBlockId& blockId, const TSharedRef& data);

private:
    class TBlockCache;
    friend class TBlockCache;

    TIntrusivePtr<TBlockCache> BlockCache;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

