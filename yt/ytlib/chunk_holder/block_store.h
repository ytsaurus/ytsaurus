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
    typedef TFuture<TPtr> TAsync;

    //! Constructs a new block from id and data.
    TCachedBlock(const NChunkClient::TBlockId& blockId, const TSharedRef& data);

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

    //! Gets (asynchronously) a block from the store.
    /*!
     * This call returns an async result that becomes set when the 
     * block is fetched. Fetching an already-cached block is cheap
     * (i.e. requires no context switch). Fetching an uncached block
     * enqueues a disk-read action to the appropriate IO queue.
     */
    TCachedBlock::TAsync::TPtr FindBlock(const NChunkClient::TBlockId& blockId);

    TCachedBlock::TPtr PutBlock(const NChunkClient::TBlockId& blockId, const TSharedRef& data);

private:
    class TBlockCache;
    friend class TBlockCache;

    TIntrusivePtr<TBlockCache> BlockCache;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

