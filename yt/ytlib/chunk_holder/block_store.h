#pragma once

#include "common.h"
#include "config.h"

#include <ytlib/misc/cache.h>
#include <ytlib/chunk_client/block_cache.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Keeps information about a peer possibly holding a block.
struct TPeerInfo
{
    Stroka Address;
    TInstant ExpirationTime;

    TPeerInfo()
    { }

    TPeerInfo(const Stroka& address, TInstant expirationTime)
        : Address(address)
        , ExpirationTime(expirationTime)
    { }
};

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

    //! Gets peers possibly holding this block.
    /*!
     *  Also sweeps expired peers.
     */
    yvector<TPeerInfo> GetPeers();

    //! Register a new peer.
    /*!
     *  Also sweeps expired peers.
     */
    void AddPeer(const TPeerInfo& peer);

private:
    TSharedRef Data;

    //! Sorted by decreasing expiration time.
    yvector<TPeerInfo> Peers;

    void SweepExpiredPeers();

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
    TAsyncGetBlockResult::TPtr GetBlock(const NChunkClient::TBlockId& blockId);

    //! Tries to find a block in the cache.
    /*!
     *  If the block is not available immediately, it returns NULL.
     *  No IO is queued.
     */
    TCachedBlock::TPtr FindBlock(const NChunkClient::TBlockId& blockId);

    //! Puts a block into the store.
    /*!
     *  The store may already have another copy of the same block.
     *  In this case the block content is checked for identity.
     */
    TCachedBlock::TPtr PutBlock(const NChunkClient::TBlockId& blockId, const TSharedRef& data);

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

