// TODO: rename to block_store.h/block_store.cpp

#pragma once

#include "common.h"

#include "../misc/cache.h"
#include "../actions/action_queue.h"
#include "misc/lease_manager.h"

#include <util/autoarray.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCachedBlock
    : public TCacheValueBase<TBlockId, TCachedBlock, TBlockIdHash>
{
public:
    typedef TIntrusivePtr<TCachedBlock> TPtr;
    typedef TAsyncResult<TPtr> TAsync;

    TCachedBlock(const TBlockId& blockId, const TSharedRef& data);

    TSharedRef GetData() const;

private:
    TSharedRef Data;

};

////////////////////////////////////////////////////////////////////////////////

class TBlockStore
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TBlockStore> TPtr;

    TBlockStore(TChunkHolderConfig& config);

    //! Gets (asynchronously) a block from the store.
    /*!
     * This call returns an async result that becomes set when the 
     * block is fetched. Fetching an already-cached block is cheap
     * (i.e. requires no context switch). Fetching an uncached block
     * enqueues a disk-read action to the appropriate IO queue.
     */
    TCachedBlock::TAsync::TPtr GetBlock(const TBlockId& blockId, i32 blockSize);

    void PutBlock(const TBlockId& blockId, const TSharedRef& data);

private:
    class TBlockCache;

    //! Caches blocks in memory.
    TIntrusivePtr<TBlockCache> Cache;

    //! Actions queues that handle IO requests to chunk storage locations.
    autoarray< THolder<TActionQueue> > IOQueues;

};

////////////////////////////////////////////////////////////////////////////////

class TChunkSessionManager;

class TChunkSession
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkSession> TPtr;

    TChunkSession(TIntrusivePtr<TChunkSessionManager> sessionManager);

    //! Returns a cached block that is still in the session window.
    TCachedBlock::TPtr GetBlock(int blockIndex);

    //! Puts (synchronously) a block into the store.
    void PutBlock(
        i32 blockIndex,
        const TBlockId& blockId,
        const TSharedRef& data);

    //! Flushes blocks from the session window.
    TAsyncResult<TVoid>::TPtr FlushBlocks(
        i32 firstBlockIndex,
        i32 blockCount);

    //! Returns target chunk location.
    int GetLocation() const;

    void SetLease(TLeaseManager::TLease lease);

    void RenewLease();

private:
    TIntrusivePtr<TChunkSessionManager> SessionManager;
    TLeaseManager::TLease Lease;

};

////////////////////////////////////////////////////////////////////////////////

class TChunkSessionManager
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkSessionManager> TPtr;
    typedef TChunkSession TSession;

    //! Starts a new chunk upload session.
    TSession::TPtr StartSession(const TChunkId& chunkId);

    //! Completes an earlier opened upload session.
    void FinishSession(TSession::TPtr session);

    //! Cancels an earlier opened upload session.
    void CancelSession(TSession::TPtr session);

    //! Finds a session by TChunkId.
    TSession::TPtr FindSession(const TChunkId& chunkId);

private:
    TLeaseManager::TPtr LeaseManager;

    TChunkHolderConfig Config; // TODO: avoid copying

    IInvoker::TPtr ServiceInvoker;

    typedef yhash_map<TChunkId, TSession::TPtr, TGUIDHash> TSessionMap;
    TSessionMap SessionMap;

    void InitLocations();

    void OnLeaseExpired(TSession::TPtr session);

};

////////////////////////////////////////////////////////////////////////////////

}
