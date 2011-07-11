#pragma once

#include "chunk.h"
#include "chunk_holder.pb.h"
#include "block_cache.h"
#include "chunk_holder_rpc.h"
#include "../misc/config.h"
#include "../misc/lease_manager.h"
#include "../rpc/client.h"
#include "../rpc/service.h"
#include "../actions/action_queue.h"

#include <util/autoarray.h>
#include <util/generic/ptr.h>
#include <util/generic/stroka.h>
#include <util/stream/ios.h>
#include <util/system/file.h>


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TChunkHolderConfig
{
    TBlockCache::TConfig BlockCacheConfig;
    i32 WindowSize;
    TDuration LeaseTimeout;
    yvector<Stroka> Locations;

    TChunkHolderConfig()
        : WindowSize(256)
        , LeaseTimeout(TDuration::Seconds(10))
    { }

    void Read(const TJsonObject* jsonConfig);
};

////////////////////////////////////////////////////////////////////////////////

class TChunkHolder
    : public NRpc::TServiceBase
{
    // IO actions
    class TWriteBlocks;

    // Service completion actions
    class TCompleteFlush;

    class TSession;
    typedef TIntrusivePtr<TSession> TSessionPtr;
    typedef yhash_map<TChunkId, TSessionPtr, TGUIDHash> TSessionMap;
    typedef yhash_map<TChunkId, i32, TGUIDHash> TLocationMap;

    const TChunkHolderConfig Config;

    THolder<TBlockCache> BlockCache; // Thread safe
    TActionQueue::TPtr ServiceQueue;
    autoarray< THolder<TActionQueue> > IOQueues;

    // TODO: introduce TChunkInfo and TChunkMap
    TLocationMap LocationMap; // Locations of complete chunks
    TSessionMap Sessions; // Write in progress
    TLeaseManager::TPtr LeaseManager; // Expiring write sessions

    typedef TChunkHolderProxy TProxy;
    typedef NRpc::TTypedServiceException<TProxy::EErrorCode> TServiceException;

    NRpc::TChannelCache ChannelCache;

public:
    TChunkHolder(const TChunkHolderConfig& config);
    ~TChunkHolder();

private:
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, StartChunk);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, FinishChunk);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, PutBlocks);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, SendBlocks);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, FlushBlocks);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, GetBlocks);

    void RegisterMethods();
    void InitLocations();

    typedef TIntrusivePtr<NRpc::TServiceContext> TServiceContextPtr;
    void ReadBlocks(TCtxGetBlocks::TPtr context, Stroka fileName, const yvector<size_t>& uncached);
    void WriteBlocks(
        TSessionPtr session,
        TAutoPtr< yvector<TCachedBlock::TPtr> > blocks,
        i32 FirstUnWritten);

    void CreateSession(const TChunkId& chunkId);
    void CancelSession(const TChunkId& chunkId);
    void DoCancelSession(TSessionPtr session);
    void DoFinishChunk(TCtxFinishChunk::TPtr context, TSessionPtr session);
    void PutBlocksCallback(TProxy::TRspPutBlocks::TPtr putResponse, TCtxSendBlocks::TPtr context);

    // Service thread only!
    i32 GetNewLocation(const TChunkId& chunkId) const;
    Stroka GetFileName(const TChunkId& chunkId) const;
    Stroka GetFileName(const TChunkId& chunkId, i32 location) const;

    TSessionPtr GetSession(const TChunkId& chunkId);
    TSessionMap::iterator GetSessionIter(const TChunkId& chunkId);
    void VerifyNoSession(const TChunkId& chunkId);
    void VerifyNoChunk(const TChunkId& chunkId);
    void CheckLease(bool leaseResult, const TChunkId& chunkId);
};

////////////////////////////////////////////////////////////////////////////////

}
