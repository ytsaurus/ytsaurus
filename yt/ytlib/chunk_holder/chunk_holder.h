#pragma once

#include "common.h"
#include "chunk_holder.pb.h"
#include "block_cache.h"
#include "chunk_holder_rpc.h"

#include "../rpc/service.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TChunkHolder
    : public NRpc::TServiceBase
{
public:
    typedef TIntrusivePtr<TChunkHolder> TPtr;
    typedef TChunkHolderConfig TConfig;

    TChunkHolder(const TConfig& config);
    ~TChunkHolder();

private:
    typedef TChunkHolderProxy TProxy;
    typedef NRpc::TTypedServiceException<TProxy::EErrorCode> TServiceException;
    typedef TChunkSession TSession;

    TConfig Config;
    TIntrusivePtr<TBlockStore> Store;
    NRpc::TChannelCache ChannelCache;

    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, StartChunk);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, FinishChunk);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, PutBlocks);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, SendBlocks);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, FlushBlocks);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, GetBlocks);

    void RegisterMethods();

    void VerifyNoSession(const TChunkId& chunkId);
    void VerifyNoChunk(const TChunkId& chunkId);
    TSession::TPtr GetSession(const TChunkId& chunkId);

    void OnGotBlock(
        TCachedBlock::TPtr block,
        int blockIndex,
        TCtxGetBlocks::TPtr context);
    void OnGotAllBlocks(TCtxGetBlocks::TPtr context);

    void OnFlushedBlocks(
        TVoid,
        TCtxFlushBlocks::TPtr context);

    void OnSentBlocks(
        TProxy::TRspPutBlocks::TPtr putResponse, 
        TCtxSendBlocks::TPtr context);

};

////////////////////////////////////////////////////////////////////////////////

}
