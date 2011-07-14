#pragma once

#include "common.h"
#include "chunk_holder.pb.h"
#include "block_store.h"
#include "session.h"
#include "chunk_store.h"
#include "chunk_holder_rpc.h"

#include "../rpc/service.h"
#include "../rpc/server.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TChunkHolder
    : public NRpc::TServiceBase
{
public:
    typedef TIntrusivePtr<TChunkHolder> TPtr;
    typedef TChunkHolderConfig TConfig;

    TChunkHolder(
        const TConfig& config,
        NRpc::TServer* server);
    ~TChunkHolder();

private:
    typedef TChunkHolderProxy TProxy;
    typedef NRpc::TTypedServiceException<TProxy::EErrorCode> TServiceException;

    TConfig Config;
    TIntrusivePtr<TBlockStore> BlockStore;
    TIntrusivePtr<TChunkStore> ChunkStore;
    TIntrusivePtr<TSessionManager> SessionManager;
    NRpc::TChannelCache ChannelCache;

    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, StartChunk);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, FinishChunk);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, PutBlocks);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, SendBlocks);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, FlushBlock);
    RPC_SERVICE_METHOD_DECL(NRpcChunkHolder, GetBlocks);

    void RegisterMethods();

    void VerifyNoSession(const TChunkId& chunkId);
    void VerifyNoChunk(const TChunkId& chunkId);
    TSession::TPtr GetSession(const TChunkId& chunkId);

    void OnFinishedChunk(
        TVoid,
        TCtxFinishChunk::TPtr context);

    void OnGotBlock(
        TCachedBlock::TPtr block,
        int blockIndex,
        TCtxGetBlocks::TPtr context);
    void OnGotAllBlocks(TCtxGetBlocks::TPtr context);

    void OnSentBlocks(
        TProxy::TRspPutBlocks::TPtr putResponse, 
        TCtxSendBlocks::TPtr context);

    void OnFlushedBlock(
        TVoid,
        TCtxFlushBlock::TPtr context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
