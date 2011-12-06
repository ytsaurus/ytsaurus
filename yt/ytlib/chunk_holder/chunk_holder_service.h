#pragma once

#include "common.h"
#include "chunk_holder_service_rpc.pb.h"
#include "chunk_holder_service_rpc.h"

#include "../rpc/server.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TChunkStore;
class TChunk;
class TBlockStore;
class TSessionManager;
class TSession;
class TReplicator;

class TChunkHolderService
    : public NRpc::TServiceBase
{
public:
    typedef TIntrusivePtr<TChunkHolderService> TPtr;
    typedef TChunkHolderConfig TConfig;

    //! Creates an instance.
    TChunkHolderService(
        const TConfig& config,
        IInvoker* serviceInvoker,
        NRpc::IRpcServer* server,
        TChunkStore* chunkStore,
        TBlockStore* blockStore,
        TSessionManager* sessionManager);
    ~TChunkHolderService();

private:
    typedef TChunkHolderService TThis;
    typedef TChunkHolderServiceProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    //! Configuration.
    TConfig Config;

    //! Caches blocks.
    TIntrusivePtr<TBlockStore> BlockStore;

    //! Manages complete chunks.
    TIntrusivePtr<TChunkStore> ChunkStore;

    //! Manages currently active upload sessions.
    TIntrusivePtr<TSessionManager> SessionManager;

    //! Caches channels that are used for sending blocks to other holders.
    NRpc::TChannelCache ChannelCache;


    DECLARE_RPC_SERVICE_METHOD(NProto, StartChunk);
    DECLARE_RPC_SERVICE_METHOD(NProto, FinishChunk);
    DECLARE_RPC_SERVICE_METHOD(NProto, PutBlocks);
    DECLARE_RPC_SERVICE_METHOD(NProto, SendBlocks);
    DECLARE_RPC_SERVICE_METHOD(NProto, FlushBlock);
    DECLARE_RPC_SERVICE_METHOD(NProto, GetBlocks);
    DECLARE_RPC_SERVICE_METHOD(NProto, PingSession);
    DECLARE_RPC_SERVICE_METHOD(NProto, GetChunkInfo);

    void ValidateNoSession(const NChunkClient::TChunkId& chunkId);
    void ValidateNoChunk(const NChunkClient::TChunkId& chunkId);

    TIntrusivePtr<TSession> GetSession(const NChunkClient::TChunkId& chunkId);
    TIntrusivePtr<TChunk> GetChunk(const NChunkClient::TChunkId& chunkId);

    void OnFinishedChunk(
        TVoid,
        TCtxFinishChunk::TPtr context);

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
