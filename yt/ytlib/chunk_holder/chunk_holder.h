#pragma once

#include "common.h"
#include "chunk_holder.pb.h"
#include "block_store.h"
#include "session.h"
#include "chunk_store.h"
#include "chunk_holder_rpc.h"
#include "master_connector.h"
#include "replicator.h"

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

    //! Creates an instance.
    TChunkHolder(
        const TConfig& config,
        IInvoker::TPtr serviceInvoker,
        NRpc::TServer::TPtr server);
    ~TChunkHolder();

private:
    typedef TChunkHolder TThis;
    typedef TChunkHolderProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

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

    //! Manages chunk replication.
    TReplicator::TPtr Replicator;

    //! Manages connection between chunk holder and master.
    TMasterConnector::TPtr MasterConnector;

    RPC_SERVICE_METHOD_DECL(NProto, StartChunk);
    RPC_SERVICE_METHOD_DECL(NProto, FinishChunk);
    RPC_SERVICE_METHOD_DECL(NProto, PutBlocks);
    RPC_SERVICE_METHOD_DECL(NProto, SendBlocks);
    RPC_SERVICE_METHOD_DECL(NProto, FlushBlock);
    RPC_SERVICE_METHOD_DECL(NProto, GetBlocks);
    RPC_SERVICE_METHOD_DECL(NProto, PingSession);

    void ValidateNoSession(const TChunkId& chunkId);
    void ValidateNoChunk(const TChunkId& chunkId);

    TSession::TPtr GetSession(const TChunkId& chunkId);

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
