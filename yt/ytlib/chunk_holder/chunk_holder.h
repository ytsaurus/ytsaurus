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
        IInvoker* serviceInvoker,
        NRpc::IRpcServer* server);
    ~TChunkHolder();

private:
    typedef TChunkHolder TThis;
    typedef TChunkHolderProxy TProxy;
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

    //! Manages chunk replication.
    TReplicator::TPtr Replicator;

    //! Manages connection between chunk holder and master.
    TMasterConnector::TPtr MasterConnector;

    DECLARE_RPC_SERVICE_METHOD(NProto, StartChunk);
    DECLARE_RPC_SERVICE_METHOD(NProto, FinishChunk);
    DECLARE_RPC_SERVICE_METHOD(NProto, PutBlocks);
    DECLARE_RPC_SERVICE_METHOD(NProto, SendBlocks);
    DECLARE_RPC_SERVICE_METHOD(NProto, FlushBlock);
    DECLARE_RPC_SERVICE_METHOD(NProto, GetBlocks);
    DECLARE_RPC_SERVICE_METHOD(NProto, PingSession);

    void ValidateNoSession(const NChunkClient::TChunkId& chunkId);
    void ValidateNoChunk(const NChunkClient::TChunkId& chunkId);

    TSession::TPtr GetSession(const NChunkClient::TChunkId& chunkId);

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
