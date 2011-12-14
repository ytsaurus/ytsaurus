#pragma once

#include "common.h"
#include "chunk_holder_service_rpc.pb.h"
#include "chunk_holder_service_rpc.h"

#include "../rpc/server.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TChunkStore;
class TChunkCache;
class TReaderCache;
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
        TConfig* config,
        IInvoker* serviceInvoker,
        NRpc::IRpcServer* server,
        TChunkStore* chunkStore,
        TChunkCache* chunkcache,
        TReaderCache* readerCache,
        TBlockStore* blockStore,
        TSessionManager* sessionManager);
    ~TChunkHolderService();

private:
    typedef TChunkHolderService TThis;
    typedef TChunkHolderServiceProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    TConfig::TPtr Config;
    TIntrusivePtr<TChunkStore> ChunkStore;
    TIntrusivePtr<TChunkCache> ChunkCache;
    TIntrusivePtr<TReaderCache> ReaderCache;
    TIntrusivePtr<TBlockStore> BlockStore;
    TIntrusivePtr<TSessionManager> SessionManager;

    NRpc::TChannelCache ChannelCache;


    DECLARE_RPC_SERVICE_METHOD(NProto, StartChunk);
    DECLARE_RPC_SERVICE_METHOD(NProto, FinishChunk);
    DECLARE_RPC_SERVICE_METHOD(NProto, PutBlocks);
    DECLARE_RPC_SERVICE_METHOD(NProto, SendBlocks);
    DECLARE_RPC_SERVICE_METHOD(NProto, FlushBlock);
    DECLARE_RPC_SERVICE_METHOD(NProto, GetBlocks);
    DECLARE_RPC_SERVICE_METHOD(NProto, PingSession);
    DECLARE_RPC_SERVICE_METHOD(NProto, GetChunkInfo);
    DECLARE_RPC_SERVICE_METHOD(NProto, PrecacheChunk);

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
