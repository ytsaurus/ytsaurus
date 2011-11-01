#include "stdafx.h"
#include "chunk_holder.h"

#include "../misc/serialize.h"
#include "../actions/action_util.h"
#include "../actions/parallel_awaiter.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkHolder::TChunkHolder(
    const TConfig& config,
    IInvoker::TPtr serviceInvoker,
    NRpc::TServer::TPtr server)
    : NRpc::TServiceBase(
        serviceInvoker,
        TProxy::GetServiceName(),
        Logger.GetCategory())
    , Config(config)
{
    YASSERT(~serviceInvoker != NULL);
    YASSERT(~server != NULL);

    ChunkStore = New<TChunkStore>(Config);
    BlockStore = New<TBlockStore>(Config, ChunkStore);

    SessionManager = New<TSessionManager>(
        Config,
        BlockStore,
        ChunkStore,
        serviceInvoker);

    Replicator = New<TReplicator>(
        ChunkStore,
        BlockStore,
        serviceInvoker);

    if (!Config.Masters.Addresses.empty()) {
        MasterConnector = New<TMasterConnector>(
            Config,
            ChunkStore,
            SessionManager,
            Replicator,
            serviceInvoker);
    } else {
        LOG_INFO("Running in standalone mode");
    }

    RegisterMethods();
    server->RegisterService(this);
}

// Do not remove this!
// Required for TInstusivePtr with an incomplete type.
TChunkHolder::~TChunkHolder() 
{ }

void TChunkHolder::RegisterMethods()
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlock));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
}

void TChunkHolder::ValidateNoSession(const TChunkId& chunkId)
{
    if (~SessionManager->FindSession(chunkId) != NULL) {
        ythrow TServiceException(EErrorCode::SessionAlreadyExists) <<
            Sprintf("Session %s already exists",
                ~chunkId.ToString());
    }
}

void TChunkHolder::ValidateNoChunk(const TChunkId& chunkId)
{
    if (~ChunkStore->FindChunk(chunkId) != NULL) {
        ythrow TServiceException(EErrorCode::ChunkAlreadyExists) <<
            Sprintf("Chunk %s already exists", ~chunkId.ToString());
    }
}

TSession::TPtr TChunkHolder::GetSession(const TChunkId& chunkId)
{
    auto session = SessionManager->FindSession(chunkId);
    if (~session == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchSession) <<
            Sprintf("Session %s is invalid or expired",
                ~chunkId.ToString());
    }
    return session;
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TChunkHolder, StartChunk)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->GetChunkId());
    int windowSize = request->GetWindowSize();

    context->SetRequestInfo("ChunkId: %s, WindowSize: %d",
        ~chunkId.ToString(),
        windowSize);

    ValidateNoSession(chunkId);
    ValidateNoChunk(chunkId);

    SessionManager->StartSession(chunkId, windowSize);

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, FinishChunk)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->GetChunkId());
    
    context->SetRequestInfo("ChunkId: %s",
        ~chunkId.ToString());

    auto session = GetSession(chunkId);

    SessionManager->FinishSession(session)->Subscribe(FromMethod(
        &TChunkHolder::OnFinishedChunk,
        TPtr(this),
        context));
}

void TChunkHolder::OnFinishedChunk(
    TVoid,
    TCtxFinishChunk::TPtr context)
{
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, PutBlocks)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->GetChunkId());
    i32 startBlockIndex = request->GetStartBlockIndex();

    context->SetRequestInfo("ChunkId: %s, StartBlockIndex: %d, BlockCount: %d",
        ~chunkId.ToString(),
        startBlockIndex,
        request->Attachments().ysize());

    auto session = GetSession(chunkId);

    i32 blockIndex = startBlockIndex;
    FOREACH(const auto& it, request->Attachments()) {
        // Make a copy of the attachment to enable separate caching
        // of blocks arriving within a single RPC request.
        TBlob data = it.ToBlob();
        session->PutBlock(blockIndex, TSharedRef(data));
        ++blockIndex;
    }
    
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, SendBlocks)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->GetChunkId());
    i32 startBlockIndex = request->GetStartBlockIndex();
    i32 blockCount = request->GetBlockCount();
    Stroka address = request->GetAddress();

    context->SetRequestInfo("ChunkId: %s, StartBlockIndex: %d, BlockCount: %d, Address: %s",
        ~chunkId.ToString(),
        startBlockIndex,
        blockCount,
        ~address);

    auto session = GetSession(chunkId);

    TCachedBlock::TPtr startBlock = session->GetBlock(startBlockIndex);

    TProxy proxy(ChannelCache.GetChannel(address));
    auto putRequest = proxy.PutBlocks();
    putRequest->SetChunkId(chunkId.ToProto());
    putRequest->SetStartBlockIndex(startBlockIndex);
    
    for (int blockIndex = startBlockIndex; blockIndex < startBlockIndex + blockCount; ++blockIndex) {
        auto block = session->GetBlock(blockIndex);
        putRequest->Attachments().push_back(block->GetData());
    }

    putRequest->Invoke(Config.RpcTimeout)->Subscribe(FromMethod(
        &TChunkHolder::OnSentBlocks,
        TPtr(this),
        context));
}

void TChunkHolder::OnSentBlocks(
    TProxy::TRspPutBlocks::TPtr putResponse,
    TCtxSendBlocks::TPtr context)
{
    if (putResponse->IsOK()) {
        context->Reply();
    } else {
        LOG_WARNING("SendBlocks: error %s putting blocks on the remote chunk holder",
            ~putResponse->GetErrorCode().ToString());
        context->Reply(TProxy::EErrorCode::RemoteCallFailed);
    }
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, GetBlocks)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->GetChunkId());
    int blockCount = static_cast<int>(request->BlockIndexesSize());
    
    context->SetRequestInfo("ChunkId: %s, BlockCount: %d",
        ~chunkId.ToString(),
        blockCount);

    response->Attachments().yresize(blockCount);

    auto awaiter = New<TParallelAwaiter>();

    for (int index = 0; index < blockCount; ++index) {
        i32 blockIndex = request->GetBlockIndexes(index);

        LOG_DEBUG("GetBlocks: (Index: %d)", blockIndex);

        TBlockId blockId(chunkId, blockIndex);
        awaiter->Await(
            BlockStore->FindBlock(blockId),
            FromFunctor([=] (TCachedBlock::TPtr block)
                {
                    if (~block == NULL) {
                        awaiter->Cancel();
                        context->Reply(TChunkHolderProxy::EErrorCode::NoSuchBlock);
                    } else {
                        context->Response().Attachments()[index] = block->GetData();
                    }
                }));
    }

    awaiter->Complete(FromFunctor([=] ()
        {
            if (!context->IsReplied()) {
                context->Reply();
            }
        }));
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, FlushBlock)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->GetChunkId());
    int blockIndex = request->GetBlockIndex();

    context->SetRequestInfo("ChunkId: %s, BlockIndex: %d",
        ~chunkId.ToString(),
        blockIndex);

    auto session = GetSession(chunkId);

    session->FlushBlock(blockIndex)->Subscribe(FromMethod(
        &TChunkHolder::OnFlushedBlock,
        TPtr(this),
        context));
}

void TChunkHolder::OnFlushedBlock(TVoid, TCtxFlushBlock::TPtr context)
{
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, PingSession)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->GetChunkId());
    auto session = GetSession(chunkId);
    session->RenewLease();

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
