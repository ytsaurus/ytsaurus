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
    NRpc::TServer::TPtr server)
    : NRpc::TServiceBase(
        TProxy::GetServiceName(),
        Logger.GetCategory())
    , Config(config)
{
    ChunkStore = new TChunkStore(Config);

    BlockStore = new TBlockStore(Config, ChunkStore);

    SessionManager = new TSessionManager(
        Config,
        BlockStore,
        ChunkStore,
        server->GetInvoker());

    if (!Config.MasterAddress.Empty()) {
        MasterConnector = new TMasterConnector(
            Config,
            ChunkStore,
            server->GetInvoker());
        MasterConnector->Initialize();
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
    RPC_REGISTER_METHOD(TChunkHolder, StartChunk);
    RPC_REGISTER_METHOD(TChunkHolder, FinishChunk);
    RPC_REGISTER_METHOD(TChunkHolder, PutBlocks);
    RPC_REGISTER_METHOD(TChunkHolder, SendBlocks);
    RPC_REGISTER_METHOD(TChunkHolder, FlushBlock);
    RPC_REGISTER_METHOD(TChunkHolder, GetBlocks);
}

void TChunkHolder::VerifyNoSession(const TChunkId& chunkId)
{
    if (~SessionManager->FindSession(chunkId) != NULL) {
        ythrow TServiceException(TProxy::EErrorCode::NoSuchSession) <<
            Sprintf("session %s already exists",
                ~chunkId.ToString());
    }
}

void TChunkHolder::VerifyNoChunk(const TChunkId& chunkId)
{
    if (~ChunkStore->FindChunk(chunkId) != NULL) {
        ythrow TServiceException(TProxy::EErrorCode::ChunkAlreadyExists) <<
            Sprintf("chunk %s already exists", ~chunkId.ToString());
    }
}

TSession::TPtr TChunkHolder::GetSession(const TChunkId& chunkId)
{
    TSession::TPtr session = SessionManager->FindSession(chunkId);
    if (~session == NULL) {
        ythrow TServiceException(TProxy::EErrorCode::NoSuchSession) <<
            Sprintf("session %s is invalid or expired",
                ~chunkId.ToString());
    }
    return session;
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TChunkHolder, StartChunk)
{
    UNUSED(response);

    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());
    int windowSize = request->GetWindowSize();

    context->SetRequestInfo("ChunkId: %s, WindowSize: %d",
        ~chunkId.ToString(),
        windowSize);

    VerifyNoSession(chunkId);
    VerifyNoChunk(chunkId);

    SessionManager->StartSession(chunkId, windowSize);

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, FinishChunk)
{
    UNUSED(response);

    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());
    
    context->SetRequestInfo("ChunkId: %s",
        ~chunkId.ToString());

    TSession::TPtr session = GetSession(chunkId);

    SessionManager->FinishSession(session)->Subscribe(FromMethod(
        &TChunkHolder::OnFinishedChunk,
        TPtr(this),
        chunkId,
        context));
}

void TChunkHolder::OnFinishedChunk(
    TVoid,
    const TChunkId& chunkId,
    TCtxFinishChunk::TPtr context)
{
    if (~MasterConnector != NULL) {
        TChunk::TPtr chunk = ChunkStore->FindChunk(chunkId);
        YASSERT(~chunk != NULL);
        MasterConnector->RegisterAddedChunk(chunk);
    }
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, PutBlocks)
{
    UNUSED(response);

    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());
    i32 startBlockIndex = request->GetStartBlockIndex();
    TBlockOffset startOffset = request->GetStartOffset();

    context->SetRequestInfo("ChunkId: %s, BlockCount: %d, StartBlockIndex: %d, StartOffset: %" PRId64,
        ~chunkId.ToString(),
        request->Attachments().ysize(),
        startBlockIndex,
        startOffset);

    TSession::TPtr session = GetSession(chunkId);

    i32 blockIndex = startBlockIndex;
    TBlockOffset offset = startOffset;
    for (yvector<TSharedRef>::iterator it = request->Attachments().begin();
         it != request->Attachments().end();
         ++it)
    {
        // Make a copy of the attachment to enable separate caching
        // of blocks arriving within a single RPC request.
        TBlob data = it->ToBlob();
        TBlockId blockId(chunkId, offset);
        // Increment offset before PutBlocks, hence data will be swapped out
        offset += data.ysize();         
        session->PutBlock(blockIndex, blockId, data);
        ++blockIndex;
    }
    
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, SendBlocks)
{
    UNUSED(response);

    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());
    i32 startBlockIndex = request->GetStartBlockIndex();
    i32 endBlockIndex = request->GetEndBlockIndex();
    Stroka destination = request->GetDestination();

    context->SetRequestInfo("ChunkId: %s, StartBlockIndex: %d, EndBlockIndex: %d, Destination: %s",
        ~chunkId.ToString(),
        startBlockIndex,
        endBlockIndex,
        ~destination);

    // TODO: (first, end) vs (first, count)?

    TSession::TPtr session = GetSession(chunkId);

    TCachedBlock::TPtr startBlock = session->GetBlock(startBlockIndex);

    TProxy::TReqPutBlocks::TPtr putRequest = TProxy(ChannelCache.GetChannel(destination)).PutBlocks();
    putRequest->SetChunkId(chunkId.ToProto());
    putRequest->SetStartBlockIndex(startBlockIndex);
    putRequest->SetStartOffset(startBlock->GetKey().Offset);
    
    for (int blockIndex = startBlockIndex; blockIndex <= endBlockIndex; ++blockIndex) {
        TCachedBlock::TPtr block = session->GetBlock(blockIndex);
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

    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());
    int blockCount = static_cast<int>(request->BlocksSize());
    
    context->SetRequestInfo("ChunkId: %s, BlockCount: %d",
        ~chunkId.ToString(),
        blockCount);

    response->Attachments().yresize(blockCount);

    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter();

    for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
        const NProto::TBlockInfo& info = request->GetBlocks(blockIndex);

        LOG_DEBUG("GetBlocks: (Index: %d, Offset: %" PRId64 ", Size: %d)",
            blockIndex,
            info.GetOffset(),
            info.GetSize());

        TBlockId blockId(chunkId, info.GetOffset());

        awaiter->Await(
            BlockStore->FindBlock(blockId, info.GetSize()),
            FromMethod(
                &TChunkHolder::OnGotBlock,
                TPtr(this),
                blockIndex,
                context));
    }

    awaiter->Complete(FromMethod(
        &TChunkHolder::OnGotAllBlocks,
        TPtr(this),
        context));
}

void TChunkHolder::OnGotBlock(
    TCachedBlock::TPtr block,
    int blockIndex,
    TCtxGetBlocks::TPtr context)
{
    YASSERT(~block != NULL);
    context->Response().Attachments().at(blockIndex) = block->GetData();
}

void TChunkHolder::OnGotAllBlocks(TCtxGetBlocks::TPtr context)
{
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, FlushBlock)
{
    UNUSED(response);

    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());
    int blockIndex = request->GetBlockIndex();

    context->SetRequestInfo("ChunkId: %s, BlockIndex: %d",
        ~chunkId.ToString(),
        blockIndex);

    TSession::TPtr session = GetSession(chunkId);

    session->FlushBlock(blockIndex)->Subscribe(FromMethod(
        &TChunkHolder::OnFlushedBlock,
        TPtr(this),
        context));
}

void TChunkHolder::OnFlushedBlock(TVoid, TCtxFlushBlock::TPtr context)
{
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
