#include "stdafx.h"
#include "chunk_holder_service.h"
#include "chunk_store.h"
#include "block_store.h"
#include "session_manager.h"

#include "../misc/serialize.h"
#include "../actions/action_util.h"
#include "../actions/parallel_awaiter.h"

namespace NYT {
namespace NChunkHolder {

using namespace NRpc;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkHolderService::TChunkHolderService(
    const TConfig& config,
    IInvoker* serviceInvoker,
    NRpc::IRpcServer* server,
    TChunkStore* chunkStore,
    TBlockStore* blockStore,
    TSessionManager* sessionManager)
    : NRpc::TServiceBase(
        serviceInvoker,
        TProxy::GetServiceName(),
        Logger.GetCategory())
    , Config(config)
    , ChunkStore(chunkStore)
    , BlockStore(blockStore)
    , SessionManager(sessionManager)
{
    YASSERT(server != NULL);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlock));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkInfo));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PrecacheChunk));
    
    server->RegisterService(this);
}

// Do not remove this!
// Required for TInstusivePtr with an incomplete type.
TChunkHolderService::~TChunkHolderService() 
{ }

void TChunkHolderService::ValidateNoSession(const TChunkId& chunkId)
{
    if (~SessionManager->FindSession(chunkId) != NULL) {
        ythrow TServiceException(EErrorCode::SessionAlreadyExists) <<
            Sprintf("Session %s already exists", ~chunkId.ToString());
    }
}

void TChunkHolderService::ValidateNoChunk(const TChunkId& chunkId)
{
    if (~ChunkStore->FindChunk(chunkId) != NULL) {
        ythrow TServiceException(EErrorCode::ChunkAlreadyExists) <<
            Sprintf("Chunk %s already exists", ~chunkId.ToString());
    }
}

TSession::TPtr TChunkHolderService::GetSession(const TChunkId& chunkId)
{
    auto session = SessionManager->FindSession(chunkId);
    if (~session == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchSession) <<
            Sprintf("Session is invalid or expired (ChunkId: %s)", ~chunkId.ToString());
    }
    return session;
}

TChunk::TPtr TChunkHolderService::GetChunk(const TChunkId& chunkId)
{
    auto chunk = ChunkStore->FindChunk(chunkId);
    if (~chunk == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchSession) <<
            Sprintf("Chunk it not found (ChunkId: %s)", ~chunkId.ToString());
    }
    return chunk;
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, StartChunk)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunkid());
    int windowSize = request->windowsize();

    context->SetRequestInfo("ChunkId: %s, WindowSize: %d",
        ~chunkId.ToString(),
        windowSize);

    ValidateNoSession(chunkId);
    ValidateNoChunk(chunkId);

    SessionManager->StartSession(chunkId, windowSize);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, FinishChunk)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunkid());
    auto& attributes = request->attributes();
    
    context->SetRequestInfo("ChunkId: %s",
        ~chunkId.ToString());

    auto session = GetSession(chunkId);

    SessionManager
        ->FinishSession(session, attributes)
        ->Subscribe(FromFunctor([=] (TVoid)
            {
                context->Reply();
            }));
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, PutBlocks)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunkid());
    i32 startBlockIndex = request->startblockindex();

    context->SetRequestInfo("ChunkId: %s, StartBlockIndex: %d, BlockCount: %d",
        ~chunkId.ToString(),
        startBlockIndex,
        request->Attachments().ysize());

    auto session = GetSession(chunkId);

    i32 blockIndex = startBlockIndex;
    FOREACH(const auto& attachment, request->Attachments()) {
        // Make a copy of the attachment to enable separate caching
        // of blocks arriving within a single RPC request.
        auto data = attachment.ToBlob();
        session->PutBlock(blockIndex, MoveRV(data));
        ++blockIndex;
    }
    
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, SendBlocks)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunkid());
    i32 startBlockIndex = request->startblockindex();
    i32 blockCount = request->blockcount();
    Stroka address = request->address();

    context->SetRequestInfo("ChunkId: %s, StartBlockIndex: %d, BlockCount: %d, Address: %s",
        ~chunkId.ToString(),
        startBlockIndex,
        blockCount,
        ~address);

    auto session = GetSession(chunkId);

    auto startBlock = session->GetBlock(startBlockIndex);

    TProxy proxy(~ChannelCache.GetChannel(address));
    proxy.SetTimeout(Config.MasterRpcTimeout);
    auto putRequest = proxy.PutBlocks();
    putRequest->set_chunkid(chunkId.ToProto());
    putRequest->set_startblockindex(startBlockIndex);
    
    for (int blockIndex = startBlockIndex; blockIndex < startBlockIndex + blockCount; ++blockIndex) {
        auto block = session->GetBlock(blockIndex);
        putRequest->Attachments().push_back(block->GetData());
    }

    putRequest->Invoke()->Subscribe(FromFunctor([=] (TProxy::TRspPutBlocks::TPtr putResponse)
        {
            if (putResponse->IsOK()) {
                context->Reply();
            } else {
                Stroka message = Sprintf("SendBlocks: Cannot put blocks on the remote chunk holder (Address: %s)\n%s",
                    ~address,
                    ~putResponse->GetError().ToString());

                LOG_WARNING("%s", ~message);
                context->Reply(TChunkHolderServiceProxy::EErrorCode::RemoteCallFailed, message);
            }
        }));
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, GetBlocks)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunkid());
    int blockCount = static_cast<int>(request->blockindexes_size());
    
    context->SetRequestInfo("ChunkId: %s, BlockCount: %d",
        ~chunkId.ToString(),
        blockCount);

    response->Attachments().resize(blockCount);

    auto awaiter = New<TParallelAwaiter>();

    for (int index = 0; index < blockCount; ++index) {
        i32 blockIndex = request->blockindexes(index);

        LOG_DEBUG("GetBlocks: (Index: %d)", blockIndex);

        TBlockId blockId(chunkId, blockIndex);
        awaiter->Await(
            BlockStore->FindBlock(blockId),
            FromFunctor([=] (TCachedBlock::TPtr block)
                {
                    if (~block == NULL) {
                        awaiter->Cancel();
                        context->Reply(
                            TChunkHolderServiceProxy::EErrorCode::NoSuchBlock,
                            Sprintf("Block not found (ChunkId: %s, Index: %d)",
                                ~chunkId.ToString(),
                                blockIndex));
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

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, FlushBlock)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunkid());
    int blockIndex = request->blockindex();

    context->SetRequestInfo("ChunkId: %s, BlockIndex: %d",
        ~chunkId.ToString(),
        blockIndex);

    auto session = GetSession(chunkId);

    session->FlushBlock(blockIndex)->Subscribe(FromFunctor([=] (TVoid)
        {
            context->Reply();
        }));
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, PingSession)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunkid());
    auto session = GetSession(chunkId);
    session->RenewLease();

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, GetChunkInfo)
{
    auto chunkId = TChunkId::FromProto(request->chunkid());

    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    auto chunk = GetChunk(chunkId);

    *response->mutable_chunkinfo() = chunk->Info();

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, PrecacheChunk)
{
    auto chunkId = TChunkId::FromProto(request->chunkid());
    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
