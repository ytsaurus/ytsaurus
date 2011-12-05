#include "stdafx.h"
#include "chunk_holder.h"

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

TChunkHolder::TChunkHolder(
    const TConfig& config,
    IInvoker* serviceInvoker,
    NRpc::IRpcServer* server)
    : NRpc::TServiceBase(
        serviceInvoker,
        TProxy::GetServiceName(),
        Logger.GetCategory())
    , Config(config)
{
    YASSERT(serviceInvoker != NULL);
    YASSERT(server != NULL);

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

    RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlock));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkInfo));
    
    server->RegisterService(this);
}

// Do not remove this!
// Required for TInstusivePtr with an incomplete type.
TChunkHolder::~TChunkHolder() 
{ }

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
            Sprintf("Session is invalid or expired (ChunkId: %s)", ~chunkId.ToString());
    }
    return session;
}

TChunk::TPtr TChunkHolder::GetChunk(const TChunkId& chunkId)
{
    auto chunk = ChunkStore->FindChunk(chunkId);
    if (~chunk == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchSession) <<
            Sprintf("Chunk it not found (ChunkId: %s)", ~chunkId.ToString());
    }
    return chunk;
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TChunkHolder, StartChunk)
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

DEFINE_RPC_SERVICE_METHOD(TChunkHolder, FinishChunk)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->GetChunkId());
    auto& attributes = request->GetAttributes();
    
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

DEFINE_RPC_SERVICE_METHOD(TChunkHolder, PutBlocks)
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
    FOREACH(const auto& attachment, request->Attachments()) {
        // Make a copy of the attachment to enable separate caching
        // of blocks arriving within a single RPC request.
        auto data = attachment.ToBlob();
        session->PutBlock(blockIndex, MoveRV(data));
        ++blockIndex;
    }
    
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolder, SendBlocks)
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

    auto startBlock = session->GetBlock(startBlockIndex);

    TProxy proxy(~ChannelCache.GetChannel(address));
    proxy.SetTimeout(Config.RpcTimeout);
    auto putRequest = proxy.PutBlocks();
    putRequest->SetChunkId(chunkId.ToProto());
    putRequest->SetStartBlockIndex(startBlockIndex);
    
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
                context->Reply(TChunkHolderProxy::EErrorCode::RemoteCallFailed, message);
            }
        }));
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolder, GetBlocks)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->GetChunkId());
    int blockCount = static_cast<int>(request->BlockIndexesSize());
    
    context->SetRequestInfo("ChunkId: %s, BlockCount: %d",
        ~chunkId.ToString(),
        blockCount);

    response->Attachments().resize(blockCount);

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
                        context->Reply(
                            TChunkHolderProxy::EErrorCode::NoSuchBlock,
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

DEFINE_RPC_SERVICE_METHOD(TChunkHolder, FlushBlock)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->GetChunkId());
    int blockIndex = request->GetBlockIndex();

    context->SetRequestInfo("ChunkId: %s, BlockIndex: %d",
        ~chunkId.ToString(),
        blockIndex);

    auto session = GetSession(chunkId);

    session->FlushBlock(blockIndex)->Subscribe(FromFunctor([=] (TVoid)
        {
            context->Reply();
        }));
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolder, PingSession)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->GetChunkId());
    auto session = GetSession(chunkId);
    session->RenewLease();

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolder, GetChunkInfo)
{
    auto chunkId = TChunkId::FromProto(request->GetChunkId());

    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    auto chunk = GetChunk(chunkId);

    *response->MutableChunkInfo() = chunk->Info();

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
