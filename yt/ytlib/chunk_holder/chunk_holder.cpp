#include "chunk_holder.h"

#include "../misc/string.h"
#include "../misc/serialize.h"
#include "../actions/action_util.h"
#include "../actions/parallel_awaiter.h"

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkHolder::TChunkHolder(const TConfig& config)
    : NRpc::TServiceBase(TProxy::GetServiceName(), Logger.GetCategory())
    , Config(config)
    , Store(new TBlockStore())
{
    RegisterMethods();
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
    RPC_REGISTER_METHOD(TChunkHolder, FlushBlocks);
    RPC_REGISTER_METHOD(TChunkHolder, SendBlocks);
    RPC_REGISTER_METHOD(TChunkHolder, GetBlocks);
}

void TChunkHolder::VerifyNoSession(const TChunkId& chunkId)
{
    if (~Store->FindSession(chunkId) != NULL) {
        ythrow TServiceException(TProxy::EErrorCode::NoSuchSession) <<
            Sprintf("session %s already exists",
                ~StringFromGuid(chunkId));
    }
}

void TChunkHolder::VerifyNoChunk(const TChunkId& chunkId)
{
    //if (LocationMap.find(chunkId) != LocationMap.end())
    //    ythrow TServiceException() << "Chunk " << StringFromGuid(chunkId) << " already exists";
}

TChunkHolder::TSession::TPtr TChunkHolder::GetSession(const TChunkId& chunkId)
{
    TSession::TPtr session = Store->FindSession(chunkId);
    if (~session == NULL) {
        ythrow TServiceException(TProxy::EErrorCode::NoSuchSession) <<
            Sprintf("session %s is invalid or expired",
                ~StringFromGuid(chunkId));
    }
    return session;
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TChunkHolder, StartChunk)
{
    UNUSED(response);

    TChunkId chunkId = GuidFromProtoGuid(request->GetChunkId());

    context->SetRequestInfo("ChunkId: %s",
        ~StringFromGuid(chunkId));

    VerifyNoSession(chunkId);
    VerifyNoChunk(chunkId);

    TSession::TPtr session = Store->StartSession(chunkId);

    context->SetResponseInfo("Location: %d",
        session->GetLocation());

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, FinishChunk)
{
    UNUSED(response);

    TChunkId chunkId = GuidFromProtoGuid(request->GetChunkId());
    
    context->SetRequestInfo("ChunkId: %s",
        ~StringFromGuid(chunkId));

    TSession::TPtr session = GetSession(chunkId);

    Store->FinishSession(session);

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, PutBlocks)
{
    UNUSED(response);

    TChunkId chunkId = GuidFromProtoGuid(request->GetChunkId());
    i32 startBlockIndex = request->GetStartBlockIndex();
    TBlockOffset startOffset = request->GetStartOffset();

    context->SetRequestInfo("ChunkId: %s, BlockCount: %d, StartBlockIndex: %s, StartOffset: %" PRId64,
        ~StringFromGuid(chunkId),
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
        session->PutBlock(blockIndex, blockId, data);
        ++blockIndex;
        offset += data.ysize();
    }
    
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, SendBlocks)
{
    UNUSED(response);

    TChunkId chunkId = GuidFromProtoGuid(request->GetChunkId());
    i32 startBlockIndex = request->GetStartBlockIndex();
    i32 endBlockIndex = request->GetEndBlockIndex();
    Stroka destination = request->GetDestination();

    context->SetRequestInfo("ChunkId: %s, StartBlockIndex: %d, EndBlockIndex: %d, Destination: %s",
        ~StringFromGuid(chunkId),
        startBlockIndex,
        endBlockIndex,
        ~destination);

    // TODO: (first, end) vs (first, count)?

    TSession::TPtr session = GetSession(chunkId);

    TCachedBlock::TPtr startBlock = session->GetBlock(startBlockIndex);

    TProxy::TReqPutBlocks::TPtr putRequest = TProxy(ChannelCache.GetChannel(destination)).PutBlocks();
    putRequest->SetChunkId(ProtoGuidFromGuid(chunkId));
    putRequest->SetStartBlockIndex(startBlockIndex);
    putRequest->SetStartOffset(startBlock->GetKey().Offset);
    
    for (int blockIndex = startBlockIndex; blockIndex <= endBlockIndex; ++blockIndex) {
        TCachedBlock::TPtr block = session->GetBlock(blockIndex);
        putRequest->Attachments().push_back(block->GetData());
    }

    // TODO: timeout
    putRequest->Invoke()->Subscribe(FromMethod(
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

RPC_SERVICE_METHOD_IMPL(TChunkHolder, FlushBlocks)
{
    UNUSED(response);

    TChunkId chunkId = GuidFromProtoGuid(request->GetChunkId());
    i32 startBlockIndex = request->GetStartBlockIndex();
    i32 endBlockIndex = request->GetStartBlockIndex();
    context->SetRequestInfo("ChunkId: %s, StartBlockIndex: %d, EndBlockIndex: %d",
        ~StringFromGuid(chunkId),
        startBlockIndex,
        endBlockIndex);

    // TODO: (first, end) vs (first, count)?

    TSession::TPtr session = GetSession(chunkId);
    session->FlushBlocks(startBlockIndex, endBlockIndex - startBlockIndex + 1)->Subscribe(FromMethod(
        &TChunkHolder::OnFlushedBlocks,
        TPtr(this),
        context));
}

void TChunkHolder::OnFlushedBlocks(
    TVoid,
    TCtxFlushBlocks::TPtr context)
{
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, GetBlocks)
{
    UNUSED(response);

    TChunkId chunkId = GuidFromProtoGuid(request->GetChunkId());
    int blockCount = static_cast<int>(request->BlocksSize());
    
    context->SetRequestInfo("ChunkId: %s, BlockCount: %d",
        ~StringFromGuid(chunkId),
        blockCount);

    response->Attachments().yresize(blockCount);

    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter();

    for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
        const NRpcChunkHolder::TBlockInfo& info = request->GetBlocks(blockIndex);

        LOG_DEBUG("GetBlocks: (Index: %d, Offset: %" PRId64 ", Size: %d)",
            ~StringFromGuid(chunkId),
            info.GetOffset(),
            info.GetSize());

        TBlockId blockId(chunkId, info.GetOffset());

        awaiter->Await(
            Store->GetBlock(blockId, info.GetSize()),
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
    context->Response().Attachments().at(blockIndex) = block->GetData();
}

void TChunkHolder::OnGotAllBlocks(TCtxGetBlocks::TPtr context)
{
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

}

