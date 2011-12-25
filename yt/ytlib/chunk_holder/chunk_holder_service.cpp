#include "stdafx.h"
#include "chunk_holder_service.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "block_store.h"
#include "session_manager.h"

#include "../misc/serialize.h"
#include "../misc/string.h"
#include "../actions/action_util.h"
#include "../actions/parallel_awaiter.h"

namespace NYT {
namespace NChunkHolder {

using namespace NRpc;
using namespace NChunkClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkHolderService::TChunkHolderService(
    TConfig* config,
    IInvoker* serviceInvoker,
    NBus::IBusServer* server,
    TChunkStore* chunkStore,
    TChunkCache* chunkCache,
    TReaderCache* readerCache,
    TBlockStore* blockStore,
    TSessionManager* sessionManager)
    : NRpc::TServiceBase(
        serviceInvoker,
        TProxy::GetServiceName(),
        Logger.GetCategory())
    , Config(config)
    , ServiceInvoker(serviceInvoker)
    , BusServer(server)
    , ChunkStore(chunkStore)
    , ChunkCache(chunkCache)
    , ReaderCache(readerCache)
    , BlockStore(blockStore)
    , SessionManager(sessionManager)
{
    YASSERT(server);
    YASSERT(chunkStore);
    YASSERT(chunkCache);
    YASSERT(readerCache);
    YASSERT(blockStore);
    YASSERT(sessionManager);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlock));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkInfo));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PrecacheChunk));
}

// Do not remove this!
// Required for TInstusivePtr with an incomplete type.
TChunkHolderService::~TChunkHolderService() 
{ }

void TChunkHolderService::ValidateNoSession(const TChunkId& chunkId)
{
    if (SessionManager->FindSession(chunkId)) {
        ythrow TServiceException(EErrorCode::SessionAlreadyExists) <<
            Sprintf("Session %s already exists", ~chunkId.ToString());
    }
}

void TChunkHolderService::ValidateNoChunk(const TChunkId& chunkId)
{
    if (ChunkStore->FindChunk(chunkId)) {
        ythrow TServiceException(EErrorCode::ChunkAlreadyExists) <<
            Sprintf("Chunk %s already exists", ~chunkId.ToString());
    }
}

TSession::TPtr TChunkHolderService::GetSession(const TChunkId& chunkId)
{
    auto session = SessionManager->FindSession(chunkId);
    if (!session) {
        ythrow TServiceException(EErrorCode::NoSuchSession) <<
            Sprintf("Session is invalid or expired (ChunkId: %s)", ~chunkId.ToString());
    }
    return session;
}

TChunk::TPtr TChunkHolderService::GetChunk(const TChunkId& chunkId)
{
    auto chunk = ChunkStore->FindChunk(chunkId);
    if (!chunk) {
        ythrow TServiceException(EErrorCode::NoSuchChunk) <<
            Sprintf("No such chunk (ChunkId: %s)", ~chunkId.ToString());
    }
    return chunk;
}

bool TChunkHolderService::CheckThrottling() const
{
    i64 responseDataSize = BusServer->GetStatistics().ResponseDataSize;
    i64 pendingReadSize = BlockStore->GetPendingReadSize();
    i64 pendingSize = responseDataSize + pendingReadSize;
    if (pendingSize > Config->ResponseThrottlingSize) {
        LOG_DEBUG("Throttling is active (PendingSize: %" PRId64 ")", pendingSize);
        return true;
    } else {
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, StartChunk)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunk_id());

    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    ValidateNoSession(chunkId);
    ValidateNoChunk(chunkId);

    SessionManager->StartSession(chunkId);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, FinishChunk)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunk_id());
    auto& attributes = request->attributes();
    
    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

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

    auto chunkId = TChunkId::FromProto(request->chunk_id());
    i32 startBlockIndex = request->start_block_index();

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

    auto chunkId = TChunkId::FromProto(request->chunk_id());
    i32 startBlockIndex = request->start_block_index();
    i32 blockCount = request->block_count();
    Stroka address = request->address();

    context->SetRequestInfo("ChunkId: %s, StartBlockIndex: %d, BlockCount: %d, Address: %s",
        ~chunkId.ToString(),
        startBlockIndex,
        blockCount,
        ~address);

    auto session = GetSession(chunkId);

    auto startBlock = session->GetBlock(startBlockIndex);

    TProxy proxy(~ChannelCache.GetChannel(address));
    proxy.SetTimeout(Config->MasterRpcTimeout);
    auto putRequest = proxy.PutBlocks();
    putRequest->set_chunk_id(chunkId.ToProto());
    putRequest->set_start_block_index(startBlockIndex);
    
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
                context->Reply(TChunkHolderServiceProxy::EErrorCode::PutBlocksFailed, message);
            }
        }));
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, GetBlocks)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunk_id());
    int blockCount = static_cast<int>(request->block_indexes_size());
    
    context->SetRequestInfo("ChunkId: %s, BlockIndexes: %s",
        ~chunkId.ToString(),
        ~JoinToString(request->block_indexes()));

    bool throttling = CheckThrottling();

    // This will hold the blocks we fetch.
    TSharedPtr< yvector<TCachedBlock::TPtr> > blocks(new yvector<TCachedBlock::TPtr>(blockCount));

    // NB: All callbacks should be handled in the service thread.
    auto awaiter = New<TParallelAwaiter>(ServiceInvoker);

    for (int index = 0; index < blockCount; ++index) {
        i32 blockIndex = request->block_indexes(index);
        TBlockId blockId(chunkId, blockIndex);
        
        auto* blockInfo = response->add_blocks();

        if (throttling) {
            // Cannot send the actual data to the client due to throttling.
            // But let's try to provide a hint a least.
            blockInfo->set_data_attached(false);
            auto block = BlockStore->FindBlock(blockId);
            if (block) {
                auto peers = block->GetPeers();
                if (!peers.empty()) {
                    FOREACH (const auto& peer, peers) {
                        blockInfo->add_peer_addresses(peer.Address);
                    }
                    LOG_DEBUG("GetBlocks: Peers suggested (BlockIndex: %d, PeerCount: %d)",
                        blockIndex,
                        peers.ysize());
                }
            }
        } else {
            // Fetch the actual data (either from cache or from disk).
            LOG_DEBUG("GetBlocks: Fetching block (BlockIndex: %d)", blockIndex);
            awaiter->Await(
                BlockStore->GetBlock(blockId),
                FromFunctor([=] (TBlockStore::TGetBlockResult result)
                    {
                        if (result.IsOK()) {
                            // Attach the real data.
                            blockInfo->set_data_attached(true);
                            auto block = result.Value();
                            (*blocks)[index] = block;
                            LOG_DEBUG("GetBlocks: Block fetched (BlockIndex: %d)", blockIndex);
                        } else if (result.GetCode() == TChunkHolderServiceProxy::EErrorCode::NoSuchChunk) {
                            // This is really sad. We neither have the full chunk nor this particular block.
                            LOG_DEBUG("GetBlocks: Chunk is missing, block is not cached (BlockIndex: %d)", blockIndex);
                        } else {
                            // Something went wrong while fetching the block.
                            // The most probable cause is that a non-existing block was requested for a chunk
                            // that is registered at the holder.
                            awaiter->Cancel();
                            context->Reply(result);
                        }
                    }));
        }
    }

    awaiter->Complete(FromFunctor([=] ()
        {
            // Prepare the attachments and reply.
            for (int index = 0; index < blockCount; ++index) {
                auto block = (*blocks)[index];
                response->Attachments().push_back(block ? block->GetData() : TSharedRef());
            }

            context->Reply();

            // Register the peer that we had just sent the reply to.
            if (request->has_peer_address() && request->has_peer_expiration_time()) {
                TPeerInfo peer(
                    request->peer_address(),
                    TInstant(request->peer_expiration_time()));
                FOREACH (auto& block, *blocks) {
                    if (block) {
                        block->AddPeer(peer);
                    }
                }
            }
        }));
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, FlushBlock)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunk_id());
    int blockIndex = request->block_index();

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

    auto chunkId = TChunkId::FromProto(request->chunk_id());
    auto session = GetSession(chunkId);
    session->RenewLease();

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, GetChunkInfo)
{
    auto chunkId = TChunkId::FromProto(request->chunk_id());

    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    auto chunk = GetChunk(chunkId);
    chunk->GetInfo()->Subscribe(FromFunctor([=] (TChunk::TGetInfoResult result)
        {
            if (result.IsOK()) {
                *response->mutable_chunk_info() = result.Value();
                context->Reply();
            } else {
                context->Reply(result);
            }
        }));
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, PrecacheChunk)
{
    auto chunkId = TChunkId::FromProto(request->chunk_id());

    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    ChunkCache->DownloadChunk(chunkId)->Subscribe(FromFunctor([=] (TChunkCache::TDownloadResult result)
        {
            if (result.IsOK()) {
                context->Reply();
            } else {
                context->Reply(
                    TChunkHolderServiceProxy::EErrorCode::ChunkPrecachingFailed,
                    Sprintf("Error precaching the chunk (ChunkId: %s)\n%s",
                        ~chunkId.ToString(),
                        ~result.ToString()));
            }
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
