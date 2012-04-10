#include "stdafx.h"
#include "chunk_holder_service.h"
#include "common.h"
#include "config.h"
#include "chunk.h"
#include "location.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "block_store.h"
#include "peer_block_table.h"
#include "session_manager.h"
#include "bootstrap.h"

#include <ytlib/chunk_holder/chunk_holder_service.pb.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/string.h>
#include <ytlib/actions/parallel_awaiter.h>

namespace NYT {
namespace NChunkHolder {

using namespace NRpc;
using namespace NChunkClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkHolderService::TChunkHolderService(
    TChunkHolderConfigPtr config,
    TBootstrap* bootstrap)
    : NRpc::TServiceBase(
        ~bootstrap->GetControlInvoker(),
        TProxy::GetServiceName(),
        ChunkHolderLogger.GetCategory())
    , Config(config)
    , Bootstrap(bootstrap)
{
    YASSERT(config);
    YASSERT(bootstrap);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlock));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkInfo));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PrecacheChunk));
    RegisterMethod(ONE_WAY_RPC_SERVICE_METHOD_DESC(UpdatePeer));
}

void TChunkHolderService::ValidateNoSession(const TChunkId& chunkId)
{
    if (Bootstrap->GetSessionManager()->FindSession(chunkId)) {
        ythrow TServiceException(EErrorCode::SessionAlreadyExists) <<
            Sprintf("Session %s already exists", ~chunkId.ToString());
    }
}

void TChunkHolderService::ValidateNoChunk(const TChunkId& chunkId)
{
    if (Bootstrap->GetChunkStore()->FindChunk(chunkId)) {
        ythrow TServiceException(EErrorCode::ChunkAlreadyExists) <<
            Sprintf("Chunk %s already exists", ~chunkId.ToString());
    }
}

TSessionPtr TChunkHolderService::GetSession(const TChunkId& chunkId)
{
    auto session = Bootstrap->GetSessionManager()->FindSession(chunkId);
    if (!session) {
        ythrow TServiceException(EErrorCode::NoSuchSession) <<
            Sprintf("Session is invalid or expired (ChunkId: %s)", ~chunkId.ToString());
    }
    return session;
}

TChunkPtr TChunkHolderService::GetChunk(const TChunkId& chunkId)
{
    auto chunk = Bootstrap->GetChunkStore()->FindChunk(chunkId);
    if (!chunk) {
        ythrow TServiceException(EErrorCode::NoSuchChunk) <<
            Sprintf("No such chunk (ChunkId: %s)", ~chunkId.ToString());
    }
    return chunk;
}

bool TChunkHolderService::CheckThrottling() const
{
    i64 responseDataSize = Bootstrap->GetBusServer()->GetStatistics().ResponseDataSize;
    i64 pendingReadSize = Bootstrap->GetBlockStore()->GetPendingReadSize();
    i64 pendingSize = responseDataSize + pendingReadSize;
    if (pendingSize > Config->ResponseThrottlingSize) {
        LOG_DEBUG("Throttling activated (PendingSize: %" PRId64 ")", pendingSize);
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

    Bootstrap->GetSessionManager()->StartSession(chunkId);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, FinishChunk)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunk_id());
    auto& attributes = request->attributes();
    
    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    auto session = GetSession(chunkId);

    Bootstrap
        ->GetSessionManager()
        ->FinishSession(~session, attributes)
        .Subscribe(BIND([=] (TChunkPtr chunk) {
            auto chunkInfo = session->GetChunkInfo();
            // Don't report attributes to the writer since it has them already.
            chunkInfo.clear_attributes();
            *response->mutable_chunk_info() = chunkInfo;
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
    auto putRequest = proxy.PutBlocks();
    *putRequest->mutable_chunk_id() = chunkId.ToProto();
    putRequest->set_start_block_index(startBlockIndex);
    
    for (int blockIndex = startBlockIndex; blockIndex < startBlockIndex + blockCount; ++blockIndex) {
        auto block = session->GetBlock(blockIndex);
        putRequest->Attachments().push_back(block->GetData());
    }

    putRequest->Invoke().Subscribe(
        BIND([=] (TProxy::TRspPutBlocks::TPtr putResponse) {
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
    auto chunkId = TChunkId::FromProto(request->chunk_id());
    int blockCount = static_cast<int>(request->block_indexes_size());
    
    context->SetRequestInfo("ChunkId: %s, BlockIndexes: %s",
        ~chunkId.ToString(),
        ~JoinToString(request->block_indexes()));

    bool throttling = CheckThrottling();

    response->Attachments().resize(blockCount);

    // NB: All callbacks should be handled in the control thread.
    auto awaiter = New<TParallelAwaiter>(~Bootstrap->GetControlInvoker());

    auto peerBlockTable = Bootstrap->GetPeerBlockTable();
    for (int index = 0; index < blockCount; ++index) {
        i32 blockIndex = request->block_indexes(index);
        TBlockId blockId(chunkId, blockIndex);
        
        auto* blockInfo = response->add_blocks();

        if (throttling) {
            // Cannot send the actual data to the client due to throttling.
            // But let's try to provide a hint a least.
            blockInfo->set_data_attached(false);
            const auto& peers = peerBlockTable->GetPeers(blockId);
            if (!peers.empty()) {
                FOREACH (const auto& peer, peers) {
                    blockInfo->add_peer_addresses(peer.Address);
                }
                LOG_DEBUG("GetBlocks: Peers suggested (BlockIndex: %d, PeerCount: %d)",
                    blockIndex,
                    peers.ysize());
            }
        } else {
            // Fetch the actual data (either from cache or from disk).
            LOG_DEBUG("GetBlocks: Fetching block (BlockIndex: %d)", blockIndex);
            awaiter->Await(
                Bootstrap->GetBlockStore()->GetBlock(blockId),
                BIND([=] (TBlockStore::TGetBlockResult result) {
                    if (result.IsOK()) {
                        // Attach the real data.
                        blockInfo->set_data_attached(true);
                        auto block = result.Value();
                        response->Attachments()[index] = block->GetData();
                        LOG_DEBUG("GetBlocks: Block fetched (BlockIndex: %d)", blockIndex);
                    } else if (result.GetCode() == TChunkHolderServiceProxy::EErrorCode::NoSuchChunk) {
                        // This is really sad. We neither have the full chunk nor this particular block.
                        blockInfo->set_data_attached(false);
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

    awaiter->Complete(BIND([=] () {
        // Compute statistics.
        int blocksWithData = 0;
        int blocksWithPeers = 0;
        FOREACH (const auto& blockInfo, response->blocks()) {
            if (blockInfo.data_attached()) {
                ++blocksWithData;
            } else if (blockInfo.peer_addresses_size() != 0) {
                ++blocksWithPeers;
            }
        }

        context->SetResponseInfo("BlocksWithData: %d, BlocksWithPeers: %d",
            blocksWithData,
            blocksWithPeers);

        context->Reply();

        // Register the peer that we had just sent the reply to.
        if (request->has_peer_address() && request->has_peer_expiration_time()) {
            TPeerInfo peer(request->peer_address(), TInstant(request->peer_expiration_time()));
            for (int index = 0; index < blockCount; ++index) {
                if (response->blocks(index).data_attached()) {
                    i32 blockIndex = request->block_indexes(index);
                    peerBlockTable->UpdatePeer(TBlockId(chunkId, blockIndex), peer);
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

    session
        ->FlushBlock(blockIndex)
        .Subscribe(BIND([=] (TVoid) {
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

    GetChunk(chunkId)
        ->GetInfo()
        .Subscribe(BIND([=] (TChunk::TGetInfoResult result) {
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

    Bootstrap
        ->GetChunkCache()
        ->DownloadChunk(chunkId)
        .Subscribe(BIND([=] (TChunkCache::TDownloadResult result) {
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

DEFINE_ONE_WAY_RPC_SERVICE_METHOD(TChunkHolderService, UpdatePeer)
{
    TPeerInfo peer(request->peer_address(), TInstant(request->peer_expiration_time()));

    context->SetRequestInfo("PeerAddress: %s, ExpirationTime: %s, BlockCount: %d",
        ~request->peer_address(),
        ~TInstant(request->peer_expiration_time()).ToString(),
        request->block_ids_size());

    auto peerBlockTable = Bootstrap->GetPeerBlockTable();
    FOREACH (const auto& block_id, request->block_ids()) {
        TBlockId blockId(TGuid::FromProto(block_id.chunk_id()), block_id.block_index());
        peerBlockTable->UpdatePeer(blockId, peer);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
