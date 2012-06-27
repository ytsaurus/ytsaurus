#include "stdafx.h"
#include "chunk_holder_service.h"
#include "private.h"
#include "config.h"
#include "chunk.h"
#include "location.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "block_store.h"
#include "peer_block_table.h"
#include "session_manager.h"
#include "bootstrap.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/misc/string.h>
#include <ytlib/misc/lazy_ptr.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/key.h>
#include <ytlib/table_client/private.h>
#include <ytlib/table_client/size_limits.h>
#include <ytlib/chunk_holder/chunk_holder_service.pb.h>
#include <ytlib/bus/tcp_dispatcher.h>

namespace NYT {
namespace NChunkHolder {

using namespace NRpc;
using namespace NChunkClient;
using namespace NProto;
using namespace NTableClient;
using ::ToString;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkHolderService::TChunkHolderService(
    TChunkHolderConfigPtr config,
    TBootstrap* bootstrap)
    : NRpc::TServiceBase(
        bootstrap->GetControlInvoker(),
        TProxy::GetServiceName(),
        ChunkHolderLogger.GetCategory())
    , Config(config)
    , Bootstrap(bootstrap)
{
    YCHECK(config);
    YCHECK(bootstrap);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk), Bootstrap->GetWorkInvoker());
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk), Bootstrap->GetWorkInvoker());
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks), Bootstrap->GetWorkInvoker());
    RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks), Bootstrap->GetWorkInvoker());
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlock), Bootstrap->GetWorkInvoker());
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession), Bootstrap->GetWorkInvoker());
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PrecacheChunk));
    RegisterMethod(ONE_WAY_RPC_SERVICE_METHOD_DESC(UpdatePeer));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableSamples));
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
            Sprintf("Chunk upload session %s is invalid or expired", ~chunkId.ToString());
    }
    return session;
}

TChunkPtr TChunkHolderService::GetChunk(const TChunkId& chunkId)
{
    auto chunk = Bootstrap->GetChunkStore()->FindChunk(chunkId);
    if (!chunk) {
        ythrow TServiceException(EErrorCode::NoSuchChunk) <<
            Sprintf("No such chunk %s", ~chunkId.ToString());
    }
    return chunk;
}

bool TChunkHolderService::CheckThrottling() const
{
    i64 responseDataSize = NBus::TTcpDispatcher::Get()->GetStatistics().PendingOutSize;
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
    auto& meta = request->chunk_meta();

    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    auto session = GetSession(chunkId);

    Bootstrap
        ->GetSessionManager()
        ->FinishSession(session, meta)
        .Subscribe(BIND([=] (TChunkPtr chunk) {
            auto chunkInfo = session->GetChunkInfo();
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
        request->Attachments().size());

    auto session = GetSession(chunkId);

    i32 blockIndex = startBlockIndex;
    FOREACH (const auto& attachment, request->Attachments()) {
        // Make a copy of the attachment to enable separate caching
        // of blocks arriving within a single RPC request.
        // TODO(babenko): switched off for now
//        auto data = attachment.ToBlob();
        auto data = attachment;
        session->PutBlock(blockIndex, data);
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

    TProxy proxy(ChannelCache.GetChannel(address));
    auto putRequest = proxy.PutBlocks()
        ->SetTimeout(Config->HolderRpcTimeout);
    *putRequest->mutable_chunk_id() = chunkId.ToProto();
    putRequest->set_start_block_index(startBlockIndex);
    
    for (int blockIndex = startBlockIndex; blockIndex < startBlockIndex + blockCount; ++blockIndex) {
        auto block = session->GetBlock(blockIndex);
        putRequest->Attachments().push_back(block->GetData());
    }

    putRequest->Invoke().Subscribe(
        BIND([=] (TProxy::TRspPutBlocksPtr putResponse) {
            if (putResponse->IsOK()) {
                context->Reply();
            } else {
                context->Reply(TError(
                    TChunkHolderServiceProxy::EErrorCode::PutBlocksFailed,
                    "Error putting blocks to %s\n%s",
                    ~address,
                    ~putResponse->GetError().ToString()));
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
    auto awaiter = New<TParallelAwaiter>(Bootstrap->GetControlInvoker());

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
                LOG_DEBUG("GetBlocks: %" PRId64 " peers suggested for block %d",
                    peers.size(),
                    blockIndex);
            }
        } else {
            // Fetch the actual data (either from cache or from disk).
            LOG_DEBUG("GetBlocks: Fetching block %d", blockIndex);
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
        .Subscribe(BIND([=] () {
            context->Reply();
        }));
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, PingSession)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunk_id());

    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    auto session = GetSession(chunkId);
    session->RenewLease();

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, GetChunkMeta)
{
    auto chunkId = TChunkId::FromProto(request->chunk_id());
    auto extensionTags = FromProto<i32>(request->extension_tags());
    auto partitionTag =
        request->has_partition_tag()
        ? TNullable<int>(request->partition_tag())
        : Null;

    context->SetRequestInfo("ChunkId: %s, AllExtensionTags: %s, ExtensionTags: [%s], PartitionTag: %s", 
        ~chunkId.ToString(),
        ~FormatBool(request->all_extension_tags()),
        ~JoinToString(extensionTags),
        ~ToString(partitionTag));

    auto chunk = GetChunk(chunkId);
    auto asyncChunkMeta = chunk->GetMeta(request->all_extension_tags() 
        ? NULL
        : &extensionTags);

    asyncChunkMeta.Subscribe(BIND([=] (TChunk::TGetMetaResult result) {
        if (!result.IsOK()) {
            context->Reply(result);
            return;
        }

        *response->mutable_chunk_meta() = result.Value();

        if (partitionTag) {
            // XXX(psushin): Don't use std::vector!
            // std::vector here causes MSVC internal compiler error :)
            yvector<NTableClient::NProto::TBlockInfo> filteredBlocks;
            auto channelsExt = GetProtoExtension<NTableClient::NProto::TChannelsExt>(
                result.Value().extensions());
            // Partition chunks must have only one channel.
            YCHECK(channelsExt.items_size() == 1);

            FOREACH (const auto& blockInfo, channelsExt.items(0).blocks()) {
                YCHECK(blockInfo.partition_tag() != NTableClient::DefaultPartitionTag);
                if (blockInfo.partition_tag() == partitionTag.Get()) {
                    filteredBlocks.push_back(blockInfo);
                }
            }

            ToProto(channelsExt.mutable_items(0)->mutable_blocks(), filteredBlocks);
            UpdateProtoExtension(
                response->mutable_chunk_meta()->mutable_extensions(), 
                channelsExt);
        }

        context->Reply();
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
                    Sprintf("Error precaching chunk %s\n%s",
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

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, GetTableSamples)
{
    context->SetRequestInfo("KeyColumnCount: %d, ChunkCount: %d",
        request->key_columns_size(),
        request->chunk_ids_size());

    auto awaiter = New<TParallelAwaiter>(Bootstrap->GetWorkInvoker());
    auto keyColumns = FromProto<Stroka>(request->key_columns());
    auto chunkIds = FromProto<TChunkId>(request->chunk_ids());

    FOREACH (const auto& chunkId, chunkIds) {
        auto* chunkSamples = response->add_samples();
        auto chunk = Bootstrap->GetChunkStore()->FindChunk(chunkId);

        if (!chunk) {
            LOG_WARNING("GetTableSamples: No such chunk %s\n", 
                ~chunkId.ToString());
            *chunkSamples->mutable_error() = TError("No such chunk").ToProto();
        } else {
            awaiter->Await(
                chunk->GetMeta(),
                BIND([=] (TChunk::TGetMetaResult result) {
                    if (!result.IsOK()) {
                        LOG_WARNING("GetTableSamples: Error getting meta of chunk %s\n", 
                            ~chunkId.ToString(),
                            ~result.ToString());
                        *chunkSamples->mutable_error() = result.ToProto();
                        return;
                    }

                    auto samplesExt = GetProtoExtension<NTableClient::NProto::TSamplesExt>(result.Value().extensions());
                    FOREACH (const auto& sample, samplesExt.items()) {
                        auto* key = chunkSamples->add_items();

                        size_t size = 0;
                        FOREACH (const auto& column, keyColumns) {
                            if (size >= NTableClient::MaxKeySize)
                                break;

                            auto* keyPart = key->add_parts();
                            auto it = std::lower_bound(
                                sample.parts().begin(),
                                sample.parts().end(),
                                column,
                                [] (const NTableClient::NProto::TSamplePart& part, const Stroka& column) {
                                    return part.column() < column;
                                });

                            size += sizeof(i32); // part type
                            if (it != sample.parts().end() && it->column() == column) {
                                keyPart->set_type(it->key_part().type());
                                switch (it->key_part().type()) {
                                    case EKeyPartType::Composite:
                                        break;
                                    case EKeyPartType::Integer:
                                        keyPart->set_int_value(it->key_part().int_value());
                                        size += sizeof(keyPart->int_value());
                                        break;
                                    case EKeyPartType::Double:
                                        keyPart->set_double_value(it->key_part().double_value());
                                        size += sizeof(keyPart->double_value());
                                        break;
                                    case EKeyPartType::String: {
                                        auto partSize = std::min(it->key_part().str_value().size(), MaxKeySize - size);
                                        keyPart->set_str_value(it->key_part().str_value().begin(), partSize);
                                        size += partSize;
                                        break;
                                    }
                                    default:
                                        YUNREACHABLE();
                                }
                            } else {
                                keyPart->set_type(EKeyPartType::Null);
                            }
                        }
                    }
                }));
        }
    }

    awaiter->Complete(BIND([=] () {
        context->Reply();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
