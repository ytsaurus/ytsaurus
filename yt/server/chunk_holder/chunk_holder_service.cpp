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
#include <ytlib/misc/random.h>
#include <ytlib/misc/nullable.h>

#include <ytlib/bus/tcp_dispatcher.h>

#include <ytlib/actions/parallel_awaiter.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/key.h>
#include <ytlib/table_client/private.h>
#include <ytlib/table_client/size_limits.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_holder_service.pb.h>

namespace NYT {
namespace NChunkHolder {

using namespace NRpc;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkHolderService::TChunkHolderService(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
    : NRpc::TServiceBase(
        bootstrap->GetControlInvoker(),
        TProxy::GetServiceName(),
        DataNodeLogger.GetCategory())
    , Config(config)
    , WorkerThread(New<TActionQueue>("HolderWorker"))
    , Bootstrap(bootstrap)
{
    YCHECK(config);
    YCHECK(bootstrap);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlock));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlocks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PrecacheChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdatePeer)
        .SetOneWay(true));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableSamples)
        .SetResponseCodec(ECodecId::Snappy)
        .SetResponseHeavy(true));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkSplits)
        .SetResponseCodec(ECodecId::Snappy)
        .SetResponseHeavy(true));
}

void TChunkHolderService::ValidateNoSession(const TChunkId& chunkId)
{
    if (Bootstrap->GetSessionManager()->FindSession(chunkId)) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::SessionAlreadyExists,
            "Session already exists: %s",
            ~chunkId.ToString());
    }
}

void TChunkHolderService::ValidateNoChunk(const TChunkId& chunkId)
{
    if (Bootstrap->GetChunkStore()->FindChunk(chunkId)) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::ChunkAlreadyExists,
            "Chunk already exists: %s",
            ~chunkId.ToString());
    }
}

TSessionPtr TChunkHolderService::GetSession(const TChunkId& chunkId)
{
    auto session = Bootstrap->GetSessionManager()->FindSession(chunkId);
    if (!session) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::NoSuchSession,
            "Session is invalid or expired: %s",
            ~chunkId.ToString());
    }
    return session;
}

TChunkPtr TChunkHolderService::GetChunk(const TChunkId& chunkId)
{
    auto chunk = Bootstrap->GetChunkStore()->FindChunk(chunkId);
    if (!chunk) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::NoSuchChunk,
            "No such chunk: %s",
            ~chunkId.ToString());
    }
    return chunk;
}

void TChunkHolderService::OnGotChunkMeta(TCtxGetChunkMetaPtr context, TNullable<int> partitionTag, TChunk::TGetMetaResult result)
{
    if (!result.IsOK()) {
        context->Reply(result);
        return;
    }

    *context->Response().mutable_chunk_meta() = result.Value();

    if (partitionTag) {
        std::vector<NTableClient::NProto::TBlockInfo> filteredBlocks;
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
            context->Response().mutable_chunk_meta()->mutable_extensions(),
            channelsExt);
    }

    context->Reply();
}


bool TChunkHolderService::CheckThrottling() const
{
    i64 responseDataSize = NBus::TTcpDispatcher::Get()->GetStatistics().PendingOutSize;
    i64 pendingReadSize = Bootstrap->GetBlockStore()->GetPendingReadSize();
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

    context->SetRequestInfo("ChunkId: %s, DirectMode: %s", 
        ~chunkId.ToString(),
        request->direct_mode() ? "True" : "False");

    ValidateNoSession(chunkId);
    ValidateNoChunk(chunkId);

    Bootstrap->GetSessionManager()->StartSession(chunkId, request->direct_mode());

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, FinishChunk)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunk_id());
    auto& meta = request->chunk_meta();

    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    auto session = GetSession(chunkId);

    YCHECK(session->GetWrittenBlockCount() == request->block_count());

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
    bool enableCaching = request->enable_caching();

    context->SetRequestInfo("ChunkId: %s, StartBlockIndex: %d, BlockCount: %d, EnableCaching: %s",
        ~chunkId.ToString(),
        startBlockIndex,
        request->Attachments().size(),
        ~FormatBool(enableCaching));

    auto session = GetSession(chunkId);

    i32 blockIndex = startBlockIndex;
    FOREACH (const auto& block, request->Attachments()) {
        session->PutBlock(blockIndex, block, enableCaching);
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
    Stroka targetAddress = request->target_address();

    context->SetRequestInfo("ChunkId: %s, StartBlockIndex: %d, BlockCount: %d, TargetAddress: %s",
        ~chunkId.ToString(),
        startBlockIndex,
        blockCount,
        ~targetAddress);

    auto session = GetSession(chunkId);

    auto startBlock = session->GetBlock(startBlockIndex);

    TProxy proxy(ChannelCache.GetChannel(targetAddress));
    auto putRequest = proxy.PutBlocks()
        ->SetTimeout(Config->NodeRpcTimeout);
    *putRequest->mutable_chunk_id() = chunkId.ToProto();
    putRequest->set_start_block_index(startBlockIndex);
    
    for (int blockIndex = startBlockIndex; blockIndex < startBlockIndex + blockCount; ++blockIndex) {
        auto block = session->GetBlock(blockIndex);
        putRequest->Attachments().push_back(block);
    }

    putRequest->Invoke().Subscribe(
        BIND([=] (TProxy::TRspPutBlocksPtr putResponse) {
            if (putResponse->IsOK()) {
                context->Reply();
            } else {
                context->Reply(TError(
                    TChunkHolderServiceProxy::EErrorCode::PutBlocksFailed,
                    "Error putting blocks to %s",
                    ~targetAddress)
                    << putResponse->GetError());
            }
        }));
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, GetBlocks)
{
    auto chunkId = TChunkId::FromProto(request->chunk_id());
    int blockCount = static_cast<int>(request->block_indexes_size());
    bool enableCaching = request->enable_caching();
    
    context->SetRequestInfo("ChunkId: %s, BlockIndexes: %s, EnableCaching: %s",
        ~chunkId.ToString(),
        ~JoinToString(request->block_indexes()),
        ~FormatBool(enableCaching));

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
                LOG_DEBUG("GetBlocks: %" PRISZT " peers suggested for block %d",
                    peers.size(),
                    blockIndex);
            }
        } else {
            // Fetch the actual data (either from cache or from disk).
            LOG_DEBUG("GetBlocks: Fetching block %d", blockIndex);
            awaiter->Await(
                Bootstrap->GetBlockStore()->GetBlock(blockId, enableCaching),
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

    asyncChunkMeta.Subscribe(BIND(&TChunkHolderService::OnGotChunkMeta,
        Unretained(this),
        context,
        partitionTag));
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
                context->Reply(TError(
                    TChunkHolderServiceProxy::EErrorCode::ChunkPrecachingFailed,
                    "Error precaching chunk %s",
                    ~chunkId.ToString())
                    << result);
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
        request->sample_requests_size());

    auto awaiter = New<TParallelAwaiter>(WorkerThread->GetInvoker());
    auto keyColumns = FromProto<Stroka>(request->key_columns());

    FOREACH (const auto& sampleRequest, request->sample_requests()) {
        auto* chunkSamples = response->add_samples();
        auto chunkId = TChunkId::FromProto(sampleRequest.chunk_id());
        auto chunk = Bootstrap->GetChunkStore()->FindChunk(chunkId);

        if (!chunk) {
            LOG_WARNING("GetTableSamples: No such chunk %s\n", 
                ~chunkId.ToString());
            ToProto(chunkSamples->mutable_error(), TError("No such chunk"));
        } else {
            awaiter->Await(chunk->GetMeta(), BIND(
                &TChunkHolderService::ProcessSample, 
                MakeStrong(this), 
                &sampleRequest,
                chunkSamples,
                keyColumns));
        }
    }

    awaiter->Complete(BIND([=] () {
        context->Reply();
    }));
}

void TChunkHolderService::ProcessSample(
    const NChunkClient::NProto::TReqGetTableSamples::TSampleRequest* sampleRequest,
    NChunkClient::NProto::TRspGetTableSamples::TChunkSamples* chunkSamples,
    const TKeyColumns& keyColumns,
    TChunk::TGetMetaResult result)
{
    auto chunkId = TChunkId::FromProto(sampleRequest->chunk_id());

    if (!result.IsOK()) {
        LOG_WARNING("GetTableSamples: Error getting meta of chunk %s\n%s", 
            ~chunkId.ToString(),
            ~ToString(result));
        ToProto(chunkSamples->mutable_error(), result);
        return;
    }

    auto samplesExt = GetProtoExtension<NTableClient::NProto::TSamplesExt>(result.Value().extensions());
    std::vector<NTableClient::NProto::TSample> samples;
    RandomSampleN(
        samplesExt.items().begin(), 
        samplesExt.items().end(), 
        std::back_inserter(samples), 
        sampleRequest->sample_count());

    FOREACH (const auto& sample, samples) {
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
}

DEFINE_RPC_SERVICE_METHOD(TChunkHolderService, GetChunkSplits)
{
    context->SetRequestInfo("KeyColumnCount: %d, ChunkCount: %d, MinSplitSize: %"PRId64,
        request->key_columns_size(),
        request->input_chunks_size(),
        request->min_split_size());

    auto awaiter = New<TParallelAwaiter>(WorkerThread->GetInvoker());
    auto keyColumns = FromProto<Stroka>(request->key_columns());

    FOREACH (const auto& inputChunk, request->input_chunks()) {
        auto chunkId = TChunkId::FromProto(inputChunk.slice().chunk_id());
        auto* splittedChunk = response->add_splitted_chunks();
        auto chunk = Bootstrap->GetChunkStore()->FindChunk(chunkId);

        if (!chunk) {
            auto error = TError("No such chunk %s", ~chunkId.ToString());
            LOG_ERROR("%s", ~ToString(error));
            ToProto(splittedChunk->mutable_error(), error);
        } else {
            awaiter->Await(chunk->GetMeta(), BIND(
                &TChunkHolderService::MakeChunkSplits, 
                MakeStrong(this),
                &inputChunk,
                splittedChunk,
                request->min_split_size(),
                keyColumns));
        }
    }

    awaiter->Complete(BIND([=] () {
        context->Reply();
    }));
}

void TChunkHolderService::MakeChunkSplits(
    const NTableClient::NProto::TInputChunk* inputChunk,
    NChunkClient::NProto::TRspGetChunkSplits::TChunkSplits* splittedChunk,
    i64 minSplitSize,
    const TKeyColumns& keyColumns,
    TChunk::TGetMetaResult result)
{
    auto chunkId = TChunkId::FromProto(inputChunk->slice().chunk_id());

    if (!result.IsOK()) {
        auto error = TError("GetChunkSplits: Error getting meta of chunk %s", ~chunkId.ToString())
            << result;
        LOG_ERROR("%s", ~ToString(error));
        ToProto(splittedChunk->mutable_error(), error);
        return;
    }

    YCHECK(result.Value().type() == EChunkType::Table);

    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(result.Value().extensions());
    if (!miscExt.sorted()) {
        auto error =  TError("GetChunkSplits: Requested chunk splits for unsorted chunk %s", 
            ~chunkId.ToString());
        LOG_ERROR("%s", ~ToString(error));
        ToProto(splittedChunk->mutable_error(), error);
        return;
    }

    auto keyColumnsExt = GetProtoExtension<NTableClient::NProto::TKeyColumnsExt>(result.Value().extensions());
    if (keyColumnsExt.values_size() < keyColumns.size()) {
        auto error = TError("Not enough key columns in chunk %s (Expected: %d, Actual: %d)", 
            ~chunkId.ToString(),
            static_cast<int>(keyColumns.size()),
            static_cast<int>(keyColumnsExt.values_size()));
        LOG_ERROR("%s", ~ToString(error));
        ToProto(splittedChunk->mutable_error(), error);
        return;
    }

    for (int i = 0; i < keyColumns.size(); ++i) {
        Stroka value = keyColumnsExt.values(i);
        if (keyColumns[i] != value) {
            auto error = TError("Invalid key columns (Expected: %s, Actual: %s)",
                ~keyColumns[i],
                ~value);
            LOG_ERROR("%s", ~ToString(error));
            ToProto(splittedChunk->mutable_error(), error);
            return;
        }
    }

    auto indexExt = GetProtoExtension<NTableClient::NProto::TIndexExt>(result.Value().extensions());
    if (indexExt.items_size() == 1) {
        // Only one index entry available - no need to split.
        splittedChunk->add_input_chunks()->CopyFrom(*inputChunk);
        return;
    }

    auto back = --indexExt.items().end();
    auto dataSizeBetweenSamples = static_cast<i64>(std::ceil(
        float(back->row_index()) /
        miscExt.row_count() *
        miscExt.uncompressed_data_size() /
        indexExt.items_size()));

    auto comparator = [&] (
        const NTableClient::NProto::TReadLimit& limit,
        const NTableClient::NProto::TIndexRow& indexRow,
        bool isStartLimit)
    {
        if (!limit.has_row_index() && !limit.has_key()) {
            return isStartLimit ? -1 : 1;
        }

        auto result = 0;
        if (limit.has_row_index()) {
            auto diff = limit.row_index() - indexRow.row_index();
            // Sign function.
            result += (diff > 0) - (diff < 0);
        }

        if (limit.has_key()) {
            result += CompareKeys(limit.key(), indexRow.key(), keyColumns.size());
        }

        if (result == 0) {
            return isStartLimit ? -1 : 1;
        }

        return (result > 0) - (result < 0);
    };

    auto beginIter = std::lower_bound(
        indexExt.items().begin(), 
        indexExt.items().end(), 
        inputChunk->slice().start_limit(), 
        [&] (const NTableClient::NProto::TIndexRow& indexRow,
             const NTableClient::NProto::TReadLimit& limit) 
        {
            return comparator(limit, indexRow, true) > 0;
        });

    auto endIter = std::upper_bound(
        beginIter, 
        indexExt.items().end(), 
        inputChunk->slice().end_limit(), 
        [&] (const NTableClient::NProto::TReadLimit& limit,
             const NTableClient::NProto::TIndexRow& indexRow) 
        {
            return comparator(limit, indexRow, false) < 0;
        });

    NTableClient::NProto::TInputChunk* currentSplit;
    NTableClient::NProto::TBoundaryKeysExt boundaryKeysExt;
    i64 endRowIndex = beginIter->row_index();
    i64 startRowIndex;
    i64 dataSize;

    auto createNewSplit = [&] () {
        currentSplit = splittedChunk->add_input_chunks();
        currentSplit->CopyFrom(*inputChunk);
        boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(currentSplit->extensions());
        startRowIndex = endRowIndex;
        dataSize = 0;
    };
    createNewSplit();

    auto samplesLeft = std::distance(beginIter, endIter) - 1;
    while(samplesLeft > 0) {
        ++beginIter;
        --samplesLeft;
        dataSize += dataSizeBetweenSamples;

        auto nextIter = beginIter + 1;
        if (nextIter == endIter) {
            break;
        }

        if (samplesLeft * dataSizeBetweenSamples < minSplitSize) {
            break;
        }

        if (CompareKeys(nextIter->key(), beginIter->key(), keyColumns.size()) == 0) {
            continue;
        }

        if (dataSize > minSplitSize) {
            auto key = beginIter->key();
            while (key.parts_size() > keyColumns.size()) {
                key.mutable_parts()->RemoveLast();
            }

            *boundaryKeysExt.mutable_end() = key;
            UpdateProtoExtension(currentSplit->mutable_extensions(), boundaryKeysExt);

            endRowIndex = beginIter->row_index();
            currentSplit->set_row_count(endRowIndex - startRowIndex);
            currentSplit->set_uncompressed_data_size(dataSize);

            key = GetSuccessorKey(key);
            *currentSplit->mutable_slice()->mutable_end_limit()->mutable_key() = key;

            createNewSplit();
            *boundaryKeysExt.mutable_start() = key;
            *currentSplit->mutable_slice()->mutable_start_limit()->mutable_key() = key;
        }
    }

    UpdateProtoExtension(currentSplit->mutable_extensions(), boundaryKeysExt);
    endRowIndex = (--endIter)->row_index();
    currentSplit->set_row_count(endRowIndex - startRowIndex);
    currentSplit->set_uncompressed_data_size(
        dataSize + (std::distance(beginIter, endIter) - 1) * dataSizeBetweenSamples);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
