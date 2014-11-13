#include "stdafx.h"
#include "data_node_service.h"
#include "private.h"
#include "config.h"
#include "chunk.h"
#include "location.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "chunk_registry.h"
#include "block_store.h"
#include "peer_block_table.h"
#include "session_manager.h"

#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/string.h>
#include <core/misc/lazy_ptr.h>
#include <core/misc/random.h>
#include <core/misc/nullable.h>

#include <core/bus/tcp_dispatcher.h>

#include <core/concurrency/parallel_awaiter.h>

#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/chunk_client/key.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/data_node_service.pb.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <server/cell_node/bootstrap.h>

#include <cmath>

namespace NYT {
namespace NDataNode {

using namespace NRpc;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NCellNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;
static auto& Profiler = DataNodeProfiler;
static auto ProfilingPeriod = TDuration::MilliSeconds(100);

const size_t MaxSampleSize = 4 * 1024;

////////////////////////////////////////////////////////////////////////////////

TDataNodeService::TDataNodeService(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
    : NRpc::TServiceBase(
        CreatePrioritizedInvoker(bootstrap->GetControlInvoker()),
        TProxy::GetServiceName(),
        DataNodeLogger.GetCategory())
    , Config(config)
    , WorkerThread(New<TActionQueue>("DataNodeWorker"))
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
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlocks)
        .SetEnableReorder(true)
        .SetMaxQueueSize(5000));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta)
        .SetEnableReorder(true)
        .SetMaxQueueSize(5000));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PrecacheChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdatePeer)
        .SetOneWay(true));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableSamples)
        .SetResponseCodec(NCompression::ECodec::Lz4)
        .SetResponseHeavy(true));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkSplits)
        .SetResponseCodec(NCompression::ECodec::Lz4)
        .SetResponseHeavy(true));

    ProfilingExecutor = New<TPeriodicExecutor>(
        Bootstrap->GetControlInvoker(),
        BIND(&TDataNodeService::OnProfiling, MakeWeak(this)),
        ProfilingPeriod);
    ProfilingExecutor->Start();
}

void TDataNodeService::ValidateNoSession(const TChunkId& chunkId)
{
    if (Bootstrap->GetSessionManager()->FindSession(chunkId)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::SessionAlreadyExists,
            "Session %s already exists",
            ~ToString(chunkId));
    }
}

void TDataNodeService::ValidateNoChunk(const TChunkId& chunkId)
{
    if (Bootstrap->GetChunkStore()->FindChunk(chunkId)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::ChunkAlreadyExists,
            "Chunk %s already exists",
            ~ToString(chunkId));
    }
}

TSessionPtr TDataNodeService::GetSession(const TChunkId& chunkId)
{
    auto session = Bootstrap->GetSessionManager()->FindSession(chunkId);
    if (!session) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoSuchSession,
            "Session %s is invalid or expired",
            ~ToString(chunkId));
    }
    return session;
}

TChunkPtr TDataNodeService::GetChunk(const TChunkId& chunkId)
{
    auto chunk = Bootstrap->GetChunkRegistry()->FindChunk(chunkId);
    if (!chunk) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoSuchChunk,
            "No such chunk %s",
            ~ToString(chunkId));
    }
    return chunk;
}

void TDataNodeService::OnGotChunkMeta(
    TCtxGetChunkMetaPtr context,
    TNullable<int> partitionTag,
    TChunk::TGetMetaResult result)
{
    if (!result.IsOK()) {
        context->Reply(result);
        return;
    }

    *context->Response().mutable_chunk_meta() = result.Value();

    if (partitionTag) {
        std::vector<NTableClient::NProto::TBlockInfo> filteredBlocks;
        auto channelsExt = GetProtoExtension<TChannelsExt>(
            result.Value().extensions());
        // Partition chunks must have only one channel.
        YCHECK(channelsExt.items_size() == 1);

        FOREACH (const auto& blockInfo, channelsExt.items(0).blocks()) {
            YCHECK(blockInfo.partition_tag() != DefaultPartitionTag);
            if (blockInfo.partition_tag() == partitionTag.Get()) {
                filteredBlocks.push_back(blockInfo);
            }
        }

        ToProto(channelsExt.mutable_items(0)->mutable_blocks(), filteredBlocks);
        SetProtoExtension(
            context->Response().mutable_chunk_meta()->mutable_extensions(),
            channelsExt);
    }

    context->Reply();
}

i64 TDataNodeService::GetPendingOutSize() const
{
    return
        NBus::TTcpDispatcher::Get()->GetStatistics(NBus::ETcpInterfaceType::Remote).PendingOutSize +
        Bootstrap->GetBlockStore()->GetPendingReadSize();
}

i64 TDataNodeService::GetPendingInSize() const
{
    return Bootstrap->GetSessionManager()->GetPendingWriteSize();
}

bool TDataNodeService::IsOutThrottling() const
{
    i64 pendingSize = GetPendingOutSize();
    if (pendingSize > Config->BusOutThrottlingLimit) {
        LOG_DEBUG("Outcoming throttling is active: %" PRId64 " > %" PRId64,
            pendingSize,
            Config->BusOutThrottlingLimit);
        return true;
    } else {
        return false;
    }
}

bool TDataNodeService::IsInThrottling() const
{
    i64 pendingSize = GetPendingInSize();
    if (pendingSize > Config->BusInThrottlingLimit) {
        LOG_DEBUG("Incoming throttling is active: %" PRId64 " > %" PRId64,
            pendingSize,
            Config->BusInThrottlingLimit);
        return true;
    } else {
        return false;
    }
}

void TDataNodeService::OnProfiling()
{
    Profiler.Enqueue("/pending_out_size", GetPendingOutSize());
    Profiler.Enqueue("/pending_in_size", GetPendingInSize());

    auto sessionManager = Bootstrap->GetSessionManager();
    FOREACH (auto typeValue, EWriteSessionType::GetDomainValues()) {
        auto type = EWriteSessionType(typeValue);
        Profiler.Enqueue("/session_count/" + FormatEnum(type), sessionManager->GetSessionCount(type));
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TDataNodeService, StartChunk)
{
    UNUSED(response);

    auto chunkId = FromProto<TChunkId>(request->chunk_id());
    auto sessionType = EWriteSessionType(request->session_type());
    bool syncOnClose = request->sync_on_close();

    context->SetRequestInfo("ChunkId: %s, SessionType: %s, SyncOnClose: %s",
        ~ToString(chunkId),
        ~sessionType.ToString(),
        ~FormatBool(syncOnClose));

    ValidateNoSession(chunkId);
    ValidateNoChunk(chunkId);

    auto sessionManager = Bootstrap->GetSessionManager();
    sessionManager->StartSession(chunkId, sessionType, syncOnClose);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TDataNodeService, FinishChunk)
{
    UNUSED(response);

    auto chunkId = FromProto<TChunkId>(request->chunk_id());
    auto& meta = request->chunk_meta();

    context->SetRequestInfo("ChunkId: %s", ~ToString(chunkId));

    auto session = GetSession(chunkId);

    YCHECK(session->GetWrittenBlockCount() == request->block_count());

    Bootstrap
        ->GetSessionManager()
        ->FinishSession(session, meta)
        .Subscribe(BIND([=] (TErrorOr<TChunkPtr> chunkOrError) {
            if (chunkOrError.IsOK()) {
                auto chunk = chunkOrError.Value();
                auto chunkInfo = session->GetChunkInfo();
                *response->mutable_chunk_info() = chunkInfo;
                context->Reply();
            } else {
                context->Reply(chunkOrError);
            }
        }));
}

DEFINE_RPC_SERVICE_METHOD(TDataNodeService, PutBlocks)
{
    UNUSED(response);

    if (IsInThrottling()) {
        context->Reply(TError(
            NRpc::EErrorCode::Unavailable,
            "Write throttling is active"));
        return;
    }

    auto chunkId = FromProto<TChunkId>(request->chunk_id());
    int startBlockIndex = request->start_block_index();
    bool enableCaching = request->enable_caching();

    context->SetRequestInfo("ChunkId: %s, StartBlock: %d, BlockCount: %d, EnableCaching: %s",
        ~ToString(chunkId),
        startBlockIndex,
        request->Attachments().size(),
        ~FormatBool(enableCaching));

    auto session = GetSession(chunkId);
    session
        ->PutBlocks(startBlockIndex, request->Attachments(), enableCaching)
        .Subscribe(BIND([=] (TError error) {
            context->Reply(error);
        }));
}

DEFINE_RPC_SERVICE_METHOD(TDataNodeService, SendBlocks)
{
    UNUSED(response);

    auto chunkId = FromProto<TChunkId>(request->chunk_id());
    int startBlockIndex = request->start_block_index();
    int blockCount = request->block_count();
    auto target = FromProto<TNodeDescriptor>(request->target());

    context->SetRequestInfo("ChunkId: %s, StartBlock: %d, BlockCount: %d, TargetAddress: %s",
        ~ToString(chunkId),
        startBlockIndex,
        blockCount,
        ~target.GetDefaultAddress());

    auto session = GetSession(chunkId);
    session
        ->SendBlocks(startBlockIndex, blockCount, target)
        .Subscribe(BIND([=] (TError error) {
            if (error.IsOK()) {
                context->Reply();
            } else {
                context->Reply(TError(
                    NChunkClient::EErrorCode::PipelineFailed,
                    "Error putting blocks to %s",
                    ~target.GetDefaultAddress())
                    << error);
            }
        }));
}

DEFINE_RPC_SERVICE_METHOD(TDataNodeService, GetBlocks)
{
    auto chunkId = FromProto<TChunkId>(request->chunk_id());
    int blockCount = static_cast<int>(request->block_indexes_size());
    bool enableCaching = request->enable_caching();
    auto sessionType = EReadSessionType(request->session_type());

    context->SetRequestInfo("ChunkId: %s, Blocks: [%s], EnableCaching: %s, SessionType: %s",
        ~ToString(chunkId),
        ~JoinToString(request->block_indexes()),
        ~FormatBool(enableCaching),
        ~sessionType.ToString());

    bool isThrottling = IsOutThrottling();

    auto chunkStore = Bootstrap->GetChunkStore();
    auto blockStore = Bootstrap->GetBlockStore();
    auto peerBlockTable = Bootstrap->GetPeerBlockTable();

    bool hasCompleteChunk = chunkStore->FindChunk(chunkId);
    response->set_has_complete_chunk(hasCompleteChunk);

    response->Attachments().resize(blockCount);

    // NB: All callbacks should be handled in the control thread.
    auto awaiter = New<TParallelAwaiter>(Bootstrap->GetControlInvoker());

    // Assign decreasing priorities to block requests to take advantage of sequential read.
    i64 priority = context->GetPriority();

    for (int index = 0; index < blockCount; ++index) {
        int blockIndex = request->block_indexes(index);
        TBlockId blockId(chunkId, blockIndex);

        auto* blockInfo = response->add_blocks();

        if (isThrottling) {
            // Cannot send the actual data to the client due to throttling.
            // Let's try to suggest some other peers.
            blockInfo->set_data_attached(false);
            const auto& peers = peerBlockTable->GetPeers(blockId);
            if (!peers.empty()) {
                FOREACH (const auto& peer, peers) {
                    ToProto(blockInfo->add_p2p_descriptors(), peer.Descriptor);
                }
                LOG_DEBUG("GetBlocks: %" PRISZT " peers suggested for block %d",
                    peers.size(),
                    blockIndex);
            }
        } else {
            // Fetch the actual data (either from cache or from disk).
            LOG_DEBUG("GetBlocks: Fetching block %d", blockIndex);
            awaiter->Await(
                blockStore->GetBlock(blockId, priority, enableCaching),
                BIND([=] (TBlockStore::TGetBlockResult result) {
                    if (result.IsOK()) {
                        // Attach the real data.
                        blockInfo->set_data_attached(true);
                        auto block = result.Value();
                        response->Attachments()[index] = block->GetData();
                        LOG_DEBUG("GetBlocks: Fetched block %d", blockIndex);
                    } else if (result.GetCode() == NChunkClient::EErrorCode::NoSuchChunk) {
                        // This is really sad. We neither have the full chunk nor this particular block.
                        blockInfo->set_data_attached(false);
                        LOG_DEBUG("GetBlocks: Chunk is missing, block %d is not cached", blockIndex);
                    } else {
                        // Something went wrong while fetching the block.
                        // The most probable cause is that a non-existing block was requested.
                        awaiter->Cancel();
                        context->Reply(result);
                    }
                }));
            --priority;
        }
    }

    awaiter->Complete(BIND(&TDataNodeService::OnGotBlocks, MakeStrong(this), context));
}

void TDataNodeService::OnGotBlocks(TCtxGetBlocksPtr context)
{
    auto* request = &context->Request();
    auto* response = &context->Response();

    auto chunkId = FromProto<TChunkId>(request->chunk_id());
    int blockCount = static_cast<int>(request->block_indexes_size());
    auto sessionType = EReadSessionType(request->session_type());

    auto peerBlockTable = Bootstrap->GetPeerBlockTable();

    // Compute statistics.
    int blocksWithData = 0;
    int blocksWithP2P = 0;
    FOREACH (const auto& blockInfo, response->blocks()) {
        if (blockInfo.data_attached()) {
            ++blocksWithData;
        }
        if (blockInfo.p2p_descriptors_size() != 0) {
            ++blocksWithP2P;
        }
    }

    i64 totalSize = 0;
    FOREACH (const auto& block, response->Attachments()) {
        totalSize += block.Size();
    }

    // Register the peer that we had just sent the reply to.
    if (request->has_peer_descriptor() && request->has_peer_expiration_time()) {
        auto descriptor = FromProto<TNodeDescriptor>(request->peer_descriptor());
        auto expirationTime = TInstant(request->peer_expiration_time());
        TPeerInfo peerInfo(descriptor, expirationTime);
        for (int index = 0; index < blockCount; ++index) {
            if (response->blocks(index).data_attached()) {
                TBlockId blockId(chunkId, request->block_indexes(index));
                peerBlockTable->UpdatePeer(blockId, peerInfo);
            }
        }
    }

    context->SetResponseInfo("HasCompleteChunk: %s, BlocksWithData: %d, BlocksWithP2P: %d",
        ~FormatBool(response->has_complete_chunk()),
        blocksWithData,
        blocksWithP2P);

    auto throttler = Bootstrap->GetOutThrottler(sessionType);
    throttler->Throttle(totalSize).Subscribe(BIND([=] () {
        context->Reply();
    }));
}

DEFINE_RPC_SERVICE_METHOD(TDataNodeService, FlushBlock)
{
    UNUSED(response);

    auto chunkId = FromProto<TChunkId>(request->chunk_id());
    int blockIndex = request->block_index();

    context->SetRequestInfo("ChunkId: %s, Block: %d",
        ~ToString(chunkId),
        blockIndex);

    auto session = GetSession(chunkId);

    session->FlushBlock(blockIndex).Subscribe(BIND([=] (TError error) {
        context->Reply(error);
    }));
}

DEFINE_RPC_SERVICE_METHOD(TDataNodeService, PingSession)
{
    UNUSED(response);

    auto chunkId = FromProto<TChunkId>(request->chunk_id());

    context->SetRequestInfo("ChunkId: %s", ~ToString(chunkId));

    auto session = GetSession(chunkId);
    session->Ping();

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TDataNodeService, GetChunkMeta)
{
    auto chunkId = FromProto<TChunkId>(request->chunk_id());
    auto extensionTags = FromProto<int>(request->extension_tags());
    auto partitionTag =
        request->has_partition_tag()
        ? TNullable<int>(request->partition_tag())
        : Null;

    context->SetRequestInfo("ChunkId: %s, AllExtensionTags: %s, ExtensionTags: [%s], PartitionTag: %s",
        ~ToString(chunkId),
        ~FormatBool(request->all_extension_tags()),
        ~JoinToString(extensionTags),
        ~ToString(partitionTag));

    auto chunk = GetChunk(chunkId);
    auto asyncChunkMeta = chunk->GetMeta(
        context->GetPriority(),
        request->all_extension_tags() ? nullptr : &extensionTags);

    asyncChunkMeta.Subscribe(BIND(&TDataNodeService::OnGotChunkMeta,
        Unretained(this),
        context,
        partitionTag).Via(WorkerThread->GetInvoker()));
}

DEFINE_RPC_SERVICE_METHOD(TDataNodeService, PrecacheChunk)
{
    auto chunkId = FromProto<TChunkId>(request->chunk_id());

    context->SetRequestInfo("ChunkId: %s", ~ToString(chunkId));

    Bootstrap
        ->GetChunkCache()
        ->DownloadChunk(chunkId)
        .Subscribe(BIND([=] (TChunkCache::TDownloadResult result) {
            if (result.IsOK()) {
                context->Reply();
            } else {
                context->Reply(TError(
                    NChunkClient::EErrorCode::ChunkPrecachingFailed,
                    "Error precaching chunk %s",
                    ~ToString(chunkId))
                    << result);
            }
        }));
}

DEFINE_ONE_WAY_RPC_SERVICE_METHOD(TDataNodeService, UpdatePeer)
{
    auto descriptor = FromProto<TNodeDescriptor>(request->peer_descriptor());
    auto expirationTime = TInstant(request->peer_expiration_time());
    TPeerInfo peer(descriptor, expirationTime);

    context->SetRequestInfo("Descriptor: %s, ExpirationTime: %s, BlockCount: %d",
        ~ToString(descriptor),
        ~ToString(expirationTime),
        request->block_ids_size());

    auto peerBlockTable = Bootstrap->GetPeerBlockTable();
    FOREACH (const auto& block_id, request->block_ids()) {
        TBlockId blockId(FromProto<TGuid>(block_id.chunk_id()), block_id.block_index());
        peerBlockTable->UpdatePeer(blockId, peer);
    }
}

DEFINE_RPC_SERVICE_METHOD(TDataNodeService, GetTableSamples)
{
    context->SetRequestInfo("KeyColumnCount: %d, ChunkCount: %d",
        request->key_columns_size(),
        request->sample_requests_size());

    auto awaiter = New<TParallelAwaiter>(WorkerThread->GetInvoker());
    auto keyColumns = FromProto<Stroka>(request->key_columns());

    FOREACH (const auto& sampleRequest, request->sample_requests()) {
        auto* chunkSamples = response->add_samples();
        auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());
        auto chunk = Bootstrap->GetChunkStore()->FindChunk(chunkId);

        if (!chunk) {
            LOG_WARNING("GetTableSamples: No such chunk %s\n",
                ~ToString(chunkId));
            ToProto(chunkSamples->mutable_error(), TError("No such chunk"));
        } else {
            awaiter->Await(
                chunk->GetMeta(context->GetPriority()),
                BIND(
                    &TDataNodeService::ProcessSample,
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

void TDataNodeService::ProcessSample(
    const TReqGetTableSamples::TSampleRequest* sampleRequest,
    TRspGetTableSamples::TChunkSamples* chunkSamples,
    const TKeyColumns& keyColumns,
    TChunk::TGetMetaResult result)
{
    auto chunkId = FromProto<TChunkId>(sampleRequest->chunk_id());

    if (!result.IsOK()) {
        LOG_WARNING(result, "GetTableSamples: Error getting meta of chunk %s",
            ~ToString(chunkId));
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
            if (size >= MaxSampleSize)
                break;

            auto* keyPart = key->add_parts();
            auto it = std::lower_bound(
                sample.parts().begin(),
                sample.parts().end(),
                column,
                [] (const NTableClient::NProto::TSamplePart& part, const Stroka& column) {
                    return part.column() < column;
            });

            size += sizeof(int); // part type
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
                    auto partSize = std::min(it->key_part().str_value().size(), MaxSampleSize - size);
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

DEFINE_RPC_SERVICE_METHOD(TDataNodeService, GetChunkSplits)
{
    context->SetRequestInfo("KeyColumnCount: %d, ChunkCount: %d, MinSplitSize: %" PRId64,
        request->key_columns_size(),
        request->chunk_specs_size(),
        request->min_split_size());

    auto awaiter = New<TParallelAwaiter>(WorkerThread->GetInvoker());
    auto keyColumns = FromProto<Stroka>(request->key_columns());

    FOREACH (const auto& chunkSpec, request->chunk_specs()) {
        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
        auto* splittedChunk = response->add_splitted_chunks();
        auto chunk = Bootstrap->GetChunkStore()->FindChunk(chunkId);

        if (!chunk) {
            auto error = TError("No such chunk: %s", ~ToString(chunkId));
            LOG_ERROR(error);
            ToProto(splittedChunk->mutable_error(), error);
        } else {
            awaiter->Await(
                chunk->GetMeta(context->GetPriority()),
                BIND(
                    &TDataNodeService::MakeChunkSplits,
                    MakeStrong(this),
                    &chunkSpec,
                    splittedChunk,
                    request->min_split_size(),
                    keyColumns));
        }
    }

    awaiter->Complete(BIND([=] () {
        context->Reply();
    }));
}

void TDataNodeService::MakeChunkSplits(
    const TChunkSpec* chunkSpec,
    TRspGetChunkSplits::TChunkSplits* splittedChunk,
    i64 minSplitSize,
    const TKeyColumns& keyColumns,
    TChunk::TGetMetaResult result)
{
    auto chunkId = FromProto<TChunkId>(chunkSpec->chunk_id());

    if (!result.IsOK()) {
        auto error = TError("GetChunkSplits: Error getting meta of chunk %s", ~ToString(chunkId))
            << result;
        LOG_ERROR(error);
        ToProto(splittedChunk->mutable_error(), error);
        return;
    }

    if (result.Value().type() != EChunkType::Table) {
        auto error =  TError("GetChunkSplits: Requested chunk splits for non-table chunk %s",
            ~ToString(chunkId));
        LOG_ERROR(error);
        ToProto(splittedChunk->mutable_error(), error);
        return;
    }

    auto miscExt = GetProtoExtension<TMiscExt>(result.Value().extensions());
    if (!miscExt.sorted()) {
        auto error =  TError("GetChunkSplits: Requested chunk splits for unsorted chunk %s",
            ~ToString(chunkId));
        LOG_ERROR(error);
        ToProto(splittedChunk->mutable_error(), error);
        return;
    }

    auto keyColumnsExt = GetProtoExtension<NTableClient::NProto::TKeyColumnsExt>(result.Value().extensions());
    if (keyColumnsExt.values_size() < keyColumns.size()) {
        auto error = TError("Not enough key columns in chunk %s: expected %d, actual %d",
            ~ToString(chunkId),
            static_cast<int>(keyColumns.size()),
            static_cast<int>(keyColumnsExt.values_size()));
        LOG_ERROR(error);
        ToProto(splittedChunk->mutable_error(), error);
        return;
    }

    for (int i = 0; i < keyColumns.size(); ++i) {
        Stroka value = keyColumnsExt.values(i);
        if (keyColumns[i] != value) {
            auto error = TError("Invalid key columns for chunk %s: expected %s, actual %s",
                ~ToString(chunkId),
                ~keyColumns[i],
                ~value);
            LOG_ERROR(error);
            ToProto(splittedChunk->mutable_error(), error);
            return;
        }
    }

    auto indexExt = GetProtoExtension<NTableClient::NProto::TIndexExt>(result.Value().extensions());
    if (indexExt.items_size() == 1) {
        // Only one index entry available - no need to split.
        splittedChunk->add_chunk_specs()->CopyFrom(*chunkSpec);
        return;
    }

    auto back = --indexExt.items().end();
    auto dataSizeBetweenSamples = static_cast<i64>(std::ceil(
        float(back->row_index()) /
        miscExt.row_count() *
        miscExt.uncompressed_data_size() /
        indexExt.items_size()));
    YCHECK(dataSizeBetweenSamples > 0);

    auto comparer = [&] (
        const TReadLimit& limit,
        const NTableClient::NProto::TIndexRow& indexRow,
        bool isStartLimit) -> int
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

    auto beginIt = std::lower_bound(
        indexExt.items().begin(),
        indexExt.items().end(),
        chunkSpec->start_limit(),
        [&] (const NTableClient::NProto::TIndexRow& indexRow,
             const TReadLimit& limit)
        {
            return comparer(limit, indexRow, true) > 0;
        });

    auto endIt = std::upper_bound(
        beginIt,
        indexExt.items().end(),
        chunkSpec->end_limit(),
        [&] (const TReadLimit& limit,
             const NTableClient::NProto::TIndexRow& indexRow)
        {
            return comparer(limit, indexRow, false) < 0;
        });

    if (std::distance(beginIt, endIt) < 2) {
        // Too small distance between given read limits.
        splittedChunk->add_chunk_specs()->CopyFrom(*chunkSpec);
        return;
    }

    TChunkSpec* currentSplit;
    NTableClient::NProto::TBoundaryKeysExt boundaryKeysExt;
    i64 endRowIndex = beginIt->row_index();
    i64 startRowIndex;
    i64 dataSize;

    auto createNewSplit = [&] () {
        currentSplit = splittedChunk->add_chunk_specs();
        currentSplit->CopyFrom(*chunkSpec);
        boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(chunkSpec->extensions());
        startRowIndex = endRowIndex;
        dataSize = 0;
    };
    createNewSplit();

    auto samplesLeft = std::distance(beginIt, endIt) - 1;
    YCHECK(samplesLeft > 0);

    while (samplesLeft > 0) {
        ++beginIt;
        --samplesLeft;
        dataSize += dataSizeBetweenSamples;

        auto nextIter = beginIt + 1;
        if (nextIter == endIt) {
            break;
        }

        if (samplesLeft * dataSizeBetweenSamples < minSplitSize) {
            break;
        }

        if (CompareKeys(nextIter->key(), beginIt->key(), keyColumns.size()) == 0) {
            continue;
        }

        if (dataSize > minSplitSize) {
            auto key = beginIt->key();

            *boundaryKeysExt.mutable_end() = key;

            // Sanity check.
            YCHECK(CompareKeys(boundaryKeysExt.start(), boundaryKeysExt.end()) <= 0);

            SetProtoExtension(currentSplit->mutable_extensions(), boundaryKeysExt);

            endRowIndex = beginIt->row_index();

            TSizeOverrideExt sizeOverride;
            sizeOverride.set_row_count(endRowIndex - startRowIndex);
            sizeOverride.set_uncompressed_data_size(dataSize);
            SetProtoExtension(currentSplit->mutable_extensions(), sizeOverride);

            key = GetKeySuccessor(key);
            *currentSplit->mutable_end_limit()->mutable_key() = key;

            createNewSplit();
            *boundaryKeysExt.mutable_start() = key;
            *currentSplit->mutable_start_limit()->mutable_key() = key;
        }
    }

    // Sanity check.
    YCHECK(CompareKeys(boundaryKeysExt.start(), boundaryKeysExt.end()) <= 0);
    SetProtoExtension(currentSplit->mutable_extensions(), boundaryKeysExt);
    endRowIndex = (--endIt)->row_index();

    TSizeOverrideExt sizeOverride;
    sizeOverride.set_row_count(endRowIndex - startRowIndex);
    sizeOverride.set_uncompressed_data_size(
        dataSize +
        (std::distance(beginIt, endIt)) * dataSizeBetweenSamples);
    SetProtoExtension(currentSplit->mutable_extensions(), sizeOverride);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
