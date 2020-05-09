#include "data_node_service.h"
#include "private.h"
#include "chunk_block_manager.h"
#include "chunk.h"
#include "chunk_cache.h"
#include "chunk_registry.h"
#include "chunk_store.h"
#include "config.h"
#include "location.h"
#include "master_connector.h"
#include "network_statistics.h"
#include "peer_block_table.h"
#include "peer_block_updater.h"
#include "peer_block_distributor.h"
#include "session.h"
#include "session_manager.h"
#include "table_schema_cache.h"

#include <yt/server/node/cluster_node/bootstrap.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/chunk_slice.h>
#include <yt/ytlib/chunk_client/proto/data_node_service.pb.h>
#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/key_set.h>

#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/chunk_state.h>
#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/samples_fetcher.h>
#include <yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/versioned_reader.h>

#include <yt/server/node/data_node/local_chunk_reader.h>
#include <yt/server/node/data_node/lookup_session.h>

#include <yt/server/node/tablet_node/sorted_dynamic_comparer.h>
#include <yt/server/node/tablet_node/versioned_chunk_meta_manager.h>

#include <yt/client/chunk_client/proto/chunk_spec.pb.h>
#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/misc/workload.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/bus/bus.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/random.h>
#include <yt/core/misc/serialize.h>
#include <yt/core/misc/string.h>

#include <yt/core/rpc/service_detail.h>

#include <cmath>

namespace NYT::NDataNode {

using namespace NRpc;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NProfiling;

using NChunkClient::TChunkReaderStatistics;
using NYT::FromProto;

using TRefCountedColumnarStatisticsSubresponse = TRefCountedProto<TRspGetColumnarStatistics::TSubresponse>;
using TRefCountedColumnarStatisticsSubresponsePtr = TIntrusivePtr<TRefCountedColumnarStatisticsSubresponse>;

////////////////////////////////////////////////////////////////////////////////

class TDataNodeService
    : public TServiceBase
{
public:
    TDataNodeService(
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetStorageLightInvoker(),
            TDataNodeServiceProxy::GetDescriptor(),
            DataNodeLogger)
        , Config_(config)
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Bootstrap_);

        // TODO(prime): disable RPC attachment checksums for methods receiving/returning blocks
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CancelChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks)
            .SetQueueSizeLimit(5000)
            .SetConcurrencyLimit(5000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks)
            .SetQueueSizeLimit(5000)
            .SetConcurrencyLimit(5000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PopulateCache)
            .SetQueueSizeLimit(5000)
            .SetConcurrencyLimit(5000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlocks)
            .SetQueueSizeLimit(5000)
            .SetConcurrencyLimit(5000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockSet)
            .SetCancelable(true)
            .SetQueueSizeLimit(5000)
            .SetConcurrencyLimit(5000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockRange)
            .SetCancelable(true)
            .SetQueueSizeLimit(5000)
            .SetConcurrencyLimit(5000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupRows)
            .SetCancelable(true)
            .SetQueueSizeLimit(5000)
            .SetConcurrencyLimit(5000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta)
            .SetCancelable(true)
            .SetQueueSizeLimit(5000)
            .SetConcurrencyLimit(5000)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdatePeer));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableSamples)
            .SetCancelable(true)
            .SetHeavy(true)
            .SetResponseCodec(NCompression::ECodec::Lz4));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkSlices)
            .SetCancelable(true)
            .SetHeavy(true)
            .SetResponseCodec(NCompression::ECodec::Lz4));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetColumnarStatistics)
            .SetCancelable(true));
    }

private:
    const TDataNodeConfigPtr Config_;
    TBootstrap* const Bootstrap_;


    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, StartChunk)
    {
        Y_UNUSED(response);

        auto sessionId = FromProto<TSessionId>(request->session_id());

        TSessionOptions options;
        options.WorkloadDescriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());
        options.SyncOnClose = request->sync_on_close();
        options.EnableMultiplexing = request->enable_multiplexing();
        options.PlacementId = FromProto<TPlacementId>(request->placement_id());
        options.EnableWriteDirectIO = ShouldUseDirectIO(Config_->UseDirectIO, request->enable_direct_io());

        context->SetRequestInfo("ChunkId: %v, Workload: %v, SyncOnClose: %v, EnableMultiplexing: %v, "
            "PlacementId: %v",
            sessionId,
            options.WorkloadDescriptor,
            options.SyncOnClose,
            options.EnableMultiplexing,
            options.PlacementId);

        ValidateConnected();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->StartSession(sessionId, options);
        context->ReplyFrom(session->Start());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FinishChunk)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        auto blockCount = request->has_block_count() ? std::make_optional(request->block_count()) : std::nullopt;

        context->SetRequestInfo("ChunkId: %v, BlockCount: %v",
            sessionId,
            blockCount);

        ValidateConnected();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);

        auto meta = request->has_chunk_meta()
            ? New<TRefCountedChunkMeta>(std::move(*request->mutable_chunk_meta()))
            : nullptr;
        session->Finish(meta, blockCount)
            .Subscribe(BIND([=] (const TErrorOr<TChunkInfo>& chunkInfoOrError) {
                if (!chunkInfoOrError.IsOK()) {
                    context->Reply(chunkInfoOrError);
                    return;
                }

                *response->mutable_chunk_info() = chunkInfoOrError.Value();
                context->Reply();
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, CancelChunk)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo("ChunkId: %v",
            sessionId);

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        session->Cancel(TError("Canceled by client request"));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PingSession)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo("ChunkId: %v",
            sessionId);

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        const auto& location = session->GetStoreLocation();
        
        session->Ping();

        response->set_close_demanded(location->IsSick() || sessionManager->GetDisableWriteSessions());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PutBlocks)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        int firstBlockIndex = request->first_block_index();
        int blockCount = static_cast<int>(request->Attachments().size());
        int lastBlockIndex = firstBlockIndex + blockCount - 1;
        bool populateCache = request->populate_cache();
        bool flushBlocks = request->flush_blocks();

        ValidateConnected();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        const auto& location = session->GetStoreLocation();

        context->SetRequestInfo("BlockIds: %v:%v-%v, PopulateCache: %v, FlushBlocks: %v, Medium: %v",
            sessionId,
            firstBlockIndex,
            lastBlockIndex,
            populateCache,
            flushBlocks,
            location->GetMediumName());

        if (location->GetPendingIOSize(EIODirection::Write, session->GetWorkloadDescriptor()) > Config_->DiskWriteThrottlingLimit) {
            const auto& locationProfiler = location->GetProfiler();
            locationProfiler.Increment(location->GetPerformanceCounters().ThrottledWrites);
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::WriteThrottlingActive, "Disk write throttling is active");
        }

        TWallTimer timer;

        response->set_close_demanded(location->IsSick() || sessionManager->GetDisableWriteSessions());

        // NB: block checksums are validated before writing to disk.
        auto result = session->PutBlocks(
            firstBlockIndex,
            GetRpcAttachedBlocks(request, /* validateChecksums */ false),
            populateCache);

        // Flush blocks if needed.
        if (flushBlocks) {
            result = result.Apply(BIND([=] () {
                return session->FlushBlocks(lastBlockIndex);
            }));
        }

        result.Subscribe(BIND([=] (const TError& error) {
            if (!error.IsOK()) {
                return;
            }
            const auto& profiler = location->GetProfiler();
            profiler.Update(location->GetPerformanceCounters().PutBlocksWallTime, DurationToValue(timer.GetElapsedTime()));
        }));

        context->ReplyFrom(result);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, SendBlocks)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();
        int lastBlockIndex = firstBlockIndex + blockCount - 1;
        auto targetDescriptor = FromProto<TNodeDescriptor>(request->target_descriptor());

        context->SetRequestInfo("BlockIds: %v:%v-%v, Target: %v",
            sessionId,
            firstBlockIndex,
            lastBlockIndex,
            targetDescriptor);

        ValidateConnected();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        session->SendBlocks(firstBlockIndex, blockCount, targetDescriptor)
            .Subscribe(BIND([=] (const TDataNodeServiceProxy::TErrorOrRspPutBlocksPtr& errorOrRsp) {
                if (errorOrRsp.IsOK()) {
                    response->set_close_demanded(errorOrRsp.Value()->close_demanded());
                    context->Reply();
                } else {
                    context->Reply(TError(
                        NChunkClient::EErrorCode::SendBlocksFailed,
                        "Error putting blocks to %v",
                        targetDescriptor.GetDefaultAddress())
                        << errorOrRsp);
                }
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FlushBlocks)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        int blockIndex = request->block_index();

        context->SetRequestInfo("BlockId: %v:%v",
            sessionId,
            blockIndex);

        ValidateConnected();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        const auto& location = session->GetStoreLocation();
        auto result = session->FlushBlocks(blockIndex);

        response->set_close_demanded(location->IsSick() || sessionManager->GetDisableWriteSessions());
        context->ReplyFrom(result);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PopulateCache)
    {
        context->SetRequestInfo("BlockCount: %v", request->blocks_size());

        ValidateConnected();

        auto blocks = GetRpcAttachedBlocks(request, true /* validateChecksums */);

        if (blocks.size() != request->blocks_size()) {
            THROW_ERROR_EXCEPTION("Number of attached blocks is different from blocks field length")
                << TErrorAttribute("attached_block_count", blocks.size())
                << TErrorAttribute("blocks_length", request->blocks_size());
        }

        const auto& blockManager = Bootstrap_->GetChunkBlockManager();
        for (size_t index = 0; index < blocks.size(); ++index) {
            const auto& block = blocks[index];
            const auto& protoBlock = request->blocks(index);
            auto blockId = FromProto<TBlockId>(protoBlock.block_id());
            auto sourceDescriptor = protoBlock.has_source_descriptor()
                ? std::make_optional(FromProto<TNodeDescriptor>(protoBlock.source_descriptor()))
                : std::nullopt;
            blockManager->PutCachedBlock(blockId, block, sourceDescriptor);
        }

        // We mimic TPeerBlockUpdater behavior here.
        const auto& peerBlockUpdater = Bootstrap_->GetPeerBlockUpdater();
        auto expirationTime = peerBlockUpdater->GetPeerUpdateExpirationTime().ToDeadLine();
        response->set_expiration_time(ToProto<i64>(expirationTime));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());
        bool populateCache = request->populate_cache();
        bool fetchFromCache = request->fetch_from_cache();
        bool fetchFromDisk = request->fetch_from_disk();
        auto workloadDescriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());

        context->SetRequestInfo("BlockIds: %v:%v, PopulateCache: %v, FetchFromCache: %v, "
            "FetchFromDisk: %v, Workload: %v",
            chunkId,
            MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3),
            populateCache,
            fetchFromCache,
            fetchFromDisk,
            workloadDescriptor);

        ValidateConnected();

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);
        bool hasCompleteChunk = chunk.operator bool();
        response->set_has_complete_chunk(hasCompleteChunk);

        i64 diskQueueSize = GetDiskReadQueueSize(chunk, workloadDescriptor);
        response->set_disk_queue_size(diskQueueSize);

        bool diskThrottling = diskQueueSize > Config_->DiskReadThrottlingLimit;
        response->set_disk_throttling(diskThrottling);
        if (diskThrottling) {
            const auto& location = chunk->GetLocation();
            const auto& locationProfiler = location->GetProfiler();
            locationProfiler.Increment(location->GetPerformanceCounters().ThrottledReads);
        }

        const auto& throttler = Bootstrap_->GetOutThrottler(workloadDescriptor);
        i64 netThrottlerQueueSize = throttler->GetQueueTotalCount();
        i64 netOutQueueSize = context->GetBusStatistics().PendingOutBytes;
        i64 netQueueSize = netThrottlerQueueSize + netOutQueueSize;

        response->set_net_queue_size(netQueueSize);

        bool netThrottling = netQueueSize > Config_->NetOutThrottlingLimit;
        response->set_net_throttling(netThrottling);
        if (netThrottling) {
            Bootstrap_->GetNetworkStatistics().IncrementReadThrottlingCounter(
                context->GetEndpointAttributes().Get("network", DefaultNetworkName));
        }

        // Try suggesting other peers. This can never hurt.
        bool registerRequestingPeer = request->has_peer_node_id() && request->has_peer_expiration_time();
        const auto& peerBlockTable = Bootstrap_->GetPeerBlockTable();
        for (int blockIndex : request->block_indexes()) {
            auto blockId = TBlockId(chunkId, blockIndex);
            auto peerData = peerBlockTable->FindOrCreatePeerData(blockId, registerRequestingPeer);
            if (peerData) {
                auto peers = peerData->GetPeers();
                if (!peers.empty()) {
                    auto* peerDescriptor = response->add_peer_descriptors();
                    peerDescriptor->set_block_index(blockIndex);
                    for (auto peer : peers) {
                        peerDescriptor->add_node_ids(peer);
                    }
                    YT_LOG_DEBUG("Peers suggested (BlockId: %v, PeerCount: %v)",
                        blockId,
                        peers.size());
                }
            }
            if (registerRequestingPeer) {
                // Register the peer we're replying to.
                peerData->AddPeer(request->peer_node_id(), FromProto<TInstant>(request->peer_expiration_time()));
            }
        }

        auto chunkReaderStatistics = New<TChunkReaderStatistics>();

        if (fetchFromCache || fetchFromDisk) {
            TBlockReadOptions options;
            options.WorkloadDescriptor = workloadDescriptor;
            options.PopulateCache = populateCache;
            options.BlockCache = Bootstrap_->GetBlockCache();
            options.FetchFromCache = fetchFromCache && !netThrottling;
            options.FetchFromDisk = fetchFromDisk && !netThrottling && !diskThrottling;
            options.ChunkReaderStatistics = chunkReaderStatistics;
            if (context->GetTimeout() && context->GetStartTime()) {
                options.Deadline = *context->GetStartTime() + *context->GetTimeout() * Config_->BlockReadTimeoutFraction;
            }

            const auto& chunkBlockManager = Bootstrap_->GetChunkBlockManager();
            auto asyncBlocks = chunkBlockManager->ReadBlockSet(
                chunkId,
                blockIndexes,
                options);

            auto blocks = WaitFor(asyncBlocks)
                .ValueOrThrow();
            for (int index = 0; index < blocks.size() && index < blockIndexes.size(); ++index) {
                if (const auto& block = blocks[index]) {
                    Bootstrap_->GetPeerBlockDistributor()->OnBlockRequested(
                        TBlockId(chunkId, blockIndexes[index]),
                        block.Size());
                }
            }
            SetRpcAttachedBlocks(response, blocks);
        }

        ToProto(response->mutable_chunk_reader_statistics(), chunkReaderStatistics);

        int blocksWithData = 0;
        for (const auto& block : response->Attachments()) {
            if (block) {
                ++blocksWithData;
            }
        }

        i64 blocksSize = GetByteSize(response->Attachments());

        context->SetResponseInfo(
            "HasCompleteChunk: %v, NetThrottling: %v, NetOutQueueSize: %v, "
            "NetThrottlerQueueSize: %v, DiskThrottling: %v, DiskQueueSize: %v, "
            "BlocksWithData: %v, BlocksWithPeers: %v, BlocksSize: %v",
            hasCompleteChunk,
            netThrottling,
            netOutQueueSize,
            netThrottlerQueueSize,
            diskThrottling,
            diskQueueSize,
            blocksWithData,
            response->peer_descriptors_size(),
            blocksSize);

        // NB: We throttle only heavy responses that contain a non-empty attachment
        // as we want responses containing the information about disk/net throttling
        // to be delivered immediately.
        if (blocksSize > 0) {
            context->SetComplete();
            context->ReplyFrom(throttler->Throttle(blocksSize));
        } else {
            context->Reply();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockRange)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto workloadDescriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());
        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();
        bool populateCache = request->populate_cache();
        bool fetchFromCache = request->fetch_from_cache();
        bool fetchFromDisk = request->fetch_from_disk();

        context->SetRequestInfo(
            "BlockIds: %v:%v-%v, PopulateCache: %v, FetchFromCache: %v, "
            "FetchFromDisk: %v, Workload: %v",
            chunkId,
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            populateCache,
            fetchFromCache,
            fetchFromDisk,
            workloadDescriptor);

        ValidateConnected();

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);
        bool hasCompleteChunk = chunk.operator bool();
        response->set_has_complete_chunk(hasCompleteChunk);

        i64 diskQueueSize = GetDiskReadQueueSize(chunk, workloadDescriptor);
        response->set_disk_queue_size(diskQueueSize);

        bool diskThrottling = diskQueueSize > Config_->DiskReadThrottlingLimit;
        response->set_disk_throttling(diskThrottling);
        if (diskThrottling) {
            const auto& location = chunk->GetLocation();
            const auto& locationProfiler = location->GetProfiler();
            locationProfiler.Increment(location->GetPerformanceCounters().ThrottledReads);
        }

        const auto& throttler = Bootstrap_->GetOutThrottler(workloadDescriptor);
        i64 netThrottlerQueueSize = throttler->GetQueueTotalCount();
        i64 netOutQueueSize = context->GetBusStatistics().PendingOutBytes;
        i64 netQueueSize = netThrottlerQueueSize + netOutQueueSize;

        response->set_net_queue_size(netQueueSize);

        bool netThrottling = netQueueSize > Config_->NetOutThrottlingLimit;
        response->set_net_throttling(netThrottling);
        if (netThrottling) {
            Bootstrap_->GetNetworkStatistics().IncrementReadThrottlingCounter(
                context->GetEndpointAttributes().Get("network", DefaultNetworkName));
        }

        auto chunkReaderStatistics = New<TChunkReaderStatistics>();

        if (fetchFromCache || fetchFromDisk) {
            TBlockReadOptions options;
            options.WorkloadDescriptor = workloadDescriptor;
            options.PopulateCache = populateCache;
            options.BlockCache = Bootstrap_->GetBlockCache();
            options.FetchFromCache = fetchFromCache && !netThrottling;
            options.FetchFromDisk = fetchFromDisk && !netThrottling && !diskThrottling;
            options.ChunkReaderStatistics = chunkReaderStatistics;
            if (context->GetTimeout() && context->GetStartTime()) {
                options.Deadline = *context->GetStartTime() + *context->GetTimeout() * Config_->BlockReadTimeoutFraction;
            }

            const auto& chunkBlockManager = Bootstrap_->GetChunkBlockManager();
            auto asyncBlocks = chunkBlockManager->ReadBlockRange(
                chunkId,
                firstBlockIndex,
                blockCount,
                options);

            auto blocks = WaitFor(asyncBlocks)
                .ValueOrThrow();
            SetRpcAttachedBlocks(response, blocks);
        }

        ToProto(response->mutable_chunk_reader_statistics(), chunkReaderStatistics);

        int blocksWithData = static_cast<int>(response->Attachments().size());
        i64 blocksSize = GetByteSize(response->Attachments());

        context->SetResponseInfo(
            "HasCompleteChunk: %v, NetThrottling: %v, NetOutQueueSize: %v, "
            "NetThrottlerQueueSize: %v, DiskThrottling: %v, DiskQueueSize: %v, "
            "BlocksWithData: %v, BlocksSize: %v",
            hasCompleteChunk,
            netThrottling,
            netOutQueueSize,
            netThrottlerQueueSize,
            diskThrottling,
            diskQueueSize,
            blocksWithData,
            blocksSize);

        // NB: We throttle only heavy responses that contain a non-empty attachment
        // as we want responses containing the information about disk/net throttling
        // to be delivered immediately.
        if (blocksSize > 0) {
            context->SetComplete();
            context->ReplyFrom(throttler->Throttle(blocksSize));
        } else {
            context->Reply();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, LookupRows)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto readSessionId = FromProto<TReadSessionId>(request->read_session_id());
        auto workloadDescriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());

        context->SetRequestInfo("ChunkId: %v, ReadSessionId: %v, Workload: %v",
            chunkId,
            readSessionId,
            workloadDescriptor);

        ValidateConnected();

        auto schemaData = request->schema_data();
        auto [tableSchema, schemaRequested] = TLookupSession::FindTableSchema(
            chunkId,
            readSessionId,
            schemaData,
            Bootstrap_->GetTableSchemaCache());

        if (!tableSchema) {
            // NB: No throttling here.
            response->set_fetched_rows(false);
            response->set_request_schema(schemaRequested);
            context->SetResponseInfo("ChunkId: %v, ReadSessionId: %v, Workload: %v, SchemaRequested: %v",
                chunkId,
                readSessionId,
                workloadDescriptor,
                schemaRequested);
            context->Reply();
            return;
        }
        YT_VERIFY(!schemaRequested);

        auto timestamp = request->timestamp();
        auto columnFilter = FromProto<NTableClient::TColumnFilter>(request->column_filter());
        auto codecId = CheckedEnumCast<NCompression::ECodec>(request->compression_codec());
        auto produceAllVersions = FromProto<bool>(request->produce_all_versions());

        TLookupSession lookupSession(
            Bootstrap_,
            chunkId,
            readSessionId,
            workloadDescriptor,
            std::move(columnFilter),
            timestamp,
            produceAllVersions,
            tableSchema,
            request->lookup_keys(),
            codecId);
        response->Attachments().push_back(lookupSession.Run());

        response->set_fetched_rows(true);
        ToProto(response->mutable_chunk_reader_statistics(), lookupSession.GetChunkReaderStatistics());

        context->SetResponseInfo("ChunkId: %v, ReadSessionId: %v",
            chunkId,
            readSessionId);

        auto throttler = Bootstrap_->GetOutThrottler(workloadDescriptor);
        context->SetComplete();
        context->ReplyFrom(throttler->Throttle(GetByteSize(response->Attachments())));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto partitionTag = request->has_partition_tag()
            ? std::make_optional(request->partition_tag())
            : std::nullopt;
        auto extensionTags = request->all_extension_tags()
            ? std::nullopt
            : std::make_optional(FromProto<std::vector<int>>(request->extension_tags()));
        auto workloadDescriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());

        context->SetRequestInfo("ChunkId: %v, ExtensionTags: %v, PartitionTag: %v, Workload: %v, EnableThrottling: %v",
            chunkId,
            extensionTags,
            partitionTag,
            workloadDescriptor,
            request->enable_throttling());

        ValidateConnected();

        if (request->enable_throttling() && context->GetBusStatistics().PendingOutBytes > Config_->NetOutThrottlingLimit) {
            response->set_net_throttling(true);
            Bootstrap_->GetNetworkStatistics().IncrementReadThrottlingCounter(
                context->GetEndpointAttributes().Get("network", DefaultNetworkName));
            context->Reply();
            return;
        }

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->GetChunkOrThrow(chunkId, AllMediaIndex);

        TBlockReadOptions options;
        options.WorkloadDescriptor = workloadDescriptor;
        options.ChunkReaderStatistics = New<TChunkReaderStatistics>();

        auto asyncChunkMeta = chunk->ReadMeta(
            options,
            extensionTags);

        context->ReplyFrom(asyncChunkMeta.Apply(BIND([=] (const TRefCountedChunkMetaPtr& meta) {
            if (context->IsCanceled()) {
                throw TFiberCanceledException();
            }

            if (partitionTag) {
                auto cachedBlockMeta = Bootstrap_->GetBlockMetaCache()->Find(chunkId);
                if (!cachedBlockMeta) {
                    auto blockMetaExt = GetProtoExtension<TBlockMetaExt>(meta->extensions());
                    cachedBlockMeta = New<TCachedBlockMeta>(chunkId, std::move(blockMetaExt));
                    Bootstrap_->GetBlockMetaCache()->TryInsert(cachedBlockMeta);
                }

                *response->mutable_chunk_meta() = FilterChunkMetaByPartitionTag(*meta, cachedBlockMeta, *partitionTag);
            } else {
                *response->mutable_chunk_meta() = static_cast<TChunkMeta>(*meta);
            }

            ToProto(response->mutable_chunk_reader_statistics(), options.ChunkReaderStatistics);
            ToProto(response->mutable_location_uuid(), chunk->GetLocation()->GetUuid());
        }).AsyncVia(Bootstrap_->GetStorageHeavyInvoker())));
    }

    template <class TRequests>
    TFuture<std::vector<TErrorOr<TRefCountedChunkMetaPtr>>> GetChunkMetasForRequests(
        const TWorkloadDescriptor& workloadDescriptor,
        const TRequests& requests)
    {
        TBlockReadOptions options;
        options.WorkloadDescriptor = workloadDescriptor;
        options.ChunkReaderStatistics = New<TChunkReaderStatistics>();

        std::vector<TFuture<TRefCountedChunkMetaPtr>> chunkMetaFutures;
        for (const auto& request : requests) {
            auto chunkId = FromProto<TChunkId>(request.chunk_id());
            auto chunk = Bootstrap_->GetChunkStore()->FindChunk(chunkId);
            if (chunk) {
                chunkMetaFutures.push_back(chunk->ReadMeta(options));
            } else {
                chunkMetaFutures.push_back(MakeFuture<TRefCountedChunkMetaPtr>(TError(
                    NChunkClient::EErrorCode::NoSuchChunk,
                    "No such chunk %v",
                    chunkId)));
            }
        }
        return CombineAll(chunkMetaFutures);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkSlices)
    {
        auto requestCount = request->slice_requests_size();
        auto workloadDescriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());

        context->SetRequestInfo(
            "RequestCount: %v, Workload: %v",
            requestCount,
            workloadDescriptor);

        ValidateConnected();

        GetChunkMetasForRequests(workloadDescriptor, request->slice_requests())
            .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TErrorOr<NChunkClient::TRefCountedChunkMetaPtr>>>& resultsError) {
                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                if (context->IsCanceled()) {
                    return;
                }

                const auto& results = resultsError.Value();
                YT_VERIFY(results.size() == requestCount);

                auto keySetWriter = New<TKeySetWriter>();

                for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
                    // COMPAT(gritukan)
                    auto sliceRequest = request->slice_requests(requestIndex);
                    if (request->has_slice_data_weight()) {
                        sliceRequest.set_slice_data_weight(request->slice_data_weight());
                    }
                    if (request->has_key_column_count()) {
                        sliceRequest.set_key_column_count(request->key_column_count());
                    }
                    if (request->has_slice_by_keys()) {
                        sliceRequest.set_slice_by_keys(request->slice_by_keys());
                    }

                    ProcessSlice(
                        sliceRequest,
                        response->add_slice_responses(),
                        keySetWriter,
                        results[requestIndex]);
                }

                response->Attachments().push_back(keySetWriter->Finish());
                context->Reply();
            }).Via(Bootstrap_->GetStorageHeavyInvoker()));
    }

    void ProcessSlice(
        const TSliceRequest& sliceRequest,
        TRspGetChunkSlices::TSliceResponse* sliceResponse,
        const TKeySetWriterPtr& keySetWriter,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        auto chunkId = FromProto<TChunkId>(sliceRequest.chunk_id());
        try {
            THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Error getting meta of chunk %v",
                chunkId);

            const auto& chunkMeta = metaOrError.Value();
            auto type = CheckedEnumCast<EChunkType>(chunkMeta->type());
            if (type != EChunkType::Table) {
                THROW_ERROR_EXCEPTION("Invalid type of chunk %v: expected %Qlv, actual %Qlv",
                    chunkId,
                    EChunkType::Table,
                    type);
            }

            auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta->extensions());
            if (!miscExt.sorted()) {
                THROW_ERROR_EXCEPTION("Chunk %v is not sorted", chunkId);
            }

            // COMPAT(savrus) Support schemaful and versioned chunks.
            TKeyColumns chunkKeyColumns;
            auto optionalKeyColumnsExt = FindProtoExtension<TKeyColumnsExt>(chunkMeta->extensions());
            if (optionalKeyColumnsExt) {
                chunkKeyColumns = FromProto<TKeyColumns>(*optionalKeyColumnsExt);
            } else {
                auto schemaExt = GetProtoExtension<TTableSchemaExt>(chunkMeta->extensions());
                chunkKeyColumns = FromProto<TTableSchema>(schemaExt).GetKeyColumns();
            }

            auto format = CheckedEnumCast<ETableChunkFormat>(chunkMeta->version());
            auto versioned =
                format == ETableChunkFormat::VersionedSimple ||
                format == ETableChunkFormat::VersionedColumnar;

            // NB(psushin): we don't validate key names, because possible column renaming could have happened.
            ValidateKeyColumnCount(
                sliceRequest.key_column_count(),
                chunkKeyColumns.size(),
                versioned);

            auto slices = SliceChunk(
                sliceRequest,
                *chunkMeta);

            for (const auto& slice : slices) {
                ToProto(keySetWriter, sliceResponse->add_chunk_slices(), slice);
            }
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_WARNING(error, "Error building chunk slices (ChunkId: %v)",
                chunkId);
            ToProto(sliceResponse->mutable_error(), error);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetTableSamples)
    {
        auto samplingPolicy = ESamplingPolicy(request->sampling_policy());
        auto keyColumns = FromProto<TKeyColumns>(request->key_columns());
        auto requestCount = request->sample_requests_size();
        auto maxSampleSize = request->max_sample_size();
        auto workloadDescriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());

        context->SetRequestInfo("SamplingPolicy: %v, KeyColumns: %v, RequestCount: %v, Workload: %v",
            samplingPolicy,
            keyColumns,
            requestCount,
            workloadDescriptor);

        ValidateConnected();

        GetChunkMetasForRequests(workloadDescriptor, request->sample_requests())
            .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TErrorOr<NChunkClient::TRefCountedChunkMetaPtr>>>& resultsError) {
                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                if (context->IsCanceled()) {
                    return;
                }

                const auto& results = resultsError.Value();
                YT_VERIFY(results.size() == requestCount);

                auto keySetWriter = New<TKeySetWriter>();

                for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
                    ProcessChunkSamples(
                        request->sample_requests(requestIndex),
                        response->add_sample_responses(),
                        samplingPolicy,
                        keyColumns,
                        maxSampleSize,
                        keySetWriter,
                        results[requestIndex]);
                }

                response->Attachments().push_back(keySetWriter->Finish());
                context->Reply();
            }).Via(Bootstrap_->GetStorageHeavyInvoker()));
    }

    void ProcessChunkSamples(
        const TReqGetTableSamples::TSampleRequest& sampleRequest,
        TRspGetTableSamples::TChunkSamples* sampleResponse,
        ESamplingPolicy samplingPolicy,
        const TKeyColumns& keyColumns,
        i32 maxSampleSize,
        const TKeySetWriterPtr& keySetWriter,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());
        try {
            THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Error getting meta of chunk %v",
                chunkId);
            const auto& meta = *metaOrError.Value();

            auto type = CheckedEnumCast<EChunkType>(meta.type());
            if (type != EChunkType::Table) {
                THROW_ERROR_EXCEPTION("Invalid type of chunk %v: expected %Qlv, actual %Qlv",
                    chunkId,
                    EChunkType::Table,
                    type);
            }

            switch (samplingPolicy) {
                case ESamplingPolicy::Sorting:
                    ProcessSortingSamples(sampleRequest, sampleResponse, keyColumns, maxSampleSize, keySetWriter, meta);
                    break;

                case ESamplingPolicy::Partitioning:
                    ProcessPartitioningSamples(sampleRequest, sampleResponse, keyColumns, keySetWriter, meta);
                    break;

                default:
                    YT_ABORT();
            }

        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_WARNING(error, "Error building chunk samples (ChunkId: %v)",
                chunkId);
            ToProto(sampleResponse->mutable_error(), error);
        }
    }

    static void SerializeSample(
        TRspGetTableSamples::TSample* protoSample,
        std::vector<TUnversionedValue> values,
        i32 maxSampleSize,
        i64 weight,
        const TKeySetWriterPtr& keySetWriter)
    {
        size_t size = 0;
        bool incomplete = false;
        for (auto& value : values) {
            auto valueSize = GetByteSize(value);
            if (incomplete || size >= maxSampleSize) {
                incomplete = true;
                value = MakeUnversionedSentinelValue(EValueType::Null);
            } else if (size + valueSize > maxSampleSize && IsStringLikeType(value.Type)) {
                value.Length = maxSampleSize - size;
                YT_VERIFY(value.Length > 0);
                size += value.Length;
                incomplete = true;
            } else {
                size += valueSize;
            }
        }

        protoSample->set_key_index(keySetWriter->WriteValueRange(MakeRange(values)));
        protoSample->set_incomplete(incomplete);
        protoSample->set_weight(weight);
    }


    void ProcessPartitioningSamples(
        const TReqGetTableSamples::TSampleRequest& sampleRequest,
        TRspGetTableSamples::TChunkSamples* chunkSamples,
        const TKeyColumns& keyColumns,
        const TKeySetWriterPtr& keySetWriter,
        const TChunkMeta& chunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());

        // COMPAT(psushin)
        TKeyColumns chunkKeyColumns;
        auto optionalKeyColumnsExt = FindProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
        if (optionalKeyColumnsExt) {
            chunkKeyColumns = FromProto<TKeyColumns>(*optionalKeyColumnsExt);
        } else {
            auto schemaExt = GetProtoExtension<TTableSchemaExt>(chunkMeta.extensions());
            chunkKeyColumns = FromProto<TTableSchema>(schemaExt).GetKeyColumns();
        }

        bool isCompatibleKeyColumns =
            keyColumns.size() >= chunkKeyColumns.size() &&
            std::equal(
                chunkKeyColumns.begin(),
                chunkKeyColumns.end(),
                keyColumns.begin());

        // Requested key can be wider than stored.
        if (!isCompatibleKeyColumns) {
            auto error = TError("Incompatible key columns in chunk %v: requested key columns %v, chunk key columns %v",
                chunkId,
                keyColumns,
                chunkKeyColumns);
            YT_LOG_WARNING(error);
            ToProto(chunkSamples->mutable_error(), error);
            return;
        }

        auto lowerKey = sampleRequest.has_lower_key()
            ? FromProto<TOwningKey>(sampleRequest.lower_key())
            : MinKey();

        auto upperKey = sampleRequest.has_upper_key()
            ? FromProto<TOwningKey>(sampleRequest.upper_key())
            : MaxKey();

        auto blocksExt = GetProtoExtension<TBlockMetaExt>(chunkMeta.extensions());

        std::vector<TOwningKey> samples;
        for (const auto& block : blocksExt.blocks()) {
            YT_VERIFY(block.has_last_key());
            auto key = FromProto<TOwningKey>(block.last_key());
            if (key >= lowerKey && key < upperKey) {
                samples.push_back(WidenKey(key, keyColumns.size()));
            }
        }

        // Don't return more than requested.
        std::random_shuffle(samples.begin(), samples.end());
        auto count = std::min(
            static_cast<int>(samples.size()),
            sampleRequest.sample_count());
        samples.erase(samples.begin() + count, samples.end());

        for (const auto& sample : samples) {
            auto* protoSample = chunkSamples->add_samples();
            protoSample->set_key_index(keySetWriter->WriteKey(sample));
            protoSample->set_incomplete(false);
            protoSample->set_weight(1);
        }
    }

    void ProcessSortingSamples(
        const TReqGetTableSamples::TSampleRequest& sampleRequest,
        TRspGetTableSamples::TChunkSamples* chunkSamples,
        const TKeyColumns& keyColumns,
        i32 maxSampleSize,
        const TKeySetWriterPtr& keySetWriter,
        const TChunkMeta& chunkMeta)
    {
        TNameTablePtr nameTable;
        std::vector<int> keyIds;

        try {
            auto nameTableExt = FindProtoExtension<TNameTableExt>(chunkMeta.extensions());
            if (nameTableExt) {
                nameTable = FromProto<TNameTablePtr>(*nameTableExt);
            } else {
                auto schemaExt = GetProtoExtension<TTableSchemaExt>(chunkMeta.extensions());
                nameTable = TNameTable::FromSchema(FromProto<TTableSchema>(schemaExt));
            }

            for (const auto& column : keyColumns) {
                keyIds.push_back(nameTable->GetIdOrRegisterName(column));
            }
        } catch (const std::exception& ex) {
            auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());
            YT_LOG_WARNING(ex, "Failed to gather chunk samples (ChunkId: %v)",
                chunkId);

            // We failed to deserialize name table, so we don't return any samples.
            return;
        }

        std::vector<int> idToKeyIndex(nameTable->GetSize(), -1);
        for (int i = 0; i < keyIds.size(); ++i) {
            idToKeyIndex[keyIds[i]] = i;
        }

        auto samplesExt = GetProtoExtension<TSamplesExt>(chunkMeta.extensions());

        // TODO(psushin): respect sampleRequest lower_limit and upper_limit.
        // Old chunks do not store samples weights.
        bool hasWeights = samplesExt.weights_size() > 0;
        for (int index = 0;
            index < samplesExt.entries_size() && chunkSamples->samples_size() < sampleRequest.sample_count();
            ++index)
        {
            int remaining = samplesExt.entries_size() - index;
            if (std::rand() % remaining >= sampleRequest.sample_count() - chunkSamples->samples_size()) {
                continue;
            }

            auto row = FromProto<TUnversionedOwningRow>(samplesExt.entries(index));
            std::vector<TUnversionedValue> values(
                keyColumns.size(),
                MakeUnversionedSentinelValue(EValueType::Null));

            for (const auto& value : row) {
                int keyIndex = idToKeyIndex[value.Id];
                if (keyIndex < 0) {
                    continue;
                }
                values[keyIndex] = value;
            }

            SerializeSample(
                chunkSamples->add_samples(),
                std::move(values),
                maxSampleSize,
                hasWeights ? samplesExt.weights(index) : samplesExt.entries(index).length(),
                keySetWriter);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetColumnarStatistics)
    {
        auto workloadDescriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());

        context->SetRequestInfo(
            "SubrequestCount: %v, Workload: %v",
            request->subrequests_size(),
            workloadDescriptor);

        ValidateConnected();

        const auto& chunkStore = Bootstrap_->GetChunkStore();

        std::vector<TRefCountedColumnarStatisticsSubresponsePtr> subresponses;
        std::vector<TFuture<void>> asyncResults;

        auto nameTable = FromProto<TNameTablePtr>(request->name_table());

        auto heavyInvoker = CreateSerializedInvoker(Bootstrap_->GetStorageHeavyInvoker());
        for (const auto& subrequest : request->subrequests()) {
            auto chunkId = FromProto<TChunkId>(subrequest.chunk_id());

            auto subresponse = New<TRefCountedColumnarStatisticsSubresponse>();
            subresponses.emplace_back(subresponse);

            auto chunk = chunkStore->FindChunk(chunkId);
            if (!chunk) {
                auto error = TError(
                    NChunkClient::EErrorCode::NoSuchChunk,
                    "No such chunk %v",
                    chunkId);
                YT_LOG_WARNING(error);
                ToProto(subresponse->mutable_error(), error);
                continue;
            }

            auto columnIds = FromProto<std::vector<int>>(subrequest.column_ids());
            std::vector<TString> columnNames;
            for (auto id : columnIds) {
                columnNames.emplace_back(nameTable->GetNameOrThrow(id));
            }

            TBlockReadOptions options;
            options.WorkloadDescriptor = workloadDescriptor;
            options.ChunkReaderStatistics = New<TChunkReaderStatistics>();

            auto asyncChunkMeta = chunk->ReadMeta(options);
            asyncResults.push_back(asyncChunkMeta.Apply(
                BIND(
                    &TDataNodeService::ProcessColumnarStatistics,
                    MakeStrong(this),
                    Passed(std::move(columnNames)),
                    chunkId,
                    Passed(std::move(subresponse)))
                    .AsyncVia(heavyInvoker)));
        }

        auto combinedResult = Combine(asyncResults);
        context->SubscribeCanceled(BIND([combinedResult = combinedResult] {
            combinedResult.Cancel(TError("RPC request canceled"));
        }));
        context->ReplyFrom(combinedResult.Apply(BIND([subresponses = std::move(subresponses), response] () mutable {
            for (int index = 0; index < subresponses.size(); ++index) {
                response->add_subresponses()->Swap(subresponses[index].Get());
            }
        })));
    }

    void ProcessColumnarStatistics(
        std::vector<TString> columnNames,
        TChunkId chunkId,
        TRefCountedColumnarStatisticsSubresponsePtr subresponse,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        try {
            THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Error getting meta of chunk %v",
                chunkId);
            const auto& meta = *metaOrError.Value();

            auto type = EChunkType(meta.type());
            if (type != EChunkType::Table) {
                THROW_ERROR_EXCEPTION("Invalid type of chunk %v: expected %Qlv, actual %Qlv",
                    chunkId,
                    EChunkType::Table,
                    type);
            }

            auto optionalColumnarStatisticsExt = FindProtoExtension<TColumnarStatisticsExt>(meta.extensions());
            if (!optionalColumnarStatisticsExt) {
                ToProto(
                    subresponse->mutable_error(),
                    TError(NChunkClient::EErrorCode::MissingExtension, "Columnar statistics chunk meta extension missing"));
                return;
            }
            const auto& columnarStatisticsExt = *optionalColumnarStatisticsExt;

            auto nameTableExt = FindProtoExtension<TNameTableExt>(meta.extensions());
            TNameTablePtr nameTable;
            if (nameTableExt) {
                nameTable = FromProto<TNameTablePtr>(*nameTableExt);
            } else {
                auto schemaExt = GetProtoExtension<TTableSchemaExt>(meta.extensions());
                nameTable = TNameTable::FromSchema(FromProto<TTableSchema>(schemaExt));
            }

            for (const auto& columnName : columnNames) {
                auto id = nameTable->FindId(columnName);
                if (id && *id < columnarStatisticsExt.data_weights().size()) {
                    subresponse->add_data_weights(columnarStatisticsExt.data_weights(*id));
                } else {
                    subresponse->add_data_weights(0);
                }
            }

            if (columnarStatisticsExt.has_timestamp_weight()) {
                subresponse->set_timestamp_total_weight(columnarStatisticsExt.timestamp_weight());
            }
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_WARNING(error);
            ToProto(subresponse->mutable_error(), error);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, UpdatePeer)
    {
        auto expirationTime = FromProto<TInstant>(request->peer_expiration_time());

        const auto& nodeDirectory = Bootstrap_->GetNodeDirectory();
        const auto* nodeDescriptor = nodeDirectory->FindDescriptor(request->peer_node_id());

        context->SetRequestInfo("PeerNodeId: %v, PeerAddress: %v, ExpirationTime: %v, BlockCount: %v",
            request->peer_node_id(),
            (nodeDescriptor ? nodeDescriptor : &NullNodeDescriptor())->GetDefaultAddress(),
            expirationTime,
            request->block_ids_size());

        const auto& peerBlockTable = Bootstrap_->GetPeerBlockTable();
        for (const auto& protoBlockId : request->block_ids()) {
            auto blockId = FromProto<TBlockId>(protoBlockId);
            auto peerData = peerBlockTable->FindOrCreatePeerData(blockId, true);
            peerData->AddPeer(request->peer_node_id(), expirationTime);
        }

        context->Reply();
    }


    void ValidateConnected()
    {
        if (!Bootstrap_->GetMasterConnector()->IsConnected()) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::MasterNotConnected,
                "Master is not connected");
        }
    }


    static bool ShouldUseDirectIO(EDirectIOPolicy policy, bool writerRequestedDirectIO)
    {
        if (policy == EDirectIOPolicy::Never) {
            return false;
        }

        if (policy == EDirectIOPolicy::Always) {
            return true;
        }

        return writerRequestedDirectIO;
    }

    static i64 GetDiskReadQueueSize(const IChunkPtr& chunk, const TWorkloadDescriptor& workloadDescriptor)
    {
        if (!chunk) {
            return 0;
        }
        return chunk->GetLocation()->GetPendingIOSize(EIODirection::Read, workloadDescriptor);
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateDataNodeService(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
{
    return New<TDataNodeService>(
        config,
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
