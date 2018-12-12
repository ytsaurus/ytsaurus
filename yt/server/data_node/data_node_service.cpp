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
#include "peer_block_table.h"
#include "peer_block_updater.h"
#include "peer_block_distributor.h"
#include "session.h"
#include "session_manager.h"
#include "network_statistics.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/chunk_slice.h>
#include <yt/client/chunk_client/proto/chunk_spec.pb.h>
#include <yt/ytlib/chunk_client/data_node_service.pb.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/key_set.h>
#include <yt/client/chunk_client/read_limit.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/client/misc/workload.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/samples_fetcher.h>

#include <yt/core/bus/bus.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

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
using namespace NCellNode;
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
            bootstrap->GetControlInvoker(),
            TDataNodeServiceProxy::GetDescriptor(),
            DataNodeLogger)
        , Config_(config)
        , Bootstrap_(bootstrap)
    {
        YCHECK(Config_);
        YCHECK(Bootstrap_);

        // TODO(prime): disable RPC attachment checksums for methods receiving/returning blocks
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CancelChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks)
            .SetMaxQueueSize(5000)
            .SetMaxConcurrency(5000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks)
            .SetMaxQueueSize(5000)
            .SetMaxConcurrency(5000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PopulateCache)
            .SetMaxQueueSize(5000)
            .SetMaxConcurrency(5000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlocks)
            .SetMaxQueueSize(5000)
            .SetMaxConcurrency(5000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockSet)
            .SetCancelable(true)
            .SetMaxQueueSize(5000)
            .SetMaxConcurrency(5000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockRange)
            .SetCancelable(true)
            .SetMaxQueueSize(5000)
            .SetMaxConcurrency(5000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta)
            .SetCancelable(true)
            .SetMaxQueueSize(5000)
            .SetMaxConcurrency(5000)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdatePeer));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableSamples)
            .SetCancelable(true)
            .SetResponseCodec(NCompression::ECodec::Lz4)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkSlices)
            .SetCancelable(true)
            .SetResponseCodec(NCompression::ECodec::Lz4)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetColumnarStatistics)
            .SetCancelable(true));
    }

private:
    const TDataNodeConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    const TActionQueuePtr WorkerThread_ = New<TActionQueue>("DataNodeWorker");
    const TActionQueuePtr MetaProcessorThread_ = New<TActionQueue>("MetaProcessor");

    bool ShouldUseDirectIO(EDirectIOPolicy policy, bool writerRequestedDirectIO) const
    {
        if (policy == EDirectIOPolicy::Never) {
            return false;
        }

        if (policy == EDirectIOPolicy::Always) {
            return true;
        }

        return writerRequestedDirectIO;
    }

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
        ValidateNoSession(sessionId);
        ValidateNoChunk(sessionId);

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->StartSession(sessionId, options);
        auto result = session->Start();
        context->ReplyFrom(result);
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
            .Subscribe(BIND([=] (const TErrorOr<IChunkPtr>& chunkOrError) {
                if (chunkOrError.IsOK()) {
                    auto chunk = chunkOrError.Value();
                    const auto& chunkInfo = session->GetChunkInfo();
                    *response->mutable_chunk_info() = chunkInfo;
                    context->Reply();
                } else {
                    context->Reply(chunkOrError);
                }
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
        Y_UNUSED(response);

        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo("ChunkId: %v",
            sessionId);

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        session->Ping();

        context->Reply();
    }


    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PutBlocks)
    {
        Y_UNUSED(response);

        auto sessionId = FromProto<TSessionId>(request->session_id());
        int firstBlockIndex = request->first_block_index();
        int blockCount = static_cast<int>(request->Attachments().size());
        int lastBlockIndex = firstBlockIndex + blockCount - 1;
        bool populateCache = request->populate_cache();
        bool flushBlocks = request->flush_blocks();

        ValidateConnected();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        auto location = session->GetStoreLocation();

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

        response->set_location_sick(location->IsSick() || sessionManager->GetDisableWriteSessions());

        // NB: block checksums are validated before writing to disk.
        auto result = session->PutBlocks(
            firstBlockIndex,
            GetRpcAttachedBlocks(request, /* validateChecksum */ false),
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
        Y_UNUSED(response);

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
                    response->set_location_sick(errorOrRsp.Value()->location_sick());
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
        Y_UNUSED(response);

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

        response->set_location_sick(location->IsSick());
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

        auto blockManager = Bootstrap_->GetChunkBlockManager();
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
        auto expirationTime = Bootstrap_->GetPeerBlockUpdater()->GetPeerUpdateExpirationTime().ToDeadLine();

        response->set_expiration_time(expirationTime.GetValue());
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
            MakeShrunkFormattableRange(blockIndexes, TDefaultFormatter(), 3),
            populateCache,
            fetchFromCache,
            fetchFromDisk,
            workloadDescriptor);

        ValidateConnected();

        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
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
            locationProfiler.Increment(location->GetPerformanceCounters().ThrottledWrites);
        }

        const auto& throttler = Bootstrap_->GetOutThrottler(workloadDescriptor);
        i64 netThrottlerQueueSize = throttler->GetQueueTotalCount();
        i64 netOutQueueSize = context->GetBusStatistics().PendingOutBytes;
        i64 netQueueSize = netThrottlerQueueSize + netOutQueueSize;

        response->set_net_queue_size(netQueueSize);

        bool netThrottling = netQueueSize > Config_->NetOutThrottlingLimit;
        response->set_net_throttling(netThrottling);
        if (netThrottling) {
            Bootstrap_->GetNetworkStatistics()->IncrementReadThrottlingCounter(
                context->GetEndpointAttributes().Get("network", DefaultNetworkName));
        }

        // Try suggesting other peers. This can never hurt.
        auto peerBlockTable = Bootstrap_->GetPeerBlockTable();
        for (int blockIndex : request->block_indexes()) {
            auto blockId = TBlockId(chunkId, blockIndex);
            const auto& peers = peerBlockTable->GetPeers(blockId);
            if (!peers.empty()) {
                auto* peerDescriptor = response->add_peer_descriptors();
                peerDescriptor->set_block_index(blockIndex);
                for (const auto& peer : peers) {
                    ToProto(peerDescriptor->add_node_descriptors(), peer.Descriptor);
                }
                YT_LOG_DEBUG("Peers suggested (BlockId: %v, PeerCount: %v)",
                    blockId,
                    peers.size());
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

            const auto& chunkBlockManager =Bootstrap_->GetChunkBlockManager();
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

        // Register the peer that we had just sent the reply to.
        if (request->has_peer_descriptor() && request->has_peer_expiration_time()) {
            auto descriptor = FromProto<TNodeDescriptor>(request->peer_descriptor());
            auto expirationTime = FromProto<TInstant>(request->peer_expiration_time());
            TPeerInfo peerInfo(descriptor, expirationTime);
            for (int blockIndex : request->block_indexes()) {
                peerBlockTable->UpdatePeer(TBlockId(chunkId, blockIndex), peerInfo);
            }
        }

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

        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
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
            Bootstrap_->GetNetworkStatistics()->IncrementReadThrottlingCounter(
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

            const auto& chunkBlockManager =Bootstrap_->GetChunkBlockManager();
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

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto mediumIndex = request->has_medium_index()
            ? request->medium_index()
            : AllMediaIndex;
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
            Bootstrap_->GetNetworkStatistics()->IncrementReadThrottlingCounter(
                context->GetEndpointAttributes().Get("network", DefaultNetworkName));
            context->Reply();
            return;
        }

        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->GetChunkOrThrow(chunkId, mediumIndex);

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
        }).AsyncVia(MetaProcessorThread_->GetInvoker())));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkSlices)
    {
        auto keyColumns = FromProto<TKeyColumns>(request->key_columns());
        auto workloadDescriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());

        context->SetRequestInfo(
            "KeyColumns: %v, ChunkCount: %v, "
            "SliceDataSize: %v, SliceByKeys: %v, KeysInAttachment: %v, Workload: %v",
            keyColumns,
            request->slice_requests_size(),
            request->slice_data_size(),
            request->slice_by_keys(),
            request->keys_in_attachment(),
            workloadDescriptor);

        ValidateConnected();

        std::vector<TFuture<void>> asyncResults;
        TKeySetWriterPtr keySetWriter = request->keys_in_attachment()
            ? New<TKeySetWriter>()
            : nullptr;

        for (const auto& sliceRequest : request->slice_requests()) {
            auto chunkId = FromProto<TChunkId>(sliceRequest.chunk_id());
            auto* slices = response->add_slices();
            auto chunk = Bootstrap_->GetChunkStore()->FindChunk(chunkId);

            if (!chunk) {
                auto error = TError(
                    NChunkClient::EErrorCode::NoSuchChunk,
                    "No such chunk %v",
                    chunkId);
                YT_LOG_WARNING(error);
                ToProto(slices->mutable_error(), error);
                continue;
            }

            TBlockReadOptions options;
            options.WorkloadDescriptor = workloadDescriptor;
            options.ChunkReaderStatistics = New<TChunkReaderStatistics>();

            auto asyncResult = chunk->ReadMeta(options);
            asyncResults.push_back(asyncResult.Apply(
                BIND(
                    &TDataNodeService::MakeChunkSlices,
                    MakeStrong(this),
                    sliceRequest,
                    slices,
                    request->slice_data_size(),
                    request->slice_by_keys(),
                    keyColumns,
                    keySetWriter)
                .AsyncVia(WorkerThread_->GetInvoker())));
        }

        context->ReplyFrom(Combine(asyncResults).Apply(BIND([=] () {
            if (context->IsCanceled()) {
                throw TFiberCanceledException();
            }

            if (keySetWriter) {
                response->set_keys_in_attachment(true);
                response->Attachments().emplace_back(keySetWriter->Finish());
            } else {
                response->set_keys_in_attachment(false);
            }
        }).AsyncVia(WorkerThread_->GetInvoker())));
    }

    void MakeChunkSlices(
        const TSliceRequest& sliceRequest,
        TRspGetChunkSlices::TChunkSlices* result,
        i64 sliceDataSize,
        bool sliceByKeys,
        const TKeyColumns& keyColumns,
        const TKeySetWriterPtr& keySetWriter,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        auto chunkId = FromProto<TChunkId>(sliceRequest.chunk_id());
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

            auto miscExt = GetProtoExtension<TMiscExt>(meta.extensions());
            if (!miscExt.sorted()) {
                THROW_ERROR_EXCEPTION("Chunk %v is not sorted", chunkId);
            }

            // COMPAT(savrus) Support schemaful and versioned chunks.
            TKeyColumns chunkKeyColumns;
            auto optionalKeyColumnsExt = FindProtoExtension<TKeyColumnsExt>(meta.extensions());
            if (optionalKeyColumnsExt) {
                chunkKeyColumns = FromProto<TKeyColumns>(*optionalKeyColumnsExt);
            } else {
                auto schemaExt = GetProtoExtension<TTableSchemaExt>(meta.extensions());
                chunkKeyColumns = FromProto<TTableSchema>(schemaExt).GetKeyColumns();
            }
            auto format = ETableChunkFormat(meta.version());
            auto isVersioned =
                format == ETableChunkFormat::VersionedSimple ||
                format == ETableChunkFormat::VersionedColumnar;

            // NB(psushin): we don't validate key names, because possible column renaming could have happened.
            ValidateKeyColumns(
                keyColumns,
                chunkKeyColumns,
                isVersioned,
                /* validateColumnNames */ false);

            auto slices = SliceChunk(
                sliceRequest,
                meta,
                sliceDataSize,
                keyColumns.size(),
                sliceByKeys);

            if (keySetWriter) {
                for (const auto& slice : slices) {
                    ToProto(keySetWriter, result->add_chunk_slices(), slice);
                }
            } else {
                for (const auto& slice : slices) {
                    ToProto(result->add_chunk_slices(), slice);
                }
            }
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_WARNING(error);
            ToProto(result->mutable_error(), error);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetTableSamples)
    {
        auto samplingPolicy = ESamplingPolicy(request->sampling_policy());
        auto keyColumns = FromProto<TKeyColumns>(request->key_columns());
        auto workloadDescriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());

        context->SetRequestInfo("SamplingPolicy: %v, KeyColumns: %v, ChunkCount: %v, KeysInAttachment: %v, Workload: %v",
            samplingPolicy,
            keyColumns,
            request->sample_requests_size(),
            request->keys_in_attachment(),
            workloadDescriptor);

        ValidateConnected();

        const auto& chunkStore = Bootstrap_->GetChunkStore();

        std::vector<TFuture<void>> asyncResults;
        TKeySetWriterPtr keySetWriter = request->keys_in_attachment()
            ? New<TKeySetWriter>()
            : nullptr;

        for (const auto& sampleRequest : request->sample_requests()) {
            auto* sampleResponse = response->add_sample_responses();
            auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());

            auto chunk = chunkStore->FindChunk(chunkId);
            if (!chunk) {
                auto error = TError(
                    NChunkClient::EErrorCode::NoSuchChunk,
                    "No such chunk %v",
                    chunkId);
                YT_LOG_WARNING(error);
                ToProto(sampleResponse->mutable_error(), error);
                continue;
            }

            TBlockReadOptions options;
            options.WorkloadDescriptor = workloadDescriptor;
            options.ChunkReaderStatistics = New<TChunkReaderStatistics>();

            auto asyncChunkMeta = chunk->ReadMeta(options);
            asyncResults.push_back(asyncChunkMeta.Apply(
                BIND(
                    &TDataNodeService::ProcessSample,
                    MakeStrong(this),
                    &sampleRequest,
                    sampleResponse,
                    samplingPolicy,
                    keyColumns,
                    request->max_sample_size(),
                    keySetWriter)
                .AsyncVia(WorkerThread_->GetInvoker())));
        }

        context->ReplyFrom(Combine(asyncResults).Apply(BIND([=] () {
            if (context->IsCanceled()) {
                throw TFiberCanceledException();
            }

            if (keySetWriter) {
                response->set_keys_in_attachment(true);
                response->Attachments().emplace_back(keySetWriter->Finish());
            } else {
                response->set_keys_in_attachment(false);
            }
        }).AsyncVia(WorkerThread_->GetInvoker())));
    }

    void ProcessSample(
        const TReqGetTableSamples::TSampleRequest* sampleRequest,
        TRspGetTableSamples::TChunkSamples* sampleResponse,
        ESamplingPolicy samplingPolicy,
        const TKeyColumns& keyColumns,
        i32 maxSampleSize,
        const TKeySetWriterPtr& keySetWriter,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        auto chunkId = FromProto<TChunkId>(sampleRequest->chunk_id());
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

            switch (samplingPolicy) {
                case ESamplingPolicy::Sorting:
                    ProcessSortingSamples(sampleRequest, sampleResponse, keyColumns, maxSampleSize, keySetWriter, meta);
                    break;

                case ESamplingPolicy::Partitioning:
                    ProcessPartitioningSamples(sampleRequest, sampleResponse, keyColumns, keySetWriter, meta);
                    break;

                default:
                    Y_UNREACHABLE();
            }

        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_WARNING(error);
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
                YCHECK(value.Length > 0);
                size += value.Length;
                incomplete = true;
            } else {
                size += valueSize;
            }
        }

        if (keySetWriter) {
            protoSample->set_key_index(keySetWriter->WriteValueRange(MakeRange(values)));
        } else {
            ToProto(protoSample->mutable_key(), values.data(), values.data() + values.size());
        }
        protoSample->set_incomplete(incomplete);
        protoSample->set_weight(weight);
    }


    void ProcessPartitioningSamples(
        const TReqGetTableSamples::TSampleRequest* sampleRequest,
        TRspGetTableSamples::TChunkSamples* chunkSamples,
        const TKeyColumns& keyColumns,
        const TKeySetWriterPtr& keySetWriter,
        const TChunkMeta& chunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(sampleRequest->chunk_id());

        // COMPAT(psushin)
        TKeyColumns chunkKeyColumns;
        auto optionalKeyColumnsExt = FindProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
        if (optionalKeyColumnsExt) {
            chunkKeyColumns = NYT::FromProto<TKeyColumns>(*optionalKeyColumnsExt);
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

        auto lowerKey = sampleRequest->has_lower_key()
            ? FromProto<TOwningKey>(sampleRequest->lower_key())
            : MinKey();

        auto upperKey = sampleRequest->has_upper_key()
            ? FromProto<TOwningKey>(sampleRequest->upper_key())
            : MaxKey();

        auto blocksExt = GetProtoExtension<TBlockMetaExt>(chunkMeta.extensions());

        std::vector<TOwningKey> samples;
        for (const auto& block : blocksExt.blocks()) {
            YCHECK(block.has_last_key());
            auto key = FromProto<TOwningKey>(block.last_key());
            if (key >= lowerKey && key < upperKey) {
                samples.push_back(WidenKey(key, keyColumns.size()));
            }
        }

        // Don't return more than requested.
        std::random_shuffle(samples.begin(), samples.end());
        auto count = std::min(
            static_cast<int>(samples.size()),
            sampleRequest->sample_count());
        samples.erase(samples.begin() + count, samples.end());

        for (const auto& sample : samples) {
            auto* protoSample = chunkSamples->add_samples();
            if (keySetWriter) {
                protoSample->set_key_index(keySetWriter->WriteKey(sample));
            } else {
                ToProto(protoSample->mutable_key(), sample);
            }
            protoSample->set_incomplete(false);
            protoSample->set_weight(1);
        }
    }

    void ProcessSortingSamples(
        const TReqGetTableSamples::TSampleRequest* sampleRequest,
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
            auto chunkId = FromProto<TChunkId>(sampleRequest->chunk_id());
            YT_LOG_WARNING(ex, "Failed to gather samples (ChunkId: %v)", chunkId);

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
            index < samplesExt.entries_size() && chunkSamples->samples_size() < sampleRequest->sample_count();
            ++index)
        {
            int remaining = samplesExt.entries_size() - index;
            if (std::rand() % remaining >= sampleRequest->sample_count() - chunkSamples->samples_size()) {
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

        TNameTablePtr nameTable = NYT::FromProto<TNameTablePtr>(request->name_table());

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
                columnNames.emplace_back(nameTable->GetName(id));
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
                    .AsyncVia(WorkerThread_->GetInvoker())));
        }

        auto combinedResult = Combine(asyncResults);
        context->SubscribeCanceled(BIND([combinedResult = combinedResult] () mutable {
            combinedResult.Cancel();
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
            // COMPAT(max42): remove second option when YT-8954 is at least several months old.
            if (!optionalColumnarStatisticsExt || optionalColumnarStatisticsExt->data_weights_size() == 0) {
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
        auto descriptor = FromProto<TNodeDescriptor>(request->peer_descriptor());
        auto expirationTime = FromProto<TInstant>(request->peer_expiration_time());
        TPeerInfo peer(descriptor, expirationTime);

        context->SetRequestInfo("Descriptor: %v, ExpirationTime: %v, BlockCount: %v",
            descriptor,
            expirationTime,
            request->block_ids_size());

        auto peerBlockTable = Bootstrap_->GetPeerBlockTable();
        for (const auto& block_id : request->block_ids()) {
            TBlockId blockId(FromProto<TGuid>(block_id.chunk_id()), block_id.block_index());
            peerBlockTable->UpdatePeer(blockId, peer);
        }

        context->Reply();
    }


    void ValidateConnected()
    {
        auto masterConnector = Bootstrap_->GetMasterConnector();
        if (!masterConnector->IsConnected()) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::MasterNotConnected,
                "Master is not connected");
        }
    }

    void ValidateNoSession(const TSessionId& sessionId)
    {
        if (Bootstrap_->GetSessionManager()->FindSession(sessionId)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::SessionAlreadyExists,
                "Session %v already exists",
                sessionId);
        }
    }

    void ValidateNoChunk(const TSessionId& sessionId)
    {
        if (Bootstrap_->GetChunkStore()->FindChunk(sessionId.ChunkId, sessionId.MediumIndex)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::ChunkAlreadyExists,
                "Chunk %v already exists",
                sessionId);
        }
    }

    i64 GetDiskReadQueueSize(const IChunkPtr& chunk, const TWorkloadDescriptor& workloadDescriptor)
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
