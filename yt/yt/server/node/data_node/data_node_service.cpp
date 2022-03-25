#include "data_node_service.h"
#include "bootstrap.h"
#include "private.h"
#include "ally_replica_manager.h"
#include "chunk_block_manager.h"
#include "chunk.h"
#include "chunk_registry.h"
#include "chunk_store.h"
#include "config.h"
#include "location.h"
#include "legacy_master_connector.h"
#include "network_statistics.h"
#include "p2p.h"
#include "session.h"
#include "session_manager.h"
#include "table_schema_cache.h"
#include "chunk_meta_manager.h"
#include "master_connector.h"

#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/chunk_file_reader.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/key_set.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/samples_fetcher.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/server/lib/rpc/per_workload_category_queue.h>

#include <yt/yt/server/node/data_node/local_chunk_reader.h>
#include <yt/yt/server/node/data_node/lookup_session.h>

#include <yt/yt/server/node/tablet_node/sorted_dynamic_comparer.h>
#include <yt/yt/server/node/tablet_node/versioned_chunk_meta_manager.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>
#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/misc/workload.h>
#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/random.h>
#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/string.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <cmath>

namespace NYT::NDataNode {

using namespace NRpc;
using namespace NIO;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NProfiling;

using NChunkClient::TChunkReaderStatistics;
using NYT::ToProto;
using NYT::FromProto;

using TRefCountedColumnarStatisticsSubresponse = TRefCountedProto<TRspGetColumnarStatistics::TSubresponse>;
using TRefCountedColumnarStatisticsSubresponsePtr = TIntrusivePtr<TRefCountedColumnarStatisticsSubresponse>;

////////////////////////////////////////////////////////////////////////////////

namespace {

THashMap<TString, TString> MakeWriteIOTags(
    TString method,
    const ISessionPtr& session,
    const IServiceContextPtr& context)
{
    auto& location = session->GetStoreLocation();
    return {
        {FormatIOTag(EAggregateIOTag::DataNodeMethod), std::move(method)},
        {FormatIOTag(ERawIOTag::WriteSessionId), ToString(session->GetId())},
        {FormatIOTag(EAggregateIOTag::LocationId), ToString(location->GetId())},
        {FormatIOTag(EAggregateIOTag::Medium), location->GetMediumName()},
        {FormatIOTag(EAggregateIOTag::DiskFamily), location->GetDiskFamily()},
        {FormatIOTag(EAggregateIOTag::User), context->GetAuthenticationIdentity().User},
        {FormatIOTag(EAggregateIOTag::Direction), "write"},
        {FormatIOTag(ERawIOTag::ChunkId), ToString(DecodeChunkId(session->GetChunkId()).Id)},
    };
}

THashMap<TString, TString> MakeReadIOTags(
    TString method,
    const TChunkLocationPtr& location,
    const IServiceContextPtr& context,
    const TChunkId& chunkId,
    TGuid readSessionId = TGuid())
{
    THashMap<TString, TString> result{
        {FormatIOTag(EAggregateIOTag::DataNodeMethod), std::move(method)},
        {FormatIOTag(EAggregateIOTag::LocationId), ToString(location->GetId())},
        {FormatIOTag(EAggregateIOTag::Medium), location->GetMediumName()},
        {FormatIOTag(EAggregateIOTag::DiskFamily), location->GetDiskFamily()},
        {FormatIOTag(EAggregateIOTag::User), context->GetAuthenticationIdentity().User},
        {FormatIOTag(EAggregateIOTag::Direction), "read"},
    };
    if (chunkId) {
        result[FormatIOTag(ERawIOTag::ChunkId)] = ToString(DecodeChunkId(chunkId).Id);
    }
    if (readSessionId) {
        result[FormatIOTag(ERawIOTag::ReadSessionId)] = ToString(readSessionId);
    }
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDataNodeService
    : public TServiceBase
{
public:
    TDataNodeService(
        TDataNodeConfigPtr config,
        IBootstrap* bootstrap)
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
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateP2PBlocks)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlocks)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProbeChunkSet)
            .SetInvoker(Bootstrap_->GetStorageLookupInvoker())
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProbeBlockSet)
            .SetCancelable(true)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockSet)
            .SetCancelable(true)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000)
            .SetRequestQueueProvider(GetBlockSetQueue_.GetProvider()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockRange)
            .SetCancelable(true)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000)
            .SetRequestQueueProvider(GetBlockRangeQueue_.GetProvider()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkFragmentSet)
            .SetInvoker(Bootstrap_->GetStorageLookupInvoker())
            .SetQueueSizeLimit(100'000)
            .SetConcurrencyLimit(100'000)
            .SetRequestQueueProvider(GetChunkFragmentSetQueue_.GetProvider()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupRows)
            .SetInvoker(Bootstrap_->GetStorageLookupInvoker())
            .SetCancelable(true)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta)
            .SetCancelable(true)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000)
            .SetHeavy(true)
            .SetRequestQueueProvider(GetChunkMetaQueue_.GetProvider()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkSliceDataWeights)
            .SetCancelable(true)
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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AnnounceChunkReplicas)
            .SetInvoker(Bootstrap_->GetStorageLightInvoker()));
    }

private:
    const TDataNodeConfigPtr Config_;
    IBootstrap* const Bootstrap_;

    TPerWorkloadCategoryRequestQueue GetBlockSetQueue_;
    TPerWorkloadCategoryRequestQueue GetBlockRangeQueue_;
    TPerWorkloadCategoryRequestQueue GetChunkFragmentSetQueue_;
    TPerWorkloadCategoryRequestQueue GetChunkMetaQueue_;


    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, StartChunk)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        TSessionOptions options;
        options.WorkloadDescriptor = GetRequestWorkloadDescriptor(context);
        options.SyncOnClose = request->sync_on_close();
        options.EnableMultiplexing = request->enable_multiplexing();
        options.PlacementId = FromProto<TPlacementId>(request->placement_id());

        context->SetRequestInfo("ChunkId: %v, Workload: %v, SyncOnClose: %v, EnableMultiplexing: %v, "
            "PlacementId: %v",
            sessionId,
            options.WorkloadDescriptor,
            options.SyncOnClose,
            options.EnableMultiplexing,
            options.PlacementId);

        ValidateOnline();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->StartSession(sessionId, options);
        ToProto(response->mutable_location_uuid(), session->GetStoreLocation()->GetUuid());
        context->ReplyFrom(session->Start());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FinishChunk)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        auto blockCount = request->has_block_count() ? std::make_optional(request->block_count()) : std::nullopt;

        context->SetRequestInfo("ChunkId: %v, BlockCount: %v",
            sessionId,
            blockCount);

        ValidateOnline();

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
                const auto& chunkInfo = chunkInfoOrError.Value();

                // Log IO events for blob chunks only. We don't enable logging for journal chunks here, since
                // they flush the data to disk in FlushBlocks(), not in FinishChunk().
                const auto& ioTracker = Bootstrap_->GetIOTracker();
                bool isBlobChunk = IsBlobChunkId(DecodeChunkId(session->GetChunkId()).Id);
                if (isBlobChunk && chunkInfo.disk_space() > 0 && ioTracker->IsEnabled()) {
                    ioTracker->Enqueue(
                        TIOCounters{.Bytes = chunkInfo.disk_space(), .IORequests = 1},
                        MakeWriteIOTags("FinishChunk", session, context));
                }

                *response->mutable_chunk_info() = chunkInfo;
                context->Reply();
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, CancelChunk)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        bool waitForCancelation = request->wait_for_cancelation();

        context->SetRequestInfo("ChunkId: %v",
            sessionId);

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        session->Cancel(TError("Canceled by client request"));

        if (waitForCancelation) {
            context->ReplyFrom(session->GetUnregisteredEvent());
        } else {
            context->Reply();
        }
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

        response->set_close_demanded(IsCloseDemanded(location));

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

        ValidateOnline();

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
            location->GetPerformanceCounters().ThrottleWrite();
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::WriteThrottlingActive, "Disk write throttling is active");
        }

        TWallTimer timer;

        response->set_close_demanded(IsCloseDemanded(location));

        // NB: block checksums are validated before writing to disk.
        auto result = session->PutBlocks(
            firstBlockIndex,
            GetRpcAttachedBlocks(request, /* validateChecksums */ false),
            populateCache);

        // Flush blocks if needed.
        if (flushBlocks) {
            result = result
                .Apply(BIND([=, this_ = MakeStrong(this)] () mutable {
                    auto result = session->FlushBlocks(lastBlockIndex);
                    result.Subscribe(BIND([=, this_ = MakeStrong(this)] (TErrorOr<TIOCounters> result) {
                        if (!result.IsOK()) {
                            return;
                        }
                        // Log IO events for journal chunks only. We don't enable logging for blob chunks here, since they flush
                        // the data to disk in FinishChunk().
                        const auto& ioTracker = Bootstrap_->GetIOTracker();
                        const auto& counters = result.ValueOrThrow();
                        bool isJournalChunk = IsJournalChunkId(DecodeChunkId(session->GetChunkId()).Id);
                        if (isJournalChunk && counters.Bytes > 0 && ioTracker->IsEnabled()) {
                            ioTracker->Enqueue(
                                counters,
                                MakeWriteIOTags("FlushBlocks", session, context));
                        }
                    }));
                    return result.AsVoid();
                }));
        } else {
            // Remains to wait on throttlers, hence mark as complete.
            context->SetComplete();
        }

        result.Subscribe(BIND([=] (const TError& error) {
            if (!error.IsOK()) {
                return;
            }
            location->GetPerformanceCounters().PutBlocksWallTime.Record(timer.GetElapsedTime());
        }));

        context->ReplyFrom(result, Bootstrap_->GetStorageLightInvoker());
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

        ValidateOnline();

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

        ValidateOnline();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        const auto& location = session->GetStoreLocation();
        auto result = session->FlushBlocks(blockIndex);

        response->set_close_demanded(location->IsSick() || sessionManager->GetDisableWriteSessions());
        result.Subscribe(BIND([=, this_ = MakeStrong(this)] (TErrorOr<TIOCounters> result) {
            if (result.IsOK()) {
                // Log IO events for journal chunks only. We don't enable logging for blob chunks here, since they flush
                // the data to disk in FinishChunk(), not in FlushBlocks().
                const auto& ioTracker = Bootstrap_->GetIOTracker();
                const auto& counters = result.Value();
                if (IsJournalChunkId(session->GetChunkId()) && counters.Bytes > 0 && ioTracker->IsEnabled()) {
                    ioTracker->Enqueue(
                        counters,
                        MakeWriteIOTags("FlushBlocks", session, context));
                }
            }
        }));
        context->ReplyFrom(result.AsVoid());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, UpdateP2PBlocks)
    {
        auto sessionId = FromProto<TGuid>(request->session_id());

        context->SetRequestInfo("SessionId: %v, Iteration: %v, ReceivedBlockCount: %v",
            sessionId,
            request->iteration(),
            request->block_indexes_size());

        ValidateOnline();

        const auto& blockCache = Bootstrap_->GetP2PBlockCache();

        auto blocks = GetRpcAttachedBlocks(request, true /* validateChecksums */);
        if (std::ssize(blocks) != request->block_indexes_size()) {
            THROW_ERROR_EXCEPTION("Number of attached blocks is different from blocks field length")
                << TErrorAttribute("attached_block_count", blocks.size())
                << TErrorAttribute("blocks_length", request->block_indexes_size());
        }

        if (request->chunk_ids_size() != request->chunk_block_count_size()) {
            THROW_ERROR_EXCEPTION("Invalid block count")
                << TErrorAttribute("chunk_count", request->chunk_ids_size())
                << TErrorAttribute("block_count", request->chunk_block_count_size());
        }

        int j = 0;
        for (int i = 0; i < request->chunk_ids_size(); i++) {
            auto chunkId = FromProto<TChunkId>(request->chunk_ids(i));

            auto blockCount = request->chunk_block_count(i);

            std::vector<int> blockIndexes;
            std::vector<NChunkClient::TBlock> chunkBlocks;

            for (int k = 0; k < blockCount; k++) {
                if (j + k >= std::ssize(blocks)) {
                    THROW_ERROR_EXCEPTION("Invalid chunk block count");
                }

                blockIndexes.push_back(request->block_indexes(j + k));
                chunkBlocks.push_back(blocks[j + k]);
            }

            blockCache->HoldBlocks(
                chunkId,
                blockIndexes,
                chunkBlocks);

            j += blockCount;
        }

        blockCache->FinishSessionIteration(sessionId, request->iteration());

        context->Reply();
    }

    template <class TContext>
    void SuggestAllyReplicas(const TContext& context)
    {
        const auto& allyReplicaManager = Bootstrap_->GetAllyReplicaManager();

        const auto* request = &context->Request();
        auto* response = &context->Response();
        auto chunkId = FromProto<TChunkId>(request->chunk_id());

        if (auto allyReplicas = allyReplicaManager->GetAllyReplicas(chunkId)) {
            ToProto(response->mutable_ally_replicas(), allyReplicas);
            YT_LOG_DEBUG("Ally replicas suggested (ChunkId: %v, Replicas: %v)",
                chunkId,
                allyReplicas.Replicas);
        }
    }

    void WaitP2PBarriers(const TReqGetBlockSet* request)
    {
        const auto& p2pBlockCache = Bootstrap_->GetP2PBlockCache();
        for (const auto& barrier : request->wait_barriers()) {
            if (static_cast<TNodeId>(barrier.if_node_id()) != Bootstrap_->GetNodeId()) {
                continue;
            }

            auto p2pSessionId = FromProto<TGuid>(barrier.session_id());
            auto asyncBarrier = p2pBlockCache->WaitSessionIteration(p2pSessionId, barrier.iteration());

            if (!asyncBarrier.IsSet()) {
                YT_LOG_DEBUG("Waiting for P2P barrier (SessionId: %v, Iteration: %v)", p2pSessionId, barrier.iteration());
                WaitFor(asyncBarrier)
                    .ThrowOnError();
            }
        }
    }

    std::vector<NChunkClient::TBlock> ReadBlocksFromP2P(
        const TChunkId chunkId,
        const std::vector<int>& blockIndexes,
        const TChunkReaderStatisticsPtr& chunkReaderStatistics)
    {
        const auto& p2pBlockCache = Bootstrap_->GetP2PBlockCache();

        // New P2P implementation stores blocks in separate block cache.
        auto blocks = p2pBlockCache->LookupBlocks(chunkId, blockIndexes);

        size_t bytesRead = 0;
        for (const auto& block : blocks) {
            if (block) {
                bytesRead += block.Size();
            }
        }
        if (bytesRead > 0) {
            chunkReaderStatistics->DataBytesReadFromCache += bytesRead;
            return blocks;
        }

        blocks = {};

        auto blockCache = Bootstrap_->GetBlockCache();

        // Old P2P implementation stores blocks in shared block cache.
        auto type = NObjectClient::TypeFromId(chunkId);
        if (type == NObjectClient::EObjectType::Chunk || type == NObjectClient::EObjectType::ErasureChunk) {
            for (int blockIndex : blockIndexes) {
                auto blockId = TBlockId(chunkId, blockIndex);
                auto block = blockCache->FindBlock(blockId, EBlockType::CompressedData).Block;
                blocks.push_back(block);
                chunkReaderStatistics->DataBytesReadFromCache += block.Size();
            }
        }
        return blocks;
    }

    void AddBlockPeers(
        google::protobuf::RepeatedPtrField<TPeerDescriptor>* peers,
        const std::vector<TP2PSuggestion>& blockPeers)
    {
        for (const auto& suggestion : blockPeers) {
            auto peerDescriptor = peers->Add();

            peerDescriptor->set_block_index(suggestion.BlockIndex);
            ToProto(peerDescriptor->mutable_node_ids(), suggestion.Peers);

            auto barrier = peerDescriptor->mutable_delivery_barier();

            barrier->set_iteration(suggestion.P2PIteration);
            ToProto(barrier->mutable_session_id(), suggestion.P2PSessionId);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ProbeChunkSet)
    {
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        auto chunkCount = request->chunk_ids_size();
        context->SetRequestInfo("ChunkCount: %v, Workload: %v",
            chunkCount,
            workloadDescriptor);

        ValidateOnline();

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();

        int completeChunkCount = 0;
        for (auto chunkIdProto : request->chunk_ids()) {
            auto chunkId = FromProto<TChunkId>(chunkIdProto);

            auto* subresponse = response->add_subresponses();

            auto chunk = chunkRegistry->FindChunk(chunkId);
            bool hasCompleteChunk = chunk.operator bool();
            subresponse->set_has_complete_chunk(hasCompleteChunk);
            if (hasCompleteChunk) {
                ++completeChunkCount;
            }

            i64 diskReadQueueSize = GetDiskReadQueueSize(chunk, workloadDescriptor);
            auto locationThrottler = GetLocationOutThrottler(chunk, workloadDescriptor);
            i64 diskThrottlerQueueSize = locationThrottler
                ? locationThrottler->GetQueueTotalCount()
                : 0LL;
            i64 diskQueueSize = diskReadQueueSize + diskThrottlerQueueSize;
            subresponse->set_disk_queue_size(diskQueueSize);

            bool diskThrottling = diskQueueSize > Config_->DiskReadThrottlingLimit;
            subresponse->set_disk_throttling(diskThrottling);

            const auto& allyReplicaManager = Bootstrap_->GetAllyReplicaManager();
            if (auto allyReplicas = allyReplicaManager->GetAllyReplicas(chunkId)) {
                ToProto(subresponse->mutable_ally_replicas(), allyReplicas);
            }
        }

        const auto& netThrottler = Bootstrap_->GetOutThrottler(workloadDescriptor);
        i64 netThrottlerQueueSize = netThrottler->GetQueueTotalCount();
        i64 netOutQueueSize = context->GetBusStatistics().PendingOutBytes;
        i64 netQueueSize = netThrottlerQueueSize + netOutQueueSize;
        response->set_net_queue_size(netQueueSize);

        bool netThrottling = netQueueSize > Config_->NetOutThrottlingLimit;
        response->set_net_throttling(netThrottling);

        context->SetResponseInfo(
            "ChunkCount: %v, CompleteChunkCount: %v, "
            "NetThrottling: %v, NetOutQueueSize: %v, NetThrottlerQueueSize: %v",
            chunkCount,
            completeChunkCount,
            netThrottling,
            netOutQueueSize,
            netThrottlerQueueSize);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ProbeBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo("BlockIds: %v:%v, Workload: %v",
            chunkId,
            MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3),
            workloadDescriptor);

        ValidateOnline();

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);
        bool hasCompleteChunk = chunk.operator bool();
        response->set_has_complete_chunk(hasCompleteChunk);

        i64 diskReadQueueSize = GetDiskReadQueueSize(chunk, workloadDescriptor);
        auto locationThrottler = GetLocationOutThrottler(chunk, workloadDescriptor);
        i64 diskThrottlerQueueSize = locationThrottler
            ? locationThrottler->GetQueueTotalCount()
            : 0LL;
        i64 diskQueueSize = diskReadQueueSize + diskThrottlerQueueSize;
        response->set_disk_queue_size(diskQueueSize);

        bool diskThrottling = diskQueueSize > Config_->DiskReadThrottlingLimit;
        response->set_disk_throttling(diskThrottling);

        const auto& netThrottler = Bootstrap_->GetOutThrottler(workloadDescriptor);
        i64 netThrottlerQueueSize = netThrottler->GetQueueTotalCount();
        i64 netOutQueueSize = context->GetBusStatistics().PendingOutBytes;
        i64 netQueueSize = netThrottlerQueueSize + netOutQueueSize;
        response->set_net_queue_size(netQueueSize);

        bool netThrottling = netQueueSize > Config_->NetOutThrottlingLimit;
        response->set_net_throttling(netThrottling);

        SuggestAllyReplicas(context);

        const auto& p2pSnooper = Bootstrap_->GetP2PSnooper();
        AddBlockPeers(response->mutable_peer_descriptors(), p2pSnooper->OnBlockProbe(chunkId, blockIndexes));

        context->SetResponseInfo(
            "HasCompleteChunk: %v, "
            "NetThrottling: %v, NetOutQueueSize: %v, NetThrottlerQueueSize: %v, "
            "DiskThrottling: %v, DiskReadQueueSize: %v, DiskThrottlerQueueSize: %v "
            "PeerDescriptorCount: %v",
            hasCompleteChunk,
            netThrottling,
            netOutQueueSize,
            netThrottlerQueueSize,
            diskThrottling,
            diskReadQueueSize,
            diskThrottlerQueueSize,
            response->peer_descriptors_size());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());
        bool populateCache = request->populate_cache();
        bool fetchFromCache = request->fetch_from_cache();
        bool fetchFromDisk = request->fetch_from_disk();
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);
        auto readSessionId = request->has_read_session_id()
            ? FromProto<TReadSessionId>(request->read_session_id())
            : TReadSessionId{};

        context->SetRequestInfo("BlockIds: %v:%v, PopulateCache: %v, FetchFromCache: %v, "
            "FetchFromDisk: %v, Workload: %v",
            chunkId,
            MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3),
            populateCache,
            fetchFromCache,
            fetchFromDisk,
            workloadDescriptor);

        ValidateOnline();

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);

        bool hasCompleteChunk = chunk.operator bool();
        response->set_has_complete_chunk(hasCompleteChunk);

        i64 diskReadQueueSize = GetDiskReadQueueSize(chunk, workloadDescriptor);
        auto locationThrottler = GetLocationOutThrottler(chunk, workloadDescriptor);
        i64 diskThrottlerQueueSize = locationThrottler
            ? locationThrottler->GetQueueTotalCount()
            : 0LL;
        i64 diskQueueSize = diskReadQueueSize + diskThrottlerQueueSize;
        response->set_disk_queue_size(diskQueueSize);

        bool diskThrottling = diskQueueSize > Config_->DiskReadThrottlingLimit;
        response->set_disk_throttling(diskThrottling);
        if (diskThrottling && chunk) {
            const auto& location = chunk->GetLocation();
            location->GetPerformanceCounters().ThrottleRead();
        }

        const auto& netThrottler = Bootstrap_->GetOutThrottler(workloadDescriptor);

        i64 netThrottlerQueueSize, netOutQueueSize;
        auto checkNetThrottling = [&] (i64 limit) {
            netThrottlerQueueSize = netThrottler->GetQueueTotalCount();
            netOutQueueSize = context->GetBusStatistics().PendingOutBytes;
            i64 netQueueSize = netThrottlerQueueSize + netOutQueueSize;
            response->set_net_queue_size(netQueueSize);

            return netQueueSize > limit;
        };

        auto netThrottling = checkNetThrottling(Config_->NetOutThrottlingLimit);
        if (netThrottling) {
            IncrementReadThrottlingCounter(context);
        }

        SuggestAllyReplicas(context);
        WaitP2PBarriers(request);

        auto chunkReaderStatistics = New<TChunkReaderStatistics>();

        std::vector<TBlock> blocks;
        bool readFromP2P = false;
        if (fetchFromCache || fetchFromDisk) {
            TChunkReadOptions options;
            options.WorkloadDescriptor = workloadDescriptor;
            options.PopulateCache = populateCache;
            options.BlockCache = Bootstrap_->GetBlockCache();
            options.FetchFromCache = fetchFromCache && !netThrottling;
            options.FetchFromDisk = fetchFromDisk && !netThrottling && !diskThrottling;
            options.ChunkReaderStatistics = chunkReaderStatistics;
            options.ReadSessionId = readSessionId;
            if (context->GetTimeout() && context->GetStartTime()) {
                options.Deadline = *context->GetStartTime() + *context->GetTimeout() * Config_->BlockReadTimeoutFraction;
            }

            if (!chunk && options.FetchFromCache) {
                blocks = ReadBlocksFromP2P(chunkId, blockIndexes, chunkReaderStatistics);
                readFromP2P = true;
            } else {
                const auto& chunkBlockManager = Bootstrap_->GetChunkBlockManager();
                auto asyncBlocks = chunkBlockManager->ReadBlockSet(
                    chunkId,
                    blockIndexes,
                    options);

                blocks = WaitFor(asyncBlocks)
                    .ValueOrThrow();
            }
        }

        // NB: p2p manager might steal blocks and assign null values.
        const auto& p2pSnooper = Bootstrap_->GetP2PSnooper();
        bool throttledLargeBlock = false;
        std::vector<TP2PSuggestion> blockPeers = p2pSnooper->OnBlockRead(
            chunkId,
            blockIndexes,
            &blocks,
            &throttledLargeBlock,
            readFromP2P);
        AddBlockPeers(response->mutable_peer_descriptors(), blockPeers);

        if (!blocks.empty()) {
            if (!checkNetThrottling(Config_->GetNetOutThrottlingHardLimit())) {
                SetRpcAttachedBlocks(response, blocks);
            } else if (!netThrottling) {
                netThrottling = true;
                IncrementReadThrottlingCounter(context);
            }
        }

        response->set_net_throttling(netThrottling);

        i64 bytesReadFromDisk =
            chunkReaderStatistics->DataBytesReadFromDisk + chunkReaderStatistics->MetaBytesReadFromDisk;
        const auto& ioTracker = Bootstrap_->GetIOTracker();
        if (bytesReadFromDisk > 0 && ioTracker->IsEnabled()) {
            ioTracker->Enqueue(
                TIOCounters{.Bytes = bytesReadFromDisk, .IORequests = 1},
                MakeReadIOTags("GetBlockSet", chunk->GetLocation(), context, chunkId));
        }

        ToProto(response->mutable_chunk_reader_statistics(), chunkReaderStatistics);

        int blocksWithData = 0;
        for (const auto& block : response->Attachments()) {
            if (block) {
                ++blocksWithData;
            }
        }

        i64 blocksSize = GetByteSize(response->Attachments());
        if (blocksSize == 0 && throttledLargeBlock) {
            response->set_net_throttling(true);
        }

        context->SetResponseInfo(
            "HasCompleteChunk: %v, "
            "NetThrottling: %v, NetOutQueueSize: %v, NetThrottlerQueueSize: %v, "
            "DiskThrottling: %v, DiskReadQueueSize: %v, DiskThrottlerQueueSize: %v, "
            "ThrottledLargeBlock: %v, "
            "BlocksWithData: %v, BlocksWithPeers: %v, BlocksSize: %v",
            hasCompleteChunk,
            netThrottling,
            netOutQueueSize,
            netThrottlerQueueSize,
            diskThrottling,
            diskReadQueueSize,
            diskThrottlerQueueSize,
            throttledLargeBlock,
            blocksWithData,
            response->peer_descriptors_size(),
            blocksSize);

        // NB: We throttle only heavy responses that contain a non-empty attachment
        // as we want responses containing the information about disk/net throttling
        // to be delivered immediately.
        if (blocksSize > 0) {
            context->SetComplete();
            context->ReplyFrom(netThrottler->Throttle(blocksSize));
        } else {
            context->Reply();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockRange)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);
        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();
        bool populateCache = request->populate_cache();

        context->SetRequestInfo(
            "BlockIds: %v:%v-%v, PopulateCache: %v, Workload: %v",
            chunkId,
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            populateCache,
            workloadDescriptor);

        ValidateOnline();

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);
        bool hasCompleteChunk = chunk.operator bool();
        response->set_has_complete_chunk(hasCompleteChunk);

        i64 diskReadQueueSize = GetDiskReadQueueSize(chunk, workloadDescriptor);
        auto locationThrottler = GetLocationOutThrottler(chunk, workloadDescriptor);
        i64 diskThrottlerQueueSize = locationThrottler
            ? locationThrottler->GetQueueTotalCount()
            : 0LL;
        i64 diskQueueSize = diskReadQueueSize + diskThrottlerQueueSize;
        response->set_disk_queue_size(diskQueueSize);

        bool diskThrottling = diskQueueSize > Config_->DiskReadThrottlingLimit;
        response->set_disk_throttling(diskThrottling);
        if (diskThrottling && chunk) {
            const auto& location = chunk->GetLocation();
            location->GetPerformanceCounters().ThrottleRead();
        }

        const auto& netThrottler = Bootstrap_->GetOutThrottler(workloadDescriptor);

        i64 netThrottlerQueueSize, netOutQueueSize;
        auto checkNetThrottling = [&] (i64 limit) {
            netThrottlerQueueSize = netThrottler->GetQueueTotalCount();
            netOutQueueSize = context->GetBusStatistics().PendingOutBytes;
            i64 netQueueSize = netThrottlerQueueSize + netOutQueueSize;
            response->set_net_queue_size(netQueueSize);

            bool netThrottling = netQueueSize > limit;
            response->set_net_throttling(netThrottling);

            return netThrottling;
        };

        bool netThrottling = checkNetThrottling(Config_->NetOutThrottlingLimit);
        if (netThrottling) {
            IncrementReadThrottlingCounter(context);
        }

        auto chunkReaderStatistics = New<TChunkReaderStatistics>();

        TChunkReadOptions options;
        options.WorkloadDescriptor = workloadDescriptor;
        options.PopulateCache = populateCache;
        options.BlockCache = Bootstrap_->GetBlockCache();
        options.FetchFromCache = !netThrottling;
        options.FetchFromDisk = !netThrottling && !diskThrottling;
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
        if (!checkNetThrottling(Config_->GetNetOutThrottlingHardLimit())) {
            SetRpcAttachedBlocks(response, blocks);
        } else if (!netThrottling) {
            netThrottling = true;
            IncrementReadThrottlingCounter(context);
        }

        i64 bytesReadFromDisk =
            chunkReaderStatistics->DataBytesReadFromDisk + chunkReaderStatistics->MetaBytesReadFromDisk;
        const auto& ioTracker = Bootstrap_->GetIOTracker();
        if (bytesReadFromDisk > 0 && ioTracker->IsEnabled()) {
            ioTracker->Enqueue(
                TIOCounters{.Bytes = bytesReadFromDisk, .IORequests = 1},
                MakeReadIOTags("GetBlockRange", chunk->GetLocation(), context, chunkId));
        }

        ToProto(response->mutable_chunk_reader_statistics(), chunkReaderStatistics);

        int blocksWithData = static_cast<int>(response->Attachments().size());
        i64 blocksSize = GetByteSize(response->Attachments());

        context->SetResponseInfo(
            "HasCompleteChunk: %v, "
            "NetThrottling: %v, NetOutQueueSize: %v, NetThrottlerQueueSize: %v, "
            "DiskThrottling: %v, DiskReadQueueSize: %v, DiskThrottlerQueueSize: %v, "
            "BlocksWithData: %v, BlocksSize: %v",
            hasCompleteChunk,
            netThrottling,
            netOutQueueSize,
            netThrottlerQueueSize,
            diskThrottling,
            diskReadQueueSize,
            diskThrottlerQueueSize,
            blocksWithData,
            blocksSize);

        // NB: We throttle only heavy responses that contain a non-empty attachment
        // as we want responses containing the information about disk/net throttling
        // to be delivered immediately.
        if (blocksSize > 0) {
            context->SetComplete();
            context->ReplyFrom(netThrottler->Throttle(blocksSize));
        } else {
            context->Reply();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkFragmentSet)
    {
        auto readSessionId = FromProto<TReadSessionId>(request->read_session_id());
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);
        auto useDirectIO = request->use_direct_io();

        int totalFragmentCount = 0;
        i64 totalFragmentSize = 0;
        for (const auto& subrequest : request->subrequests()) {
            for (const auto& fragment : subrequest.fragments()) {
                totalFragmentCount += 1;
                totalFragmentSize += fragment.length();
            }
        }

        context->SetRequestInfo("ReadSessionId: %v, Workload: %v, SubrequestCount: %v, FragmentsSize: %v/%v",
            readSessionId,
            workloadDescriptor,
            request->subrequests_size(),
            totalFragmentSize,
            totalFragmentCount);

        ValidateOnline();

        const auto& netThrottler = Bootstrap_->GetOutThrottler(workloadDescriptor);
        i64 netThrottlerQueueSize = netThrottler->GetQueueTotalCount();
        i64 netOutQueueSize = context->GetBusStatistics().PendingOutBytes;
        i64 netQueueSize = netThrottlerQueueSize + netOutQueueSize;
        bool netThrottling = netQueueSize > Config_->NetOutThrottlingLimit;
        if (netThrottling) {
            IncrementReadThrottlingCounter(context);
        }

        THashMap<TChunkLocation*, int> locationToLocationIndex;
        std::vector<std::pair<TChunkLocation*, int>> requestedLocations;

        struct TChunkRequestInfo
        {
            TChunkReadGuard Guard;
            int LocationIndex;
        };
        std::vector<TChunkRequestInfo> chunkRequestInfos;
        chunkRequestInfos.reserve(request->subrequests_size());

        std::vector<TFuture<void>> prepareReaderFutures;

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        {
            for (const auto& subrequest : request->subrequests()) {
                auto chunkId = FromProto<TChunkId>(subrequest.chunk_id());
                auto chunk = chunkRegistry->FindChunk(chunkId);

                auto* subresponse = response->add_subresponses();

                const auto& allyReplicaManager = Bootstrap_->GetAllyReplicaManager();
                if (auto allyReplicas = allyReplicaManager->GetAllyReplicas(chunkId)) {
                    ToProto(subresponse->mutable_ally_replicas(), allyReplicas);
                }

                bool chunkAvailable = false;
                if (chunk) {
                    if (auto guard = TChunkReadGuard::TryAcquire(std::move(chunk))) {
                        auto* location = guard.GetChunk()->GetLocation().Get();
                        auto [it, emplaced] = locationToLocationIndex.try_emplace(
                            location,
                            std::ssize(locationToLocationIndex));
                        if (emplaced) {
                            requestedLocations.emplace_back(location, 0);
                        }
                        requestedLocations[it->second].second += subrequest.fragments_size();

                        TClientChunkReadOptions options{
                            .WorkloadDescriptor = workloadDescriptor,
                            .ReadSessionId = readSessionId
                        };
                        if (auto future = guard.GetChunk()->PrepareToReadChunkFragments(options, useDirectIO)) {
                            YT_LOG_DEBUG("Will wait for chunk reader to become prepared (ChunkId: %v)",
                                guard.GetChunk()->GetId());
                            prepareReaderFutures.push_back(std::move(future));
                        }
                        chunkRequestInfos.push_back({
                            .Guard = std::move(guard),
                            .LocationIndex = it->second
                        });
                        chunkAvailable = true;
                    }
                }

                if (!chunkAvailable) {
                    chunkRequestInfos.emplace_back();
                }

                subresponse->set_has_complete_chunk(chunkAvailable);
            }
        }

        auto afterReadersPrepared =
            [
                =,
                this_ = MakeStrong(this),
                &netThrottler,
                requestedLocations = std::move(requestedLocations),
                chunkRequestInfos = std::move(chunkRequestInfos)
            ] (const TError& error) mutable {
                if (!error.IsOK()) {
                    context->Reply(error);
                    return;
                }

                std::vector<std::vector<int>> locationFragmentIndices(requestedLocations.size());
                std::vector<std::vector<IIOEngine::TReadRequest>> locationRequests(requestedLocations.size());
                for (auto [_, locationRequestCount] : requestedLocations) {
                    locationFragmentIndices.reserve(locationRequestCount);
                    locationRequests.reserve(locationRequestCount);
                }

                int fragmentIndex = 0;
                {
                    for (int subrequestIndex = 0; subrequestIndex < request->subrequests_size(); ++subrequestIndex) {
                        const auto& subrequest = request->subrequests(subrequestIndex);
                        const auto& chunkRequestInfo = chunkRequestInfos[subrequestIndex];
                        const auto& chunk = chunkRequestInfo.Guard.GetChunk();
                        if (!chunk) {
                            fragmentIndex += subrequest.fragments().size();
                            continue;
                        }

                        auto locationIndex = chunkRequestInfo.LocationIndex;
                        for (const auto& fragment : subrequest.fragments()) {
                            auto readRequest = chunk->MakeChunkFragmentReadRequest(
                                TChunkFragmentDescriptor{
                                    .Length = fragment.length(),
                                    .BlockIndex = fragment.block_index(),
                                    .BlockOffset = fragment.block_offset()
                                });
                            locationRequests[locationIndex].push_back(std::move(readRequest));
                            locationFragmentIndices[locationIndex].push_back(fragmentIndex);
                            ++fragmentIndex;
                        }
                    }
                }

                response->Attachments().resize(fragmentIndex);

                std::vector<TFuture<IIOEngine::TReadResponse>> readFutures;
                readFutures.reserve(requestedLocations.size());

                for (int index = 0; index < std::ssize(requestedLocations); ++index) {
                    auto [location, locationRequestCount] = requestedLocations[index];
                    YT_VERIFY(locationRequestCount == std::ssize(locationRequests[index]));
                    YT_LOG_DEBUG("Reading block fragments (LocationId: %v, FragmentCount: %v)",
                        location->GetId(),
                        locationRequestCount);
                    const auto& ioEngine = location->GetIOEngine();

                    struct TChunkFragmentBuffer
                    { };
                    readFutures.push_back(
                        ioEngine->Read<TChunkFragmentBuffer>(std::move(locationRequests[index])));
                }

                AllSucceeded(std::move(readFutures))
                    .SubscribeUnique(BIND(
                        [
                            =,
                            this_ = MakeStrong(this),
                            &netThrottler,
                            chunkRequestInfos = std::move(chunkRequestInfos),
                            locationFragmentIndices = std::move(locationFragmentIndices),
                            requestedLocations = std::move(requestedLocations)
                        ] (TErrorOr<std::vector<IIOEngine::TReadResponse>>&& resultsOrError) {
                            if (!resultsOrError.IsOK()) {
                                context->Reply(resultsOrError);
                                return;
                            }

                            auto& results = resultsOrError.Value();
                            YT_VERIFY(results.size() == locationFragmentIndices.size());

                            i64 dataBytesReadFromDisk = 0;
                            i64 dataIORequests = 0;
                            for (int resultIndex = 0; resultIndex < std::ssize(results); ++resultIndex) {
                                auto& result = results[resultIndex];
                                const auto& fragmentIndices = locationFragmentIndices[resultIndex];
                                YT_VERIFY(result.OutputBuffers.size() == fragmentIndices.size());
                                for (int index = 0; index < std::ssize(fragmentIndices); ++index) {
                                    response->Attachments()[fragmentIndices[index]] = std::move(result.OutputBuffers[index]);
                                }

                                const auto& ioTracker = Bootstrap_->GetIOTracker();
                                if (result.PaddedBytes > 0 && ioTracker->IsEnabled()) {
                                    ioTracker->Enqueue(
                                        TIOCounters{
                                            .Bytes = result.PaddedBytes,
                                            .IORequests = result.IORequests
                                        },
                                        // NB: Now we do not track chunk id for this method.
                                        MakeReadIOTags(
                                            "GetChunkFragmentSet",
                                            requestedLocations[resultIndex].first,
                                            context,
                                            NullChunkId,
                                            readSessionId));
                                }

                                dataBytesReadFromDisk += result.PaddedBytes;
                                dataIORequests += result.IORequests;
                            }

                            response->mutable_chunk_reader_statistics()->set_data_bytes_read_from_disk(dataBytesReadFromDisk);
                            response->mutable_chunk_reader_statistics()->set_data_io_requests(dataIORequests);

                            if (netThrottler->IsOverdraft()) {
                                context->SetComplete();
                                context->ReplyFrom(netThrottler->Throttle(totalFragmentSize));
                            } else {
                                netThrottler->Acquire(totalFragmentSize);
                                context->Reply();
                            }
                        }));
            };

        if (prepareReaderFutures.empty()) {
            afterReadersPrepared(TError());
        } else {
            AllSucceeded(std::move(prepareReaderFutures))
                .Subscribe(BIND(std::move(afterReadersPrepared))
                    .Via(Bootstrap_->GetStorageLookupInvoker()));
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, LookupRows)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto readSessionId = FromProto<TReadSessionId>(request->read_session_id());
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);
        auto populateCache = request->populate_cache();
        auto rejectIfThrottling = request->reject_if_throttling();

        context->SetRequestInfo("ChunkId: %v, ReadSessionId: %v, Workload: %v, PopulateCache: %v, RejectIfThrottling: %v",
            chunkId,
            readSessionId,
            workloadDescriptor,
            populateCache,
            rejectIfThrottling);

        ValidateOnline();

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->GetChunkOrThrow(chunkId);

        // NB: Heating table schema caches up is of higher priority than advisory throttling.
        const auto& schemaData = request->schema_data();
        auto [tableSchema, schemaRequested] = TLookupSession::FindTableSchema(
            chunkId,
            readSessionId,
            schemaData,
            Bootstrap_->GetTableSchemaCache());
        if (!tableSchema) {
            response->set_fetched_rows(false);
            response->set_request_schema(schemaRequested);
            context->SetResponseInfo(
                "ChunkId: %v, ReadSessionId: %v, Workload: %v, SchemaRequested: %v",
                chunkId,
                readSessionId,
                workloadDescriptor,
                schemaRequested);
            context->Reply();
            return;
        }
        YT_VERIFY(!schemaRequested);

        i64 diskReadQueueSize = GetDiskReadQueueSize(chunk, workloadDescriptor);
        auto locationThrottler = GetLocationOutThrottler(chunk, workloadDescriptor);
        i64 diskThrottlerQueueSize = locationThrottler
            ? locationThrottler->GetQueueTotalCount()
            : 0LL;
        i64 diskQueueSize = diskReadQueueSize + diskThrottlerQueueSize;
        bool diskThrottling = diskQueueSize > Config_->DiskReadThrottlingLimit;
        response->set_disk_throttling(diskThrottling);
        response->set_disk_queue_size(diskQueueSize);
        if (diskThrottling) {
            const auto& location = chunk->GetLocation();
            location->GetPerformanceCounters().ThrottleRead();
        }

        i64 netThrottlerQueueSize = Bootstrap_->GetOutThrottler(workloadDescriptor)->GetQueueTotalCount();
        i64 netOutQueueSize = context->GetBusStatistics().PendingOutBytes;
        i64 netQueueSize = netThrottlerQueueSize + netOutQueueSize;
        bool netThrottling = netQueueSize > Config_->NetOutThrottlingLimit;
        response->set_net_throttling(netThrottling);
        response->set_net_queue_size(netQueueSize);
        if (netThrottling) {
            IncrementReadThrottlingCounter(context);
        }

        if (rejectIfThrottling && (diskThrottling || netThrottling)) {
            YT_LOG_DEBUG("Rows lookup failed to start due to net throttling "
                "(ChunkId: %v, ReadSessionId: %v, DiskThrottling: %v, DiskQueueSize: %v, NetThrottling: %v, NetQueueSize: %v)",
                chunkId,
                readSessionId,
                diskThrottling,
                diskQueueSize,
                netThrottling,
                netQueueSize);

            response->set_fetched_rows(false);
            response->set_rejected_due_to_throttling(true);

            context->SetResponseInfo("ChunkId: %v, ReadSessionId: %v, Workload: %v",
                chunkId,
                readSessionId,
                workloadDescriptor);
            context->Reply();
            return;
        }

        auto timestamp = request->timestamp();
        auto columnFilter = FromProto<NTableClient::TColumnFilter>(request->column_filter());
        auto codecId = CheckedEnumCast<NCompression::ECodec>(request->compression_codec());
        auto produceAllVersions = FromProto<bool>(request->produce_all_versions());
        auto overrideTimestamp = request->has_override_timestamp() ? request->override_timestamp() : NullTimestamp;

        auto lookupSession = New<TLookupSession>(
            Bootstrap_,
            std::move(chunk),
            readSessionId,
            workloadDescriptor,
            std::move(columnFilter),
            timestamp,
            produceAllVersions,
            std::move(tableSchema),
            request->Attachments(),
            codecId,
            overrideTimestamp,
            populateCache);

        context->ReplyFrom(lookupSession->Run()
            .Apply(BIND([=, this_ = MakeStrong(this)] (const TSharedRef& result) {
                context->SetResponseInfo(
                    "ChunkId: %v, ReadSessionId: %v, Workload: %v, "
                    "DiskThrottling: %v, DiskQueueSize: %v, NetThrottling: %v, NetQueueSize: %v",
                    chunkId,
                    readSessionId,
                    workloadDescriptor,
                    diskThrottling,
                    diskQueueSize,
                    netThrottling,
                    netQueueSize);

                if (rejectIfThrottling) {
                    i64 netThrottlerQueueSize = Bootstrap_->GetOutThrottler(workloadDescriptor)->GetQueueTotalCount();
                    i64 netOutQueueSize = context->GetBusStatistics().PendingOutBytes;
                    i64 netQueueSize = netThrottlerQueueSize + netOutQueueSize;
                    bool netThrottling = netQueueSize > Config_->NetOutThrottlingLimit;
                    if (netThrottling) {
                        // NB: Flow of lookups may be dense enough, so upon start of throttling
                        // we will come up with few throttled lookups on the data node. We would like to avoid this.
                        // This may be even more painful when peer probing is disabled.
                        YT_LOG_DEBUG("Rows lookup failed to finish due to net throttling "
                            "(ChunkId: %v, ReadSessionId: %v, NetThrottling: %v, NetQueueSize: %v)",
                            chunkId,
                            readSessionId,
                            netThrottling,
                            netQueueSize);

                        IncrementReadThrottlingCounter(context);

                        response->set_fetched_rows(false);
                        response->set_rejected_due_to_throttling(true);
                        response->set_net_throttling(netThrottling);
                        response->set_net_queue_size(netQueueSize);

                        return VoidFuture;
                    }
                }

                response->Attachments().push_back(result);
                response->set_fetched_rows(true);
                ToProto(response->mutable_chunk_reader_statistics(), lookupSession->GetChunkReaderStatistics());

                context->SetComplete();

                const auto& netThrottler = Bootstrap_->GetOutThrottler(workloadDescriptor);
                return netThrottler->Throttle(GetByteSize(response->Attachments()));
            })));
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
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo("ChunkId: %v, ExtensionTags: %v, PartitionTag: %v, Workload: %v, EnableThrottling: %v",
            chunkId,
            extensionTags,
            partitionTag,
            workloadDescriptor,
            request->enable_throttling());

        ValidateOnline();

        if (request->enable_throttling() && context->GetBusStatistics().PendingOutBytes > Config_->NetOutThrottlingLimit) {
            IncrementReadThrottlingCounter(context);

            response->set_net_throttling(true);
            context->Reply();
            return;
        }

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->GetChunkOrThrow(chunkId, AllMediaIndex);

        TChunkReadOptions options;
        options.WorkloadDescriptor = workloadDescriptor;
        options.ChunkReaderStatistics = New<TChunkReaderStatistics>();

        auto asyncChunkMeta = chunk->ReadMeta(
            options,
            extensionTags);

        context->ReplyFrom(asyncChunkMeta.Apply(BIND([=] (const TRefCountedChunkMetaPtr& meta) {
            if (context->IsCanceled()) {
                throw TFiberCanceledException();
            }

            {
                ui64 chunkFeatures = meta->features();
                ui64 supportedChunkFeatures = request->supported_chunk_features();
                ValidateChunkFeatures(chunkId, chunkFeatures, supportedChunkFeatures);
            }

            if (partitionTag) {
                const auto& blockMetaCache = Bootstrap_->GetChunkMetaManager()->GetBlockMetaCache();
                auto cachedBlockMeta = blockMetaCache->Find(chunkId);
                if (!cachedBlockMeta) {
                    auto blockMetaExt = GetProtoExtension<TDataBlockMetaExt>(meta->extensions());
                    cachedBlockMeta = New<TCachedBlockMeta>(chunkId, std::move(blockMetaExt));
                    blockMetaCache->TryInsert(cachedBlockMeta);
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
        TChunkReadOptions options;
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
        return AllSet(chunkMetaFutures);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkSliceDataWeights)
    {
        auto requestCount = request->chunk_requests_size();
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo("RequestCount: %v, Workload: %v",
            requestCount,
            workloadDescriptor);

        ValidateOnline();

        GetChunkMetasForRequests(workloadDescriptor, request->chunk_requests())
            .Subscribe(BIND([=, this_ = MakeStrong(this)](const TErrorOr<std::vector<TErrorOr<NChunkClient::TRefCountedChunkMetaPtr>>>& resultsError) {
                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                if (context->IsCanceled()) {
                    return;
                }

                const auto& results = resultsError.Value();
                YT_VERIFY(std::ssize(results) == requestCount);

                auto keySetWriter = New<TKeySetWriter>();

                for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
                    ProcessSliceSize(
                        request->chunk_requests(requestIndex),
                        response->add_chunk_responses(),
                        results[requestIndex]);
                }

                context->Reply();
        }).Via(Bootstrap_->GetStorageHeavyInvoker()));
    }

    void ProcessSliceSize(
        const TReqGetChunkSliceDataWeights::TChunkSlice& weightedChunkRequest,
        TRspGetChunkSliceDataWeights::TWeightedChunk* weightedChunkResponse,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        auto chunkId = FromProto<TChunkId>(weightedChunkRequest.chunk_id());
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

            auto dataWeight = NChunkClient::GetChunkSliceDataWeights(
                weightedChunkRequest,
                *chunkMeta);

            weightedChunkResponse->set_data_weight(dataWeight);
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_WARNING(error, "Error building chunk slices (ChunkId: %v)",
                chunkId);
            ToProto(weightedChunkResponse->mutable_error(), error);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkSlices)
    {
        auto requestCount = request->slice_requests_size();
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo(
            "RequestCount: %v, Workload: %v",
            requestCount,
            workloadDescriptor);

        ValidateOnline();

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
                YT_VERIFY(std::ssize(results) == requestCount);

                auto keysWriter = New<TKeySetWriter>();
                auto keyBoundsWriter = New<TKeySetWriter>();

                for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
                    ProcessSlice(
                        request->slice_requests(requestIndex),
                        response->add_slice_responses(),
                        keysWriter,
                        keyBoundsWriter,
                        results[requestIndex]);
                }

                response->Attachments().push_back(keysWriter->Finish());
                response->Attachments().push_back(keyBoundsWriter->Finish());
                context->Reply();
            }).Via(Bootstrap_->GetStorageHeavyInvoker()));
    }

    void ProcessSlice(
        const TSliceRequest& sliceRequest,
        TRspGetChunkSlices::TSliceResponse* sliceResponse,
        const TKeySetWriterPtr& keysWriter,
        const TKeySetWriterPtr& keyBoundsWriter,
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

            auto slices = SliceChunk(
                sliceRequest,
                *chunkMeta);

            for (const auto& slice : slices) {
                ToProto(keysWriter, keyBoundsWriter, sliceResponse->add_chunk_slices(), slice);
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
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo("SamplingPolicy: %v, KeyColumns: %v, RequestCount: %v, Workload: %v",
            samplingPolicy,
            keyColumns,
            requestCount,
            workloadDescriptor);

        ValidateOnline();

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
                YT_VERIFY(std::ssize(results) == requestCount);

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
        ssize_t size = 0;
        bool incomplete = false;
        for (auto& value : values) {
            ssize_t valueSize = GetByteSize(value);
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
            ? FromProto<TLegacyOwningKey>(sampleRequest.lower_key())
            : MinKey();

        auto upperKey = sampleRequest.has_upper_key()
            ? FromProto<TLegacyOwningKey>(sampleRequest.upper_key())
            : MaxKey();

        auto blockMeta = GetProtoExtension<TDataBlockMetaExt>(chunkMeta.extensions());

        std::vector<TLegacyOwningKey> samples;
        for (const auto& block : blockMeta.data_blocks()) {
            YT_VERIFY(block.has_last_key());
            auto key = FromProto<TLegacyOwningKey>(block.last_key());
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
        for (int i = 0; i < std::ssize(keyIds); ++i) {
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
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo(
            "SubrequestCount: %v, Workload: %v",
            request->subrequests_size(),
            workloadDescriptor);

        ValidateOnline();

        const auto& chunkStore = Bootstrap_->GetChunkStore();

        std::vector<TFuture<TRefCountedChunkMetaPtr>> asyncResults;

        auto nameTable = FromProto<TNameTablePtr>(request->name_table());

        std::vector<int> subresponseIndices;
        std::vector<TGuid> chunkIds;
        chunkIds.reserve(request->subrequests_size());
        std::vector<std::vector<TStableName>> columnStableNames;
        columnStableNames.reserve(request->subrequests_size());
        for (int index = 0; index < request->subrequests_size(); ++index) {
            response->add_subresponses();
        }

        const auto& heavyInvoker = Bootstrap_->GetStorageHeavyInvoker();
        for (int index = 0; index < request->subrequests_size(); ++index) {
            const auto& subrequest = request->subrequests(index);
            auto chunkId = FromProto<TChunkId>(subrequest.chunk_id());

            auto chunk = chunkStore->FindChunk(chunkId);
            if (!chunk) {
                auto error = TError(
                    NChunkClient::EErrorCode::NoSuchChunk,
                    "No such chunk %v",
                    chunkId);
                YT_LOG_WARNING(error);
                ToProto(response->mutable_subresponses(index)->mutable_error(), error);
                continue;
            }

            subresponseIndices.push_back(index);
            chunkIds.push_back(chunkId);

            auto columnIds = FromProto<std::vector<int>>(subrequest.column_ids());
            columnStableNames.emplace_back();
            for (auto id : columnIds) {
                columnStableNames.back().push_back(TStableName(TString(nameTable->GetNameOrThrow(id))));
            }

            TChunkReadOptions options;
            options.WorkloadDescriptor = workloadDescriptor;
            options.ChunkReaderStatistics = New<TChunkReaderStatistics>();

            auto asyncChunkMeta = chunk->ReadMeta(options);
            // Fault-injection for tests.
            if (Config_->TestingOptions->ColumnarStatisticsChunkMetaFetchMaxDelay) {
                asyncChunkMeta = asyncChunkMeta.Apply(BIND([=, Logger = Logger, config = Config_] (const TRefCountedChunkMetaPtr& result) {
                    auto maxDelay = *config->TestingOptions->ColumnarStatisticsChunkMetaFetchMaxDelay;
                    auto delay = index * maxDelay / request->subrequests_size();
                    YT_LOG_DEBUG("Injected a random delay after a chunk meta fetch (Delay: %v)", delay);
                    TDelayedExecutor::WaitForDuration(delay);
                    return result;
                }).AsyncVia(heavyInvoker));
            }
            asyncResults.push_back(asyncChunkMeta);
        }

        std::optional<TDuration> earlyFinishTimeout;
        if (request->enable_early_finish() && context->GetTimeout()) {
            earlyFinishTimeout = *context->GetTimeout() * Config_->ColumnarStatisticsReadTimeoutFraction;
            YT_LOG_DEBUG("Early finish is enabled (EarlyFinishTimeout: %v)", earlyFinishTimeout);
        }

        auto combinedResult = (earlyFinishTimeout ? AllSetWithTimeout(asyncResults, *earlyFinishTimeout) : AllSet(asyncResults));
        context->SubscribeCanceled(BIND([combinedResult = combinedResult] {
            combinedResult.Cancel(TError("RPC request canceled"));
        }));
        context->ReplyFrom(combinedResult.Apply(
            BIND(
                &TDataNodeService::ProcessColumnarStatistics,
                MakeWeak(this),
                subresponseIndices,
                columnStableNames,
                chunkIds,
                context)
                .AsyncVia(heavyInvoker)));
    }

    void ProcessColumnarStatistics(
        const std::vector<int>& subresponseIndices,
        const std::vector<std::vector<TStableName>>& columnStableNames,
        const std::vector<TChunkId>& chunkIds,
        const TCtxGetColumnarStatisticsPtr& context,
        const std::vector<TErrorOr<TRefCountedChunkMetaPtr>>& metaOrErrors)
    {
        YT_VERIFY(subresponseIndices.size() == columnStableNames.size());
        YT_VERIFY(subresponseIndices.size() == chunkIds.size());
        YT_VERIFY(subresponseIndices.size() == metaOrErrors.size());
        for (int index = 0; index < std::ssize(subresponseIndices); ++index) {
            auto chunkId = chunkIds[index];
            const auto& subresponse = context->Response().mutable_subresponses(subresponseIndices[index]);
            const auto& metaOrError = metaOrErrors[index];
            try {
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    metaOrError,
                    "Error getting meta of chunk %v",
                    chunkId);
                const auto& meta = *metaOrError.Value();

                auto type = EChunkType(meta.type());
                if (type != EChunkType::Table) {
                    THROW_ERROR_EXCEPTION(
                        "Invalid type of chunk %v: expected %Qlv, actual %Qlv",
                        chunkId,
                        EChunkType::Table,
                        type);
                }

                auto optionalColumnarStatisticsExt = FindProtoExtension<TColumnarStatisticsExt>(meta.extensions());
                if (!optionalColumnarStatisticsExt) {
                    ToProto(
                        subresponse->mutable_error(),
                        TError(
                            NChunkClient::EErrorCode::MissingExtension,
                            "Columnar statistics chunk meta extension missing"));
                    continue;
                }
                const auto& columnarStatisticsExt = *optionalColumnarStatisticsExt;

                auto nameTableExt = FindProtoExtension<TNameTableExt>(meta.extensions());
                TNameTablePtr nameTable;
                if (nameTableExt) {
                    nameTable = FromProto<TNameTablePtr>(*nameTableExt);
                } else {
                    auto schemaExt = GetProtoExtension<TTableSchemaExt>(meta.extensions());
                    nameTable = TNameTable::FromSchemaStable(FromProto<TTableSchema>(schemaExt));
                }

                for (const auto& columnName: columnStableNames[index]) {
                    auto id = nameTable->FindId(columnName.Get());
                    if (id && *id < columnarStatisticsExt.data_weights().size()) {
                        YT_LOG_DEBUG("name %v, id %v, weight %v", columnName.Get(), id, columnarStatisticsExt.data_weights(*id));
                        subresponse->add_data_weights(columnarStatisticsExt.data_weights(*id));
                    } else {
                        YT_LOG_DEBUG("name %v, id %v, not found", columnName.Get(), id);
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
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, UpdatePeer)
    {
        // NB: this method is no longer used, but noop stub is kept here for smooth rolling update.
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, AnnounceChunkReplicas)
    {
        context->SetRequestInfo("SubrequestCount: %v, SourceNodeId: %v",
            request->announcements_size(),
            request->source_node_id());

        const auto& allyReplicaManager = Bootstrap_->GetAllyReplicaManager();
        allyReplicaManager->OnAnnouncementsReceived(
            MakeRange(request->announcements()),
            request->source_node_id());

        context->Reply();
    }


    bool IsCloseDemanded(const TStoreLocationPtr& location)
    {
        const auto& sessionManager = Bootstrap_->GetSessionManager();
        return location->IsSick() || sessionManager->GetDisableWriteSessions();
    }

    void ValidateOnline()
    {
        if (!Bootstrap_->GetMasterConnector()->IsOnline()) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::MasterNotConnected,
                "Master is not connected");
        }
    }

    static i64 GetDiskReadQueueSize(const IChunkPtr& chunk, const TWorkloadDescriptor& workloadDescriptor)
    {
        if (!chunk) {
            return 0;
        }
        return chunk->GetLocation()->GetPendingIOSize(EIODirection::Read, workloadDescriptor);
    }

    static IThroughputThrottlerPtr GetLocationOutThrottler(const IChunkPtr& chunk, const TWorkloadDescriptor& workloadDescriptor)
    {
        if (!chunk) {
            return nullptr;
        }
        return chunk->GetLocation()->GetOutThrottler(workloadDescriptor);
    }

    template <class TContextPtr>
    void IncrementReadThrottlingCounter(const TContextPtr& context)
    {
        Bootstrap_->GetNetworkStatistics().IncrementReadThrottlingCounter(
            context->GetEndpointAttributes().Get("network", DefaultNetworkName));
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateDataNodeService(
    TDataNodeConfigPtr config,
    IBootstrap* bootstrap)
{
    return New<TDataNodeService>(
        config,
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
