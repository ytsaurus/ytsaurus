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
#include "session.h"
#include "master_connector.h"

#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/string.h>
#include <core/misc/lazy_ptr.h>
#include <core/misc/random.h>
#include <core/misc/nullable.h>

#include <core/bus/tcp_dispatcher.h>

#include <core/rpc/service_detail.h>

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/action_queue.h>

#include <core/profiling/profile_manager.h>

#include <ytlib/table_client/name_table.h>
#include <ytlib/table_client/private.h>
#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/unversioned_row.h>

#include <ytlib/chunk_client/data_node_service_proxy.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/data_node_service.pb.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/chunk_slice.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <server/cell_node/bootstrap.h>

#include <cmath>

namespace NYT {
namespace NDataNode {

using namespace NRpc;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NCellNode;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = DataNodeProfiler;

static const auto ProfilingPeriod = TDuration::MilliSeconds(100);
static const size_t MaxSampleSize = 4 * 1024;

////////////////////////////////////////////////////////////////////////////////

class TDataNodeService
    : public TServiceBase
{
public:
    TDataNodeService(
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TServiceBase(
            CreatePrioritizedInvoker(bootstrap->GetControlInvoker()),
            TDataNodeServiceProxy::GetServiceName(),
            DataNodeLogger)
        , Config_(config)
        , WorkerThread_(New<TActionQueue>("DataNodeWorker"))
        , Bootstrap_(bootstrap)
    {
        YCHECK(Config_);
        YCHECK(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CancelChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlocks)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockSet)
            .SetCancelable(true)
            .SetEnableReorder(true)
            .SetMaxQueueSize(5000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockRange)
            .SetCancelable(true)
            .SetEnableReorder(true)
            .SetMaxQueueSize(5000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta)
            .SetCancelable(true)
            .SetEnableReorder(true)
            .SetMaxQueueSize(5000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdatePeer)
            .SetOneWay(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableSamples)
            .SetCancelable(true)
            .SetResponseCodec(NCompression::ECodec::Lz4)
            .SetResponseHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkSplits)
            .SetCancelable(true)
            .SetResponseCodec(NCompression::ECodec::Lz4)
            .SetResponseHeavy(true));

        for (auto type : TEnumTraits<EWriteSessionType>::GetDomainValues()) {
            SessionTypeToTag_[type] = NProfiling::TProfileManager::Get()->RegisterTag("type", type);
        }

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TDataNodeService::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();
    }

private:
    const TDataNodeConfigPtr Config_;
    const TActionQueuePtr WorkerThread_;
    TBootstrap* const Bootstrap_;

    TPeriodicExecutorPtr ProfilingExecutor_;

    TEnumIndexedVector<NProfiling::TTagId, EWriteSessionType> SessionTypeToTag_;


    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, StartChunk)
    {
        UNUSED(response);

        auto chunkId = FromProto<TChunkId>(request->chunk_id());

        TSessionOptions options;
        options.SessionType = EWriteSessionType(request->session_type());
        options.SyncOnClose = request->sync_on_close();
        options.OptimizeForLatency = request->sync_on_close();

        context->SetRequestInfo("ChunkId: %v, SessionType: %v, SyncOnClose: %v, OptimizeForLatency: %v",
            chunkId,
            options.SessionType,
            options.SyncOnClose,
            options.OptimizeForLatency);

        ValidateConnected();
        ValidateNoSession(chunkId);
        ValidateNoChunk(chunkId);

        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->StartSession(chunkId, options);
        auto result = session->Start();
        context->ReplyFrom(result);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FinishChunk)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto& meta = request->chunk_meta();
        auto blockCount = request->has_block_count() ? MakeNullable(request->block_count()) : Null;

        context->SetRequestInfo("ChunkId: %v, BlockCount: %v",
            chunkId,
            blockCount);

        ValidateConnected();

        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSession(chunkId);

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
        auto chunkId = FromProto<TChunkId>(request->chunk_id());

        context->SetRequestInfo("ChunkId: %v",
            chunkId);

        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSession(chunkId);
        session->Cancel(TError("Canceled by client request"));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PingSession)
    {
        UNUSED(response);

        auto chunkId = FromProto<TChunkId>(request->chunk_id());

        context->SetRequestInfo("ChunkId: %v", chunkId);

        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSession(chunkId);
        session->Ping();

        context->Reply();
    }


    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PutBlocks)
    {
        UNUSED(response);

        if (IsInThrottling()) {
            context->Reply(TError(
                NRpc::EErrorCode::Unavailable,
                "Write throttling is active"));
            return;
        }

        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        int firstBlockIndex = request->first_block_index();
        int blockCount = static_cast<int>(request->Attachments().size());
        int lastBlockIndex = firstBlockIndex + blockCount - 1;
        bool populateCache = request->populate_cache();
        bool flushBlocks = request->flush_blocks();

        context->SetRequestInfo("BlockIds: %v:%v-%v, PopulateCache: %v, FlushBlocks: %v",
            chunkId,
            firstBlockIndex,
            lastBlockIndex,
            populateCache,
            flushBlocks);

        ValidateConnected();

        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSession(chunkId);

        // Put blocks.
        auto result = session->PutBlocks(
            firstBlockIndex,
            request->Attachments(),
            populateCache);
        
        // Flush blocks if needed.
        if (flushBlocks) {
            result = result.Apply(BIND([=] () {
                return session->FlushBlocks(lastBlockIndex);
            }));
        }

        context->ReplyFrom(result);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, SendBlocks)
    {
        UNUSED(response);

        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();
        int lastBlockIndex = firstBlockIndex + blockCount - 1;
        auto targetDescriptor = FromProto<TNodeDescriptor>(request->target_descriptor());

        context->SetRequestInfo("BlockIds: %v:%v-%v, Target: %v",
            chunkId,
            firstBlockIndex,
            lastBlockIndex,
            targetDescriptor);

        ValidateConnected();

        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSession(chunkId);
        session->SendBlocks(firstBlockIndex, blockCount, targetDescriptor)
            .Subscribe(BIND([=] (const TError& error) {
                if (error.IsOK()) {
                    context->Reply();
                } else {
                    context->Reply(TError(
                        NChunkClient::EErrorCode::PipelineFailed,
                        "Error putting blocks to %v",
                        targetDescriptor.GetDefaultAddress())
                        << error);
                }
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FlushBlocks)
    {
        UNUSED(response);

        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        int blockIndex = request->block_index();

        context->SetRequestInfo("BlockId: %v:%v",
            chunkId,
            blockIndex);

        ValidateConnected();

        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSession(chunkId);
        auto result = session->FlushBlocks(blockIndex);
        context->ReplyFrom(result);
    }


    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = NYT::FromProto<int>(request->block_indexes());
        bool populateCache = request->populate_cache();
        auto sessionType = EReadSessionType(request->session_type());

        context->SetRequestInfo("BlockIds: %v:[%v], PopulateCache: %v, SessionType: %v",
            chunkId,
            JoinToString(blockIndexes),
            populateCache,
            sessionType);

        ValidateConnected();

        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto blockStore = Bootstrap_->GetBlockStore();
        auto peerBlockTable = Bootstrap_->GetPeerBlockTable();

        response->set_has_complete_chunk(chunkRegistry->FindChunk(chunkId).operator bool());
        response->set_throttling(IsOutThrottling());

        if (IsOutThrottling()) {
            // Cannot send the actual data to the client due to throttling.
            // Let's try to suggest some other peers.
            for (int blockIndex : request->block_indexes()) {
                auto blockId = TBlockId(chunkId, blockIndex);
                const auto& peers = peerBlockTable->GetPeers(blockId);
                if (!peers.empty()) {
                    auto* peerDescriptor = response->add_peer_descriptors();
                    peerDescriptor->set_block_index(blockIndex);
                    for (const auto& peer : peers) {
                        ToProto(peerDescriptor->add_node_descriptors(), peer.Descriptor);
                    }
                    LOG_DEBUG("Peers suggested (BlockId: %v, PeerCount: %v)",
                        blockId,
                        peers.size());
                }
            }
        } else {
            auto blockCache = Bootstrap_->GetBlockCache();
            auto asyncBlocks = blockStore->ReadBlockSet(
                chunkId,
                blockIndexes,
                context->GetPriority(),
                blockCache,
                populateCache);
            response->Attachments() = WaitFor(asyncBlocks)
                .ValueOrThrow();
        }


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
            auto expirationTime = TInstant(request->peer_expiration_time());
            TPeerInfo peerInfo(descriptor, expirationTime);
            for (int blockIndex : request->block_indexes()) {
                peerBlockTable->UpdatePeer(TBlockId(chunkId, blockIndex), peerInfo);
            }
        }

        context->SetResponseInfo("HasCompleteChunk: %v, Throttling: %v, BlocksWithData: %v, BlocksWithPeers: %v, BlocksSize: %v",
            response->has_complete_chunk(),
            response->throttling(),
            blocksWithData,
            response->peer_descriptors_size(),
            blocksSize);

        // Throttle response.
        auto throttler = Bootstrap_->GetOutThrottler(sessionType);
        context->ReplyFrom(throttler->Throttle(blocksSize));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockRange)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto sessionType = EReadSessionType(request->session_type());
        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();
        bool populateCache = request->populate_cache();

        context->SetRequestInfo("BlockIds: %v:%v-%v, PopulateCache: %v, SessionType: %v",
            chunkId,
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            populateCache,
            sessionType);

        ValidateConnected();

        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
        response->set_has_complete_chunk(chunkRegistry->FindChunk(chunkId).operator bool());

        response->set_throttling(IsOutThrottling());

        if (!IsOutThrottling()) {
            auto blockStore = Bootstrap_->GetBlockStore();
            auto blockCache = Bootstrap_->GetBlockCache();
            auto asyncBlocks = blockStore->ReadBlockRange(
                chunkId,
                firstBlockIndex,
                blockCount,
                context->GetPriority(),
                blockCache,
                populateCache);
            response->Attachments() = WaitFor(asyncBlocks)
                .ValueOrThrow();
        }

        int blocksWithData = response->Attachments().size();
        i64 blocksSize = GetByteSize(response->Attachments());

        context->SetResponseInfo("HasCompleteChunk: %v, Throttling: %v, BlocksWithData: %v, BlocksSize: %v",
            response->has_complete_chunk(),
            response->throttling(),
            blocksWithData,
            blocksSize);

        // Throttle response.
        auto throttler = Bootstrap_->GetOutThrottler(sessionType);
        context->ReplyFrom(throttler->Throttle(blocksSize));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto partitionTag = request->has_partition_tag()
            ? MakeNullable(request->partition_tag())
            : Null;
        auto extensionTags = request->all_extension_tags()
            ? Null
            : MakeNullable(FromProto<int>(request->extension_tags()));

        context->SetRequestInfo("ChunkId: %v, ExtensionTags: %v, PartitionTag: %v",
            chunkId,
            extensionTags ? "[" + JoinToString(*extensionTags) + "]" : "<Null>",
            partitionTag);

        ValidateConnected();

        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->GetChunkOrThrow(chunkId);

        auto asyncChunkMeta = chunk->ReadMeta(context->GetPriority(), extensionTags);
        asyncChunkMeta.Subscribe(BIND([=] (const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError) {
            if (!metaOrError.IsOK()) {
                context->Reply(metaOrError);
                return;
            }

            const auto& meta = *metaOrError.Value();
            *context->Response().mutable_chunk_meta() = partitionTag
                ? FilterChunkMetaByPartitionTag(meta, *partitionTag)
                : TChunkMeta(meta);

            context->Reply();
        }).Via(WorkerThread_->GetInvoker()));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkSplits)
    {
        context->SetRequestInfo("KeyColumnCount: %v, ChunkCount: %v, MinSplitSize: %v",
            request->key_columns_size(),
            request->chunk_specs_size(),
            request->min_split_size());

        ValidateConnected();

        std::vector<TFuture<void>> asyncResults;
        auto keyColumns = NYT::FromProto<Stroka>(request->key_columns());
        for (const auto& chunkSpec : request->chunk_specs()) {
            auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
            auto* splits = response->add_splits();
            auto chunk = Bootstrap_->GetChunkStore()->FindChunk(chunkId);

            if (!chunk) {
                auto error = TError(
                    NChunkClient::EErrorCode::NoSuchChunk,
                    "No such chunk %v",
                    chunkId);
                LOG_WARNING(error);
                ToProto(splits->mutable_error(), error);
                continue;
            }

            auto asyncResult = chunk->ReadMeta(context->GetPriority());
            asyncResults.push_back(asyncResult.Apply(
                BIND(
                    &TDataNodeService::MakeChunkSplits,
                    MakeStrong(this),
                    &chunkSpec,
                    splits,
                    request->min_split_size(),
                    keyColumns)
                .AsyncVia(WorkerThread_->GetInvoker())));
        }

        context->ReplyFrom(Combine(asyncResults));
    }

    void MakeChunkSplits(
        const NChunkClient::NProto::TChunkSpec* chunkSpec,
        NChunkClient::NProto::TRspGetChunkSplits::TChunkSplits* splits,
        i64 minSplitSize,
        const TKeyColumns& keyColumns,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        auto chunkId = FromProto<TChunkId>(chunkSpec->chunk_id());
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

            auto keyColumnsExt = GetProtoExtension<TKeyColumnsExt>(meta.extensions());
            auto chunkKeyColumns = FromProto<TKeyColumns>(keyColumnsExt);
            ValidateKeyColumns(keyColumns, chunkKeyColumns);

            auto newChunkSpec = *chunkSpec;
            *newChunkSpec.mutable_chunk_meta() = meta;
            auto slices = SliceChunkByKeys(
                New<TRefCountedChunkSpec>(std::move(newChunkSpec)), 
                minSplitSize, 
                keyColumns.size());

            for (const auto& slice : slices) {
                ToProto(splits->add_chunk_specs(), *slice);
            }
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            LOG_WARNING(error);
            ToProto(splits->mutable_error(), error);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetTableSamples)
    {
        auto keyColumns = FromProto<Stroka>(request->key_columns());

        context->SetRequestInfo("KeyColumns: [%v], ChunkCount: %v",
            JoinToString(keyColumns),
            request->sample_requests_size());

        ValidateConnected();

        auto chunkStore = Bootstrap_->GetChunkStore();

        std::vector<TFuture<void>> asyncResults;
        for (const auto& sampleRequest : request->sample_requests()) {
            auto* sampleResponse = response->add_sample_responses();
            auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());

            auto chunk = chunkStore->FindChunk(chunkId);
            if (!chunk) {
                auto error = TError(
                    NChunkClient::EErrorCode::NoSuchChunk,
                    "No such chunk %v",
                    chunkId);
                LOG_WARNING(error);
                ToProto(sampleResponse->mutable_error(), error);
                continue;
            }

            auto asyncChunkMeta = chunk->ReadMeta(context->GetPriority());
            asyncResults.push_back(asyncChunkMeta.Apply(
                BIND(
                    &TDataNodeService::ProcessSample,
                    MakeStrong(this),
                    &sampleRequest,
                    sampleResponse,
                    keyColumns)
                .AsyncVia(WorkerThread_->GetInvoker())));
        }

        context->ReplyFrom(Combine(asyncResults));
    }

    void ProcessSample(
        const TReqGetTableSamples::TSampleRequest* sampleRequest,
        TRspGetTableSamples::TChunkSamples* sampleResponse,
        const TKeyColumns& keyColumns,
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

            auto formatVersion = ETableChunkFormat(meta.version());
            switch (formatVersion) {
                case ETableChunkFormat::Old:
                    ProcessOldChunkSamples(sampleRequest, sampleResponse, keyColumns, meta);
                    break;

                case ETableChunkFormat::VersionedSimple:
                    ProcessVersionedChunkSamples(sampleRequest, sampleResponse, keyColumns, meta);
                    break;

                case ETableChunkFormat::SchemalessHorizontal:
                    ProcessUnversionedChunkSamples(sampleRequest, sampleResponse, keyColumns, meta);
                    break;

                default:
                    THROW_ERROR_EXCEPTION("Invalid version %v of chunk %v",
                            meta.version(),
                            chunkId);
            }
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            LOG_WARNING(error);
            ToProto(sampleResponse->mutable_error(), error);
        }
    }

    void ProcessOldChunkSamples(
        const TReqGetTableSamples::TSampleRequest* sampleRequest,
        TRspGetTableSamples::TChunkSamples* chunkSamples,
        const TKeyColumns& keyColumns,
        const TChunkMeta& chunkMeta)
    {
        auto samplesExt = GetProtoExtension<TOldSamplesExt>(chunkMeta.extensions());
        std::vector<TSample> samples;
        RandomSampleN(
            samplesExt.items().begin(),
            samplesExt.items().end(),
            std::back_inserter(samples),
            sampleRequest->sample_count());

        for (const auto& sample : samples) {
            TUnversionedRowBuilder rowBuilder;
            auto* key = chunkSamples->add_keys();
            size_t size = 0;
            for (const auto& column : keyColumns) {
                if (size >= MaxSampleSize)
                    break;

                auto it = std::lower_bound(
                    sample.parts().begin(),
                    sample.parts().end(),
                    column,
                    [] (const TSamplePart& part, const Stroka& column) {
                        return part.column() < column;
                    });

                auto keyPart = MakeUnversionedSentinelValue(EValueType::Null);
                size += sizeof(keyPart); // part type
                if (it != sample.parts().end() && it->column() == column) {
                    switch (ELegacyKeyPartType(it->key_part().type())) {
                        case ELegacyKeyPartType::Composite:
                            keyPart = MakeUnversionedAnyValue(TStringBuf());
                            break;
                        case ELegacyKeyPartType::Int64:
                            keyPart = MakeUnversionedInt64Value(it->key_part().int64_value());
                            break;
                        case ELegacyKeyPartType::Double:
                            keyPart = MakeUnversionedDoubleValue(it->key_part().double_value());
                            break;
                        case ELegacyKeyPartType::String: {
                            auto partSize = std::min(it->key_part().str_value().size(), MaxSampleSize - size);
                            auto value = TStringBuf(it->key_part().str_value().begin(), partSize);
                            keyPart = MakeUnversionedStringValue(value);
                            size += partSize;
                            break;
                        }
                        default:
                            YUNREACHABLE();
                    }
                }
                rowBuilder.AddValue(keyPart);
            }
            ToProto(key, rowBuilder.GetRow());
        }
    }

    void ProcessVersionedChunkSamples(
        const TReqGetTableSamples::TSampleRequest* sampleRequest,
        TRspGetTableSamples::TChunkSamples* chunkSamples,
        const TKeyColumns& keyColumns,
        const TChunkMeta& chunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(sampleRequest->chunk_id());

        auto keyColumnsExt = GetProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
        auto chunkKeyColumns = NYT::FromProto<TKeyColumns>(keyColumnsExt);

        int prefixLength = std::min(chunkKeyColumns.size(), keyColumns.size());
        bool isCompatibleKeyColumns = std::equal(
            chunkKeyColumns.begin(), 
            chunkKeyColumns.begin() + prefixLength, 
            keyColumns.begin());

        // Requested key can be wider than stored.
        if (!isCompatibleKeyColumns || chunkKeyColumns.size() > prefixLength) {
            auto error = TError("Incompatible key columns in chunk %v: requested key columns [%v], chunk key columns [%v]",
                chunkId,
                JoinToString(keyColumns),
                JoinToString(chunkKeyColumns));
            LOG_WARNING(error);
            ToProto(chunkSamples->mutable_error(), error);
            return;
        }

        auto samplesExt = GetProtoExtension<TSamplesExt>(chunkMeta.extensions());
        auto samples = FromProto<TOwningKey>(samplesExt.entries());

        auto lowerKey = sampleRequest->has_lower_key() 
            ? FromProto<TOwningKey>(sampleRequest->lower_key()) 
            : MinKey();

        auto upperKey = sampleRequest->has_upper_key() 
            ? FromProto<TOwningKey>(sampleRequest->upper_key()) 
            : MaxKey();

        auto it = std::remove_if(
            samples.begin(),
            samples.end(),
            [&] (const TOwningKey& key) {
                return  key < lowerKey || key >= upperKey;
            });

        std::random_shuffle(samples.begin(), it);
        auto count = std::min(
            static_cast<int>(std::distance(samples.begin(), it)), 
            sampleRequest->sample_count());
        samples.erase(samples.begin() + count, samples.end());

        int keyPadding = keyColumns.size() - prefixLength;
        if (keyPadding > 0) {
            // Requested key is wider than the keys stored in chunk.
            std::vector<TUnversionedValue> values(keyColumns.size(), MakeUnversionedSentinelValue(EValueType::Null, 0));
            for (int i = 0; i < samples.size(); ++i) {
                YCHECK(samples[i].GetCount() == chunkKeyColumns.size());
                samples[i] = WidenKey(samples[i], keyPadding);
            }
        }

        ToProto(chunkSamples->mutable_keys(), samples);
    }

    void ProcessUnversionedChunkSamples(
        const TReqGetTableSamples::TSampleRequest* sampleRequest,
        TRspGetTableSamples::TChunkSamples* chunkSamples,
        const TKeyColumns& keyColumns,
        const TChunkMeta& chunkMeta)
    {
        auto nameTableExt = GetProtoExtension<TNameTableExt>(chunkMeta.extensions());
        auto nameTable = FromProto<TNameTablePtr>(nameTableExt);

        std::vector<int> keyIds;
        for (const auto& column : keyColumns) {
            keyIds.push_back(nameTable->GetIdOrRegisterName(column));
        }

        std::vector<int> idToKeyIndex(nameTable->GetSize(), -1);
        for (int i = 0; i < keyIds.size(); ++i) {
            idToKeyIndex[keyIds[i]] = i;
        }

        auto samplesExt = GetProtoExtension<TSamplesExt>(chunkMeta.extensions());
        std::vector<TProtoStringType> samples;
        samples.reserve(sampleRequest->sample_count());

        RandomSampleN(
            samplesExt.entries().begin(),
            samplesExt.entries().end(),
            std::back_inserter(samples),
            sampleRequest->sample_count());

        for (const auto& protoSample : samples) {
            std::vector<TUnversionedValue> keyValues(keyColumns.size(), MakeUnversionedSentinelValue(EValueType::Null));
            TUnversionedOwningRow row = FromProto<TUnversionedOwningRow>(protoSample);

            for (int i = 0; i < row.GetCount(); ++i) {
                auto& value = row[i];
                int keyIndex = idToKeyIndex[value.Id];
                if (keyIndex < 0) {
                    continue;
                }

                keyValues[keyIndex] = value;
            }

            size_t size = 0;
            for (const auto& value : keyValues) {
                size += GetByteSize(value);
            }

            while (size > MaxSampleSize && keyValues.size() > 1) {
                size -= GetByteSize(keyValues.back());
                keyValues.pop_back();
            }

            if (size > MaxSampleSize) {
                YCHECK(keyValues.size() == 1);
                YCHECK(keyValues.front().Type == EValueType::String);
                keyValues.front().Length = MaxSampleSize;
            }

            auto* key = chunkSamples->add_keys();
            ToProto(key, keyValues.data(), keyValues.data() + keyValues.size());
        }
    }

    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NChunkClient::NProto, UpdatePeer)
    {
        auto descriptor = FromProto<TNodeDescriptor>(request->peer_descriptor());
        auto expirationTime = TInstant(request->peer_expiration_time());
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

    void ValidateNoSession(const TChunkId& chunkId)
    {
        if (Bootstrap_->GetSessionManager()->FindSession(chunkId)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::SessionAlreadyExists,
                "Session %v already exists",
                chunkId);
        }
    }

    void ValidateNoChunk(const TChunkId& chunkId)
    {
        if (Bootstrap_->GetChunkStore()->FindChunk(chunkId)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::ChunkAlreadyExists,
                "Chunk %v already exists",
                chunkId);
        }
    }

    i64 GetPendingOutSize() const
    {
        return
            NBus::TTcpDispatcher::Get()->GetStatistics(NBus::ETcpInterfaceType::Remote).PendingOutBytes +
            Bootstrap_->GetBlockStore()->GetPendingReadSize();
    }

    i64 GetPendingInSize() const
    {
        return Bootstrap_->GetSessionManager()->GetPendingWriteSize();
    }

    bool IsOutThrottling() const
    {
        i64 pendingSize = GetPendingOutSize();
        if (pendingSize > Config_->BusOutThrottlingLimit) {
            LOG_DEBUG("Outcoming throttling is active: %v > %v",
                pendingSize,
                Config_->BusOutThrottlingLimit);
            return true;
        } else {
            return false;
        }
    }

    bool IsInThrottling() const
    {
        i64 pendingSize = GetPendingInSize();
        if (pendingSize > Config_->BusInThrottlingLimit) {
            LOG_DEBUG("Incoming throttling is active: %v > %v",
                pendingSize,
                Config_->BusInThrottlingLimit);
            return true;
        } else {
            return false;
        }
    }


    void OnProfiling()
    {
        Profiler.Enqueue("/pending_out_size", GetPendingOutSize());
        Profiler.Enqueue("/pending_in_size", GetPendingInSize());

        auto sessionManager = Bootstrap_->GetSessionManager();
        for (auto type : TEnumTraits<EWriteSessionType>::GetDomainValues()) {
            Profiler.Enqueue(
                "/session_count",
                sessionManager->GetSessionCount(type),
                {SessionTypeToTag_[type]});
        }
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

} // namespace NDataNode
} // namespace NYT
