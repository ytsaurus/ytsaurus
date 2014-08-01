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

#include <ytlib/table_client/public.h>

#include <server/cell_node/public.h>
#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/string.h>
#include <core/misc/lazy_ptr.h>
#include <core/misc/random.h>
#include <core/misc/nullable.h>

#include <core/bus/tcp_dispatcher.h>

#include <core/rpc/service_detail.h>

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/action_queue.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/private.h>

#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/chunk_client/data_node_service_proxy.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/data_node_service.pb.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>
#include <ytlib/chunk_client/read_limit.h>

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
using namespace NVersionedTableClient;
using namespace NVersionedTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Profiler = DataNodeProfiler;
static auto ProfilingPeriod = TDuration::MilliSeconds(100);

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

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CancelChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlocks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockSet)
            .SetEnableReorder(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockRange)
            .SetEnableReorder(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta)
            .SetEnableReorder(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PrecacheChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdatePeer)
            .SetOneWay(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableSamples)
            .SetResponseCodec(NCompression::ECodec::Lz4)
            .SetResponseHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkSplits)
            .SetResponseCodec(NCompression::ECodec::Lz4)
            .SetResponseHeavy(true));

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TDataNodeService::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();
    }

private:
    TDataNodeConfigPtr Config_;
    TActionQueuePtr WorkerThread_;
    TBootstrap* Bootstrap_;

    TPeriodicExecutorPtr ProfilingExecutor_;


    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, StartChunk)
    {
        UNUSED(response);

        auto chunkId = FromProto<TChunkId>(request->chunk_id());

        TSessionOptions options;
        options.SessionType = EWriteSessionType(request->session_type());
        options.SyncOnClose = request->sync_on_close();
        options.OptimizeForLatency = request->sync_on_close();

        context->SetRequestInfo("ChunkId: %s, SessionType: %s, SyncOnClose: %s, OptimizeForLatency: %s",
            ~ToString(chunkId),
            ~ToString(options.SessionType),
            ~FormatBool(options.SyncOnClose),
            ~FormatBool(options.OptimizeForLatency));

        ValidateNoSession(chunkId);
        ValidateNoChunk(chunkId);

        auto sessionManager = Bootstrap_->GetSessionManager();
        sessionManager->StartSession(chunkId, options);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FinishChunk)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto& meta = request->chunk_meta();
        auto blockCount = request->has_block_count() ? MakeNullable<int>(request->block_count()) : Null;

        context->SetRequestInfo("ChunkId: %s, BlockCount: %s",
            ~ToString(chunkId),
            blockCount ? ~ToString(*blockCount) : "<null>");

        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSession(chunkId);

        session->Finish(meta, blockCount)
            .Subscribe(BIND([=] (TErrorOr<IChunkPtr> chunkOrError) {
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

        context->SetRequestInfo("ChunkId: %s",
            ~ToString(chunkId));

        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSession(chunkId);
        session->Cancel(TError("Canceled by client request"));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PingSession)
    {
        UNUSED(response);

        auto chunkId = FromProto<TChunkId>(request->chunk_id());

        context->SetRequestInfo("ChunkId: %s", ~ToString(chunkId));

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
        bool enableCaching = request->enable_caching();
        bool flushBlocks = request->flush_blocks();

        context->SetRequestInfo("BlockIds: %s:%d-%d, EnableCaching: %s, FlushBlocks: %s",
            ~ToString(chunkId),
            firstBlockIndex,
            lastBlockIndex,
            ~FormatBool(enableCaching),
            ~FormatBool(flushBlocks));

        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSession(chunkId);

        // Put blocks.
        auto result = session->PutBlocks(
            firstBlockIndex,
            request->Attachments(),
            enableCaching);
        
        // Flush blocks if needed.
        if (flushBlocks) {
            result = result.Apply(BIND([=] (TError error) -> TAsyncError {
                if (!error.IsOK()) {
                    return MakeFuture(error);
                }
                return session->FlushBlocks(lastBlockIndex);
            }));
        }

        result.Subscribe(BIND([=] (TError error) {
            context->Reply(error);
        }));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, SendBlocks)
    {
        UNUSED(response);

        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();
        int lastBlockIndex = firstBlockIndex + blockCount - 1;
        auto target = FromProto<TNodeDescriptor>(request->target());

        context->SetRequestInfo("BlockIds: %s:%d-%d, TargetAddress: %s",
            ~ToString(chunkId),
            firstBlockIndex,
            lastBlockIndex,
            ~target.GetDefaultAddress());

        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSession(chunkId);
        session->SendBlocks(firstBlockIndex, blockCount, target)
            .Subscribe(BIND([=] (TError error) {
                if (error.IsOK()) {
                    context->Reply();
                } else {
                    context->Reply(TError(
                        NChunkClient::EErrorCode::PipelineFailed,
                        "Error putting blocks to %v",
                        target.GetDefaultAddress())
                        << error);
                }
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FlushBlocks)
    {
        UNUSED(response);

        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        int blockIndex = request->block_index();

        context->SetRequestInfo("BlockId: %s:%d",
            ~ToString(chunkId),
            blockIndex);

        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSession(chunkId);

        session->FlushBlocks(blockIndex)
            .Subscribe(BIND([=] (TError error) {
                context->Reply(error);
            }));
    }


    class TGetBlockSetSession
        : public TIntrinsicRefCounted
    {
    public:
        typedef NRpc::TTypedServiceContext<TReqGetBlockSet, TRspGetBlockSet> TCtxGetBlockSet;
        typedef TIntrusivePtr<TCtxGetBlockSet> TCtxGetBlockSetPtr;

        TGetBlockSetSession(
            TIntrusivePtr<TDataNodeService> owner,
            TCtxGetBlockSetPtr context)
            : Owner_(std::move(owner))
            , Context_(std::move(context))
            , Logger(DataNodeLogger)
            , Awaiter_(New<TParallelAwaiter>(Owner_->Bootstrap_->GetControlInvoker()))
            , BlocksWithData_(0)
            , BlocksSize_(0)
        { }

        void Run()
        {
            const auto& request = Context_->Request();
            auto& response = Context_->Response();

            auto chunkId = FromProto<TChunkId>(request.chunk_id());
            bool enableCaching = request.enable_caching();
            auto sessionType = EReadSessionType(request.session_type());

            Context_->SetRequestInfo("BlockIds: %s:%s, EnableCaching: %s, SessionType: %s",
                ~ToString(chunkId),
                ~JoinToString(request.block_indexes()),
                ~FormatBool(enableCaching),
                ~ToString(sessionType));

            auto chunkStore = Owner_->Bootstrap_->GetChunkStore();
            auto blockStore = Owner_->Bootstrap_->GetBlockStore();
            auto peerBlockTable = Owner_->Bootstrap_->GetPeerBlockTable();

            response.set_has_complete_chunk(chunkStore->FindChunk(chunkId) != nullptr);
            response.set_throttling(Owner_->IsOutThrottling());

            if (response.throttling()) {
                // Cannot send the actual data to the client due to throttling.
                // Let's try to suggest some other peers.
                for (int blockIndex : request.block_indexes()) {
                    TBlockId blockId(chunkId, blockIndex);
                    const auto& peers = peerBlockTable->GetPeers(blockId);
                    if (!peers.empty()) {
                        auto* peerDescriptor = response.add_peer_descriptors();
                        peerDescriptor->set_block_index(blockIndex);
                        for (const auto& peer : peers) {
                            ToProto(peerDescriptor->add_node_descriptors(), peer.Descriptor);
                        }
                        LOG_DEBUG("Peers suggested (BlockId: %s, PeerCount: %d)",
                            ~ToString(blockId),
                            static_cast<int>(peers.size()));
                    }
                }
            } else {
                response.Attachments().resize(request.block_indexes().size());

                // Assign decreasing priorities to block requests to take advantage of sequential read.
                i64 priority = Context_->GetPriority();

                for (int index = 0; index < request.block_indexes().size(); ++index) {
                    int blockIndex = request.block_indexes(index);

                    LOG_DEBUG("Fetching block (BlockId: %s:%d)",
                        ~ToString(chunkId),
                        blockIndex);

                    Awaiter_->Await(
                        blockStore->GetBlock(
                            chunkId,
                            blockIndex,
                            priority,
                            enableCaching),
                        BIND(
                            &TGetBlockSetSession::OnGotBlock,
                            MakeStrong(this),
                            index));

                    --priority;
                }
            }

            Awaiter_->Complete(
                BIND(&TGetBlockSetSession::OnComplete, MakeStrong(this)));
        }

    private:
        TIntrusivePtr<TDataNodeService> Owner_;
        TCtxGetBlockSetPtr Context_;
        NLog::TLogger Logger;

        TParallelAwaiterPtr Awaiter_;

        std::atomic<int> BlocksWithData_;
        std::atomic<i64> BlocksSize_;


        void OnGotBlock(int index, TBlockStore::TGetBlockResult result)
        {
            if (!result.IsOK()) {
                // Something went wrong while fetching the blocks.
                Awaiter_->Cancel();
                Context_->Reply(result);
                return;
            }

            const auto& block = result.Value();
            if (block) {
                auto& response = Context_->Response();
                response.Attachments()[index] = block;
                BlocksWithData_ += 1;
                BlocksSize_ += block.Size();
            }
        }

        void OnComplete()
        {
            const auto& request = Context_->Request();
            auto& response = Context_->Response();
            auto chunkId = FromProto<TChunkId>(request.chunk_id());
            auto peerBlockTable = Owner_->Bootstrap_->GetPeerBlockTable();

            // Register the peer that we had just sent the reply to.
            if (request.has_peer_descriptor() && request.has_peer_expiration_time()) {
                auto descriptor = FromProto<TNodeDescriptor>(request.peer_descriptor());
                auto expirationTime = TInstant(request.peer_expiration_time());
                TPeerInfo peerInfo(descriptor, expirationTime);
                for (int blockIndex : request.block_indexes()) {
                    peerBlockTable->UpdatePeer(TBlockId(chunkId, blockIndex), peerInfo);
                }
            }

            Context_->SetResponseInfo("HasCompleteChunk: %s, Throttling: %s, BlocksWithData: %d, BlocksWithPeers: %d, BlocksSize: %" PRId64,
                ~FormatBool(response.has_complete_chunk()),
                ~FormatBool(response.throttling()),
                BlocksWithData_.load(),
                response.peer_descriptors_size(),
                BlocksSize_.load());

            // Throttle response.
            auto sessionType = EReadSessionType(request.session_type());
            auto throttler = Owner_->Bootstrap_->GetOutThrottler(sessionType);
            throttler->Throttle(BlocksSize_).Subscribe(BIND([=] () {
                Context_->Reply();
            }));
        }

    };

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockSet)
    {
        New<TGetBlockSetSession>(this, std::move(context))->Run();
    }

    class TGetBlockRangeSession
        : public TIntrinsicRefCounted
    {
    public:
        typedef NRpc::TTypedServiceContext<TReqGetBlockRange, TRspGetBlockRange> TCtxGetBlockRange;
        typedef TIntrusivePtr<TCtxGetBlockRange> TCtxGetBlockRangePtr;

        TGetBlockRangeSession(TIntrusivePtr<TDataNodeService> owner, TCtxGetBlockRangePtr context)
            : Owner_(std::move(owner))
            , Context_(std::move(context))
            , BlocksWithData_(0)
            , BlocksSize_(0)
        { }

        void Run()
        {
            const auto& request = Context_->Request();
            auto& response = Context_->Response();

            auto chunkId = FromProto<TChunkId>(request.chunk_id());
            auto sessionType = EReadSessionType(request.session_type());
            int firstBlockIndex = request.first_block_index();
            int blockCount = request.block_count();

            Context_->SetRequestInfo("BlockIds: %s:%d-%d, SessionType: %s",
                ~ToString(chunkId),
                firstBlockIndex,
                firstBlockIndex + blockCount - 1,
                ~ToString(sessionType));

            auto chunkStore = Owner_->Bootstrap_->GetChunkStore();
            auto blockStore = Owner_->Bootstrap_->GetBlockStore();

            response.set_has_complete_chunk(chunkStore->FindChunk(chunkId) != nullptr);
            response.set_throttling(Owner_->IsOutThrottling());

            if (response.throttling()) {
                OnComplete();
            } else {
                blockStore
                    ->GetBlocks(
                        chunkId,
                        firstBlockIndex,
                        blockCount,
                        Context_->GetPriority())
                    .Subscribe(BIND(&TGetBlockRangeSession::OnGotBlocks, MakeStrong(this)));
            }
        }

    private:
        TIntrusivePtr<TDataNodeService> Owner_;
        TCtxGetBlockRangePtr Context_;

        int BlocksWithData_;
        i64 BlocksSize_;


        void OnGotBlocks(TBlockStore::TGetBlocksResult result)
        {
            if (!result.IsOK()) {
                // Something went wrong while fetching the blocks.
                Context_->Reply(result);
                return;
            }

            auto& response = Context_->Response();
            const auto& blocks = result.Value();
            for (const auto& block : blocks) {
                response.Attachments().push_back(block);
                BlocksWithData_ += 1;
                BlocksSize_ += block.Size();
            }

            OnComplete();
        }

        void OnComplete()
        {
            const auto& request = Context_->Request();
            auto& response = Context_->Response();

            Context_->SetResponseInfo("HasCompleteChunk: %s, Throttling: %s, BlocksWithData: %d, BlocksSize: %" PRId64,
                ~FormatBool(response.has_complete_chunk()),
                ~FormatBool(response.throttling()),
                BlocksWithData_,
                BlocksSize_);

            // Throttle response.
            auto sessionType = EReadSessionType(request.session_type());
            auto throttler = Owner_->Bootstrap_->GetOutThrottler(sessionType);
            throttler->Throttle(BlocksSize_).Subscribe(BIND([=] () {
                Context_->Reply();
            }));
        }

    };

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockRange)
    {
        New<TGetBlockRangeSession>(this, std::move(context))->Run();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkMeta)
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

        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->GetChunk(chunkId);
        auto asyncChunkMeta = chunk->GetMeta(
            context->GetPriority(),
            request->all_extension_tags() ? nullptr : &extensionTags);

        asyncChunkMeta.Subscribe(BIND([=] (IChunk::TGetMetaResult result) {
            if (!result.IsOK()) {
                context->Reply(result);
                return;
            }

            const auto& chunkMeta = *result.Value();

            *context->Response().mutable_chunk_meta() = partitionTag
                ? FilterChunkMetaByPartitionTag(chunkMeta, *partitionTag)
                : TChunkMeta(chunkMeta);

            context->Reply();
        }).Via(WorkerThread_->GetInvoker()));
    }


    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkSplits)
    {
        context->SetRequestInfo("KeyColumnCount: %d, ChunkCount: %d, MinSplitSize: %" PRId64,
            request->key_columns_size(),
            request->chunk_specs_size(),
            request->min_split_size());

        auto awaiter = New<TParallelAwaiter>(WorkerThread_->GetInvoker());
        auto keyColumns = NYT::FromProto<Stroka>(request->key_columns());

        for (const auto& chunkSpec : request->chunk_specs()) {
            auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
            auto* splittedChunk = response->add_splitted_chunks();
            auto chunk = Bootstrap_->GetChunkStore()->FindChunk(chunkId);

            if (!chunk) {
                auto error = TError("No such chunk %v",
                    chunkId);
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

    void MakeChunkSplits(
        const NChunkClient::NProto::TChunkSpec* chunkSpec,
        NChunkClient::NProto::TRspGetChunkSplits::TChunkSplits* splittedChunk,
        i64 minSplitSize,
        const NTableClient::TKeyColumns& keyColumns,
        IChunk::TGetMetaResult result)
    {
        auto chunkId = FromProto<TChunkId>(chunkSpec->chunk_id());

        if (!result.IsOK()) {
            auto error = TError("Error getting meta of chunk %v",
                chunkId)
                << result;
            LOG_WARNING(error);
            ToProto(splittedChunk->mutable_error(), error);
            return;
        }

        const auto& chunkMeta = *result.Value();

        if (chunkMeta.type() != EChunkType::Table) {
            auto error =  TError("Invalid type of chunk %v: expected %Qv, actual %Qv",
                chunkId,
                EChunkType(EChunkType::Table),
                EChunkType(chunkMeta.type()));
            LOG_ERROR(error);
            ToProto(splittedChunk->mutable_error(), error);
            return;
        }

        // XXX(psushin): implement splitting for new chunks.
        if (chunkMeta.version() != 1) {
            // Only old chunks support splitting now.
            auto error = TError("Invalid version of chunk %v: expected: 1, actual %v",
                chunkId,
                chunkMeta.version());
            LOG_ERROR(error);
            ToProto(splittedChunk->mutable_error(), error);
            return;
        }

        auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta.extensions());
        if (!miscExt.sorted()) {
            auto error =  TError("Chunk %v is not sorted",
                chunkId);
            LOG_ERROR(error);
            ToProto(splittedChunk->mutable_error(), error);
            return;
        }

        auto keyColumnsExt = GetProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
        if (keyColumnsExt.names_size() < keyColumns.size()) {
            auto error = TError("Not enough key columns in chunk %v: expected %v, actual %v",
                chunkId,
                keyColumns.size(),
                keyColumnsExt.names_size());
            LOG_ERROR(error);
            ToProto(splittedChunk->mutable_error(), error);
            return;
        }

        for (int i = 0; i < keyColumns.size(); ++i) {
            const auto& value = keyColumnsExt.names(i);
            if (keyColumns[i] != value) {
                auto error = TError("Invalid key column in chunk %v: expected %Qv, actual %Qv",
                    chunkId,
                    keyColumns[i],
                    value);
                LOG_ERROR(error);
                ToProto(splittedChunk->mutable_error(), error);
                return;
            }
        }

        auto indexExt = GetProtoExtension<TIndexExt>(chunkMeta.extensions());
        if (indexExt.items_size() == 1) {
            // Only one index entry available - no need to split.
            splittedChunk->add_chunk_specs()->CopyFrom(*chunkSpec);
            return;
        }

        auto backIt = --indexExt.items().end();
        auto dataSizeBetweenSamples = static_cast<i64>(std::ceil(
            static_cast<double>(backIt->row_index()) /
            miscExt.row_count() *
            miscExt.uncompressed_data_size() /
            indexExt.items_size()));
    	YCHECK(dataSizeBetweenSamples > 0);

        using NChunkClient::TReadLimit;
        auto comparer = [&] (
            const TReadLimit& limit,
            const TIndexRow& indexRow,
            bool isStartLimit) -> int
        {
            if (!limit.HasRowIndex() && !limit.HasKey()) {
                return isStartLimit ? -1 : 1;
            }

            auto result = 0;
            if (limit.HasRowIndex()) {
                auto diff = limit.GetRowIndex() - indexRow.row_index();
                // Sign function.
                result += (diff > 0) - (diff < 0);
            }

            if (limit.HasKey()) {
                TOwningKey indexKey;
                FromProto(&indexKey, indexRow.key());
                result += CompareRows(limit.GetKey(), indexKey, keyColumns.size());
            }

            if (result == 0) {
                return isStartLimit ? -1 : 1;
            }

            return (result > 0) - (result < 0);
        };

        auto beginIt = std::lower_bound(
            indexExt.items().begin(),
            indexExt.items().end(),
            TReadLimit(chunkSpec->upper_limit()),
            [&] (const TIndexRow& indexRow, const TReadLimit& limit) {
                return comparer(limit, indexRow, true) > 0;
            });

        auto endIt = std::upper_bound(
            beginIt,
            indexExt.items().end(),
            TReadLimit(chunkSpec->lower_limit()),
            [&] (const TReadLimit& limit, const TIndexRow& indexRow) {
                return comparer(limit, indexRow, false) < 0;
            });

    	if (std::distance(beginIt, endIt) < 2) {
       	 	// Too small distance between given read limits.
	        splittedChunk->add_chunk_specs()->CopyFrom(*chunkSpec);
        	return;
    	}

        TChunkSpec* currentSplit;
        TOldBoundaryKeysExt boundaryKeysExt;
        i64 endRowIndex = beginIt->row_index();
        i64 startRowIndex;
        i64 dataSize;

        auto createNewSplit = [&] () {
            currentSplit = splittedChunk->add_chunk_specs();
            currentSplit->CopyFrom(*chunkSpec);
            boundaryKeysExt = GetProtoExtension<TOldBoundaryKeysExt>(chunkSpec->chunk_meta().extensions());
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
                SetProtoExtension(currentSplit->mutable_chunk_meta()->mutable_extensions(), boundaryKeysExt);

                endRowIndex = beginIt->row_index();

                TSizeOverrideExt sizeOverride;
                sizeOverride.set_row_count(endRowIndex - startRowIndex);
                sizeOverride.set_uncompressed_data_size(dataSize);
                SetProtoExtension(currentSplit->mutable_chunk_meta()->mutable_extensions(), sizeOverride);

                key = GetKeySuccessor(key);
                TOwningKey limitKey;
                FromProto(&limitKey, key);

                ToProto(currentSplit->mutable_lower_limit()->mutable_key(), limitKey);

                createNewSplit();
                *boundaryKeysExt.mutable_start() = key;
                ToProto(currentSplit->mutable_upper_limit()->mutable_key(), limitKey);
            }
        }

        // Sanity check.
        YCHECK(CompareKeys(boundaryKeysExt.start(), boundaryKeysExt.end()) <= 0);
        SetProtoExtension(currentSplit->mutable_chunk_meta()->mutable_extensions(), boundaryKeysExt);
        endRowIndex = (--endIt)->row_index();

        TSizeOverrideExt sizeOverride;
        sizeOverride.set_row_count(endRowIndex - startRowIndex);
        sizeOverride.set_uncompressed_data_size(
            dataSize +
            (std::distance(beginIt, endIt)) * dataSizeBetweenSamples);
        SetProtoExtension(currentSplit->mutable_chunk_meta()->mutable_extensions(), sizeOverride);
    }


    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetTableSamples)
    {
        context->SetRequestInfo("KeyColumnCount: %d, ChunkCount: %d",
            request->key_columns_size(),
            request->sample_requests_size());

        auto awaiter = New<TParallelAwaiter>(WorkerThread_->GetInvoker());
        auto keyColumns = FromProto<Stroka>(request->key_columns());

        for (const auto& sampleRequest : request->sample_requests()) {
            auto* sampleResponse = response->add_sample_responses();
            auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());
            auto chunk = Bootstrap_->GetChunkStore()->FindChunk(chunkId);

            if (!chunk) {
                auto error = TError("No such chunk %v",
                    chunkId);
                LOG_WARNING(error);
                ToProto(sampleResponse->mutable_error(), error);
                continue;
            }

            awaiter->Await(
                chunk->GetMeta(context->GetPriority()),
                BIND(
                    &TDataNodeService::ProcessSample,
                    MakeStrong(this),
                    &sampleRequest,
                    sampleResponse,
                    keyColumns));
        }

        awaiter->Complete(BIND([=] () {
            context->Reply();
        }));
    }

    void ProcessSample(
        const NChunkClient::NProto::TReqGetTableSamples::TSampleRequest* sampleRequest,
        NChunkClient::NProto::TRspGetTableSamples::TChunkSamples* sampleResponse,
        const NTableClient::TKeyColumns& keyColumns,
        IChunk::TGetMetaResult result)
    {
        auto chunkId = FromProto<TChunkId>(sampleRequest->chunk_id());

        if (!result.IsOK()) {
            auto error = TError("Error getting meta of chunk %v",
                chunkId)
                << result;
            LOG_WARNING(error);
            ToProto(sampleResponse->mutable_error(), error);
            return;
        }

        const auto& chunkMeta = *result.Value();
        if (chunkMeta.type() != EChunkType::Table) {
            auto error = TError("Invalid type of chunk %v: expected %Qv, actual %Qv",
                chunkId,
                EChunkType(EChunkType::Table),
                EChunkType(chunkMeta.type()));
            LOG_WARNING(error);
            ToProto(sampleResponse->mutable_error(), error);
            return;
        }

        switch (chunkMeta.version()) {
            case ETableChunkFormat::Old:
                ProcessOldChunkSamples(sampleRequest, sampleResponse, keyColumns, chunkMeta);
                break;

            case ETableChunkFormat::VersionedSimple:
                ProcessVersionedChunkSamples(sampleRequest, sampleResponse, keyColumns, chunkMeta);
                break;

            default:
                auto error = TError("Invalid version %v of chunk %v",
                    chunkMeta.version(),
                    chunkId);
                LOG_WARNING(error);
                ToProto(sampleResponse->mutable_error(), error);
                break;
        }
    }

    void ProcessOldChunkSamples(
        const TReqGetTableSamples::TSampleRequest* sampleRequest,
        TRspGetTableSamples::TChunkSamples* chunkSamples,
        const NTableClient::TKeyColumns& keyColumns,
        const NChunkClient::NProto::TChunkMeta& chunkMeta)
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

                TUnversionedValue keyPart = MakeUnversionedSentinelValue(EValueType::Null);
                size += sizeof(keyPart); // part type
                if (it != sample.parts().end() && it->column() == column) {
                    switch (it->key_part().type()) {
                        case EKeyPartType::Composite:
                            keyPart = MakeUnversionedAnyValue(TStringBuf());
                            break;
                        case EKeyPartType::Int64:
                            keyPart = MakeUnversionedInt64Value(it->key_part().int64_value());
                            break;
                        case EKeyPartType::Uint64:
                            keyPart = MakeUnversionedUint64Value(it->key_part().uint64_value());
                            break;
                        case EKeyPartType::Double:
                            keyPart = MakeUnversionedDoubleValue(it->key_part().double_value());
                            break;
                        case EKeyPartType::Boolean:
                            keyPart = MakeUnversionedBooleanValue(it->key_part().boolean_value());
                            break;
                        case EKeyPartType::String: {
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
        const NTableClient::TKeyColumns& keyColumns,
        const NChunkClient::NProto::TChunkMeta& chunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(sampleRequest->chunk_id());

        auto keyColumnsExt = GetProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
        auto chunkKeyColumns = NYT::FromProto<TKeyColumns>(keyColumnsExt);

        if (chunkKeyColumns != keyColumns) {
            auto error = TError("Key columns mismatch in chunk %v: expected [%v], actual [%v]",
                chunkId,
                JoinToString(keyColumns),
                JoinToString(chunkKeyColumns));
            LOG_WARNING(error);
            ToProto(chunkSamples->mutable_error(), error);
            return;
        }

        auto samplesExt = GetProtoExtension<TSamplesExt>(chunkMeta.extensions());
        std::vector<Stroka> samples;
        RandomSampleN(
            samplesExt.entries().begin(),
            samplesExt.entries().end(),
            std::back_inserter(samples),
            sampleRequest->sample_count());

        ToProto(chunkSamples->mutable_keys(), samples);
    }


    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PrecacheChunk)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());

        context->SetRequestInfo("ChunkId: %s",
            ~ToString(chunkId));

        Bootstrap_
            ->GetChunkCache()
            ->DownloadChunk(chunkId)
            .Subscribe(BIND([=] (TChunkCache::TDownloadResult result) {
                if (result.IsOK()) {
                    context->Reply();
                } else {
                    context->Reply(TError(
                        NChunkClient::EErrorCode::ChunkPrecachingFailed,
                        "Error precaching chunk %v",
                        chunkId)
                        << result);
                }
            }));
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
            NBus::TTcpDispatcher::Get()->GetStatistics(NBus::ETcpInterfaceType::Remote).PendingOutSize +
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
            LOG_DEBUG("Outcoming throttling is active: %" PRId64 " > %" PRId64,
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
            LOG_DEBUG("Incoming throttling is active: %" PRId64 " > %" PRId64,
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
        for (auto type : EWriteSessionType::GetDomainValues()) {
            Profiler.Enqueue("/session_count/" + FormatEnum(type), sessionManager->GetSessionCount(type));
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
