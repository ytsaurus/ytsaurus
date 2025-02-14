#include "distributed_chunk_session_controller.h"

#include "config.h"
#include "distributed_chunk_session_service_proxy.h"
#include "private.h"

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NDistributedChunkSessionClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NThreading;

using NApi::NNative::IClientPtr;
using NChunkClient::NProto::TChunkInfo;
using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::TMiscExt;
using NTableClient::NProto::TDataBlockMetaExt;
using NTableClient::NProto::TNameTableExt;

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionController
    : public IDistributedChunkSessionController
{
public:
    TDistributedChunkSessionController(
        IClientPtr client,
        TDistributedChunkSessionControllerConfigPtr config,
        TTransactionId transactionId,
        TNameTablePtr chunkNameTable,
        IInvokerPtr invoker)
        : Client_(std::move(client))
        , Config_(std::move(config))
        , TransactionId_(transactionId)
        , ChunkNameTable_(std::move(chunkNameTable))
        , Invoker_(std::move(invoker))
        , SerializedInvoker_(CreateSerializedInvoker(Invoker_))
        , Logger(DistributedChunkSessionLogger().WithTag("(TransactionId: %v)", TransactionId_))
    { }

    TFuture<TNodeDescriptor> StartSession() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND(
            &TDistributedChunkSessionController::DoStartSession,
            MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    bool IsActive() const final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(CloseSpinLock_);
        return !CloseInitiated_;
    }

    TFuture<void> Close() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_VERIFY(SessionStarted_);

        CloseSession();
        return CloseFuture_;
    }

    TSessionId GetSessionId() const final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return SessionId_;
    }

private:
    struct TNode
    {
        TNode(
            TNodeDescriptor descriptor,
            IChannelPtr channel,
            TChunkReplicaWithMedium chunkReplica)
            : Descriptor(std::move(descriptor))
            , Channel(std::move(channel))
            , ChunkReplica(std::move(chunkReplica))
        { }

        TNodeDescriptor Descriptor;
        IChannelPtr Channel;
        TChunkReplicaWithMedium ChunkReplica;
        TChunkLocationUuid TargetLocationUuid = InvalidChunkLocationUuid;

        TPeriodicExecutorPtr PingExecutor;
        TChunkInfo ChunkInfo;
    };

    const IClientPtr Client_;
    const TDistributedChunkSessionControllerConfigPtr Config_;
    const TTransactionId TransactionId_;

    const TNameTablePtr ChunkNameTable_;

    const IInvokerPtr Invoker_;
    const IInvokerPtr SerializedInvoker_;

    TLogger Logger;

    TPeriodicExecutorPtr CoordinatorPingExecutor_;

    TSessionId SessionId_;
    std::vector<TNode> Nodes_;

    TNode* CoordinatorNode_ = nullptr;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, CloseSpinLock_);
    bool CloseInitiated_ = false;
    TFuture<void> CloseFuture_;

    int AcknowledgedBlockCount_ = 0;
    TMiscExt ChunkMiscMeta_;
    TDataBlockMetaExt DataBlockMetas_;

    TChunkMeta ChunkMeta_;

    std::atomic_bool SessionStarted_ = false;

    TNodeDescriptor DoStartSession()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto options = New<TMultiChunkWriterOptions>();
        options->Account = Config_->Account;
        options->ReplicationFactor = Config_->ReplicationFactor;

        SessionId_ = CreateChunk(
            Client_,
            CellTagFromId(TransactionId_),
            std::move(options),
            TransactionId_,
            TChunkListId{},
            Logger());

        Logger.AddTag("ChunkId: %v", SessionId_);

        YT_LOG_INFO("Chunk created");

        auto targets = AllocateWriteTargets(
            Client_,
            TSessionId(DecodeChunkId(SessionId_.ChunkId).Id, SessionId_.MediumIndex),
            /*desiredTargetCount*/ Config_->ReplicationFactor,
            /*minTargetCount*/ Config_->ReplicationFactor,
            /*replicationFactorOverride*/ {},
            /*preferredHostName*/ {},
            /*forbiddenAddresses*/ {},
            /*allocatedAddresses*/ {},
            Logger);

        Nodes_.reserve(targets.size());

        const auto& nodeDirectory = Client_->GetNativeConnection()->GetNodeDirectory();
        const auto& channelFactory = Client_->GetChannelFactory();
        const auto& networks = Client_->GetNativeConnection()->GetNetworks();
        for (auto& target : targets) {
            auto descriptor = nodeDirectory->GetDescriptor(target);
            auto channel = channelFactory->CreateChannel(descriptor.GetAddressOrThrow(networks));
            Nodes_.emplace_back(
                std::move(descriptor),
                std::move(channel),
                std::move(target));
        }

        std::vector<TFuture<TDataNodeServiceProxy::TRspStartChunkPtr>> asyncResults;
        asyncResults.reserve(Nodes_.size());
        for (int index = 0; index < std::ssize(Nodes_); ++index) {
            TDataNodeServiceProxy proxy(Nodes_[index].Channel);
            auto req = proxy.StartChunk();
            req->SetTimeout(Config_->NodeRpcTimeout);
            ToProto(req->mutable_session_id(), SessionId_);
            req->set_sync_on_close(true);
            req->set_disable_send_blocks(true);
            SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::UserBatch));

            asyncResults.push_back(req->Invoke());
        }

        auto responses = WaitFor(AllSucceeded(std::move(asyncResults)))
            .ValueOrThrow();

        for (int index = 0; index < std::ssize(Nodes_); ++index) {
            YT_VERIFY(responses[index]->has_location_uuid());
            Nodes_[index].TargetLocationUuid = FromProto<TChunkLocationUuid>(responses[index]->location_uuid());
            Nodes_[index].PingExecutor = New<TPeriodicExecutor>(
                SerializedInvoker_,
                BIND(&TDistributedChunkSessionController::SendDataNodePing, MakeStrong(this), index),
                Config_->DataNodePingPeriod);

            Nodes_[index].PingExecutor->Start();
        }

        CoordinatorNode_ = &Nodes_[RandomNumber<ui32>(Nodes_.size())];

        YT_LOG_INFO("Selected coordinator node (Address: %v)", CoordinatorNode_->Descriptor.GetAddressOrThrow(networks));

        TDistributedChunkSessionServiceProxy proxy(CoordinatorNode_->Channel);
        auto req = proxy.StartSession();
        ToProto(req->mutable_session_id(), SessionId_);
        for (const auto& node : Nodes_) {
            ToProto(req->add_chunk_replicas(), node.Descriptor);
        }
        WaitFor(req->Invoke())
            .ThrowOnError();

        CoordinatorPingExecutor_ = New<TPeriodicExecutor>(
            SerializedInvoker_,
            BIND(&TDistributedChunkSessionController::SendCoordinatorPing, MakeStrong(this)),
            Config_->WriteSessionPingPeriod);

        CoordinatorPingExecutor_->Start();

        SessionStarted_ = true;

        return CoordinatorNode_->Descriptor;
    }

    void SendDataNodePing(int index)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto& node = Nodes_[index];

        YT_LOG_DEBUG("Sending data node ping (Address: %v)",
            node.Descriptor.GetDefaultAddress());

        TDataNodeServiceProxy proxy(node.Channel);
        auto req = proxy.PingSession();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Data node ping failed (Address: %v)",
                node.Descriptor.GetDefaultAddress());
            return;
        }

        if (rspOrError.Value()->close_demanded()) {
            CloseSession();
        }
    }

    void SendCoordinatorPing()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        YT_VERIFY(CoordinatorNode_);

        YT_LOG_DEBUG("Sending coordinator ping (Address: %v)",
            CoordinatorNode_->Descriptor.GetDefaultAddress());

        TDistributedChunkSessionServiceProxy proxy(CoordinatorNode_->Channel);
        auto req = proxy.PingSession();
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_acknowledged_block_count(AcknowledgedBlockCount_);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Coordinator ping failed (Address: %v)",
                CoordinatorNode_->Descriptor.GetDefaultAddress());
            return;
        }

        {
            auto guard = ReaderGuard(CloseSpinLock_);
            if (CloseInitiated_) {
                YT_LOG_DEBUG("Drop coordinator ping response since close is initiated");
                return;
            }
        }

        YT_VERIFY(AcknowledgedBlockCount_ == req->acknowledged_block_count());

        auto rsp = rspOrError.Value();

        YT_VERIFY(AcknowledgedBlockCount_ <= rsp->written_block_count());
        AcknowledgedBlockCount_ = rsp->written_block_count();

        YT_VERIFY(DataBlockMetas_.data_blocks_size() + rsp->data_block_metas_size() == rsp->written_block_count());

        for (const auto& blockMeta : rsp->data_block_metas()) {
            *DataBlockMetas_.add_data_blocks() = blockMeta;
        }

        ChunkMiscMeta_ = std::move(*rsp->mutable_chunk_misc_meta());

        YT_LOG_DEBUG("Updated statistics (BlockCount: %v, DataWeight: %v, RowCount: %v, CloseDemanded: %v)",
            AcknowledgedBlockCount_,
            ChunkMiscMeta_.data_weight(),
            ChunkMiscMeta_.row_count(),
            rsp->close_demanded());

        if (rsp->close_demanded()) {
            CloseSession();
        }
    }

    void CloseSession()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(CloseSpinLock_);
        if (std::exchange(CloseInitiated_, true)) {
            return;
        }

        YT_LOG_DEBUG("Initiating session close (BlockCount: %v, DataWeight: %v, RowCount: %v)",
            AcknowledgedBlockCount_,
            ChunkMiscMeta_.data_weight(),
            ChunkMiscMeta_.row_count());

        CloseFuture_ = BIND(
            &TDistributedChunkSessionController::DoCloseSession,
            MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    void DoCloseSession()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto stopCoordinatorFuture = StopCoordinator();
        auto stopCoordinatorPingFuture = CoordinatorPingExecutor_->Stop();

        FillChunkMeta();

        auto finishedNodesOrError = WaitFor(FinishChunkOnNodes());
        YT_LOG_FATAL_IF(
            !finishedNodesOrError.IsOK(),
            finishedNodesOrError,
            "Unexpected failure during finishing chunk on nodes");

        auto& finishedNodes = finishedNodesOrError.Value();
        YT_LOG_INFO("Chunk was successfully finished on %v nodes", finishedNodes.size());

        auto confirmChunkFuture = ConfirmChunk(std::move(finishedNodes));

        std::vector<TFuture<void>> stopNodesPingFutures;
        stopNodesPingFutures.reserve(Nodes_.size());
        for (auto& node : Nodes_) {
            stopNodesPingFutures.emplace_back(node.PingExecutor->Stop());
        }

        auto stopCoordinatorResult = WaitFor(stopCoordinatorFuture);
        YT_LOG_FATAL_IF(
            !stopCoordinatorResult.IsOK(),
            stopCoordinatorResult,
            "Unexpected failure during coordinator stopping");

        auto stopCoordinatorPingResult = WaitFor(stopCoordinatorPingFuture);
        YT_LOG_FATAL_IF(
            !stopCoordinatorPingResult.IsOK(),
            stopCoordinatorPingResult,
            "Unexpected failure during coordinator ping executor stopping");

        auto confirmChunkError = WaitFor(confirmChunkFuture);
        THROW_ERROR_EXCEPTION_IF_FAILED(
            confirmChunkError,
            "Failed to confirm chunk %v",
            SessionId_.ChunkId);

        auto stopNodesPingResult = WaitFor(AllSucceeded(stopNodesPingFutures));
        YT_LOG_FATAL_IF(
            !stopNodesPingResult.IsOK(),
            stopNodesPingResult,
            "Unexpected failure during nodes ping executors stopping");

        YT_LOG_DEBUG("Chunk confirmed");
    }

    void FillChunkMeta()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        ChunkMeta_.set_type(ToProto(EChunkType::Table));
        ChunkMeta_.set_format(ToProto(EChunkFormat::TableUnversionedSchemalessHorizontal));

        SetProtoExtension(ChunkMeta_.mutable_extensions(), ChunkMiscMeta_);
        SetProtoExtension(ChunkMeta_.mutable_extensions(), DataBlockMetas_);
        auto nameTableExt = ToProto<TNameTableExt>(ChunkNameTable_);
        SetProtoExtension(ChunkMeta_.mutable_extensions(), nameTableExt);
    }

    TFuture<void> StopCoordinator()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
        YT_VERIFY(CoordinatorNode_);

        TDistributedChunkSessionServiceProxy proxy(CoordinatorNode_->Channel);
        auto req = proxy.FinishSession();
        ToProto(req->mutable_session_id(), SessionId_);

        return req->Invoke().ApplyUnique(BIND(&TDistributedChunkSessionController::OnCoordinatorStopped, MakeStrong(this)));
    }

    void OnCoordinatorStopped(TErrorOr<TDistributedChunkSessionServiceProxy::TRspFinishSessionPtr>&& rspOrError)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_LOG_INFO_IF(!rspOrError.IsOK(), rspOrError, "Coordinator stop failed");
    }

    TFuture<std::vector<TNode*>> FinishChunkOnNodes()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<TFuture<void>> finishChunkFutures;
        finishChunkFutures.reserve(Nodes_.size());
        for (auto& node : Nodes_) {
            finishChunkFutures.emplace_back(FinishChunkOnNode(&node));
        }

        return AllSet(std::move(finishChunkFutures))
            .ApplyUnique(BIND(&TDistributedChunkSessionController::OnAllNodesFinished, MakeStrong(this)));
    }

    std::vector<TNode*> OnAllNodesFinished(std::vector<TError>&& responses)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        std::vector<TNode*> finishedNodes;
        finishedNodes.reserve(std::size(responses));

        for (int index = 0; index < std::ssize(responses); ++index) {
            if (responses[index].IsOK()) {
                finishedNodes.push_back(&Nodes_[index]);
            }
        }

        return finishedNodes;
    }

    TFuture<void> FinishChunkOnNode(TNode* node)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        TDataNodeServiceProxy proxy(node->Channel);
        auto req = proxy.FinishChunk();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_block_count(AcknowledgedBlockCount_);
        *req->mutable_chunk_meta() = ChunkMeta_;

        return req->Invoke().ApplyUnique(BIND(&TDistributedChunkSessionController::OnChunkFinished, MakeStrong(this), node));
    }

    void OnChunkFinished(TNode* node, TErrorOr<TDataNodeServiceProxy::TRspFinishChunkPtr>&& rspOrError)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_UNUSED_FUTURE(node->PingExecutor->Stop());

        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Finish chunk failed (Address: %v)", node->Descriptor.GetDefaultAddress());
            rspOrError.ThrowOnError();
            return;
        }

        YT_LOG_DEBUG(
            "Chunk finished (Address: %v, DiskSpace: %v)",
            node->Descriptor.GetDefaultAddress(),
            rspOrError.Value()->chunk_info().disk_space());

        node->ChunkInfo = std::move(*rspOrError.Value()->mutable_chunk_info());
    }

    TFuture<void> ConfirmChunk(std::vector<TNode*> nodes)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
        THROW_ERROR_EXCEPTION_IF(nodes.empty(), "All chunk nodes failed");

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CellTagFromId(SessionId_.ChunkId));
        TChunkServiceProxy proxy(channel);

        auto req = proxy.ConfirmChunk();
        GenerateMutationId(req);

        ToProto(req->mutable_chunk_id(), SessionId_.ChunkId);

        const TChunkInfo* chunkInfo = nullptr;
        for (const auto* node : nodes) {
            if (!chunkInfo) {
                chunkInfo = &node->ChunkInfo;
                continue;
            }

            YT_LOG_WARNING_IF(
                chunkInfo->disk_space() != node->ChunkInfo.disk_space(),
                "Chunk takes different amount of disk space on nodes");

            if (chunkInfo->disk_space() < node->ChunkInfo.disk_space()) {
                chunkInfo = &node->ChunkInfo;
            }
        }

        *req->mutable_chunk_info() = *chunkInfo;
        *req->mutable_chunk_meta() = ChunkMeta_;

        req->set_location_uuids_supported(true);

        for (const auto& node : nodes) {
            auto* replicaInfo = req->add_replicas();
            replicaInfo->set_replica(ToProto(node->ChunkReplica));
            ToProto(replicaInfo->mutable_location_uuid(), node->TargetLocationUuid);
        }

        return req->Invoke().AsVoid();
    }
};

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionControllerPtr CreateDistributedChunkSessionController(
    IClientPtr client,
    TDistributedChunkSessionControllerConfigPtr config,
    TTransactionId transactionId,
    TNameTablePtr chunkNameTable,
    IInvokerPtr invoker)
{
    return New<TDistributedChunkSessionController>(
        std::move(client),
        std::move(config),
        transactionId,
        std::move(chunkNameTable),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
