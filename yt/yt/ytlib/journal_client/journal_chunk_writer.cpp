#include "journal_chunk_writer.h"

#include "helpers.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/retrying_channel.h>

namespace NYT::NJournalClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TJournalChunkWriter
    : public IJournalChunkWriter
{
public:
    TJournalChunkWriter(
        NApi::NNative::IClientPtr client,
        TSessionId sessionId,
        TJournalChunkWriterOptionsPtr options,
        TJournalChunkWriterConfigPtr config,
        const NLogging::TLogger& logger)
        : Client_(std::move(client))
        , SessionId_(sessionId)
        , ChunkId_(SessionId_.ChunkId)
        , Options_(std::move(options))
        , Config_(std::move(config))
        , ReplicaCount_(GetReplicaCount(Options_))
        , Logger(logger.WithTag("ChunkId: %v", ChunkId_))
    { }

    TFuture<void> Open() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&TJournalChunkWriter::DoOpen, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    TFuture<void> Close() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&TJournalChunkWriter::DoClose, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    TFuture<void> WriteRecord(TSharedRef record) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<TSharedRef> recordParts{std::move(record)};
        return BIND(&TJournalChunkWriter::DoWriteRecord,
            MakeStrong(this),
            Passed(std::move(recordParts)),
            /*alreadyEncoded*/ false)
            .AsyncVia(Invoker_)
            .Run();
    }

    TFuture<void> WriteEncodedRecordParts(std::vector<TSharedRef> recordParts) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_VERIFY(Options_->ErasureCodec != NErasure::ECodec::None);

        return BIND(&TJournalChunkWriter::DoWriteRecord,
            MakeStrong(this),
            Passed(std::move(recordParts)),
            /*alreadyEncoded*/ true)
            .AsyncVia(Invoker_)
            .Run();
    }

    bool IsCloseDemanded() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IsCloseDemanded_.load();
    }

private:
    const NApi::NNative::IClientPtr Client_;

    const TSessionId SessionId_;
    const TChunkId ChunkId_;

    const TJournalChunkWriterOptionsPtr Options_;
    const TJournalChunkWriterConfigPtr Config_;

    const int ReplicaCount_;

    const IInvokerPtr Invoker_ = CreateSerializedInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker());

    const NLogging::TLogger Logger;

    struct TNode
        : public TRefCounted
    {
        const int Index;
        const TNodeDescriptor Descriptor;

        TDataNodeServiceProxy LightProxy;
        TDataNodeServiceProxy HeavyProxy;
        TPeriodicExecutorPtr PingExecutor;

        TChunkLocationUuid TargetLocationUuid = InvalidChunkLocationUuid;

        bool IsFlushing = false;

        i64 FirstUnflushedRecordIndex = 0;

        TNode(
            int index,
            TNodeDescriptor descriptor,
            IChannelPtr lightChannel,
            IChannelPtr heavyChannel,
            TDuration requestTimeout)
            : Index(index)
            , Descriptor(std::move(descriptor))
            , LightProxy(std::move(lightChannel))
            , HeavyProxy(std::move(heavyChannel))
        {
            LightProxy.SetDefaultTimeout(requestTimeout);
            HeavyProxy.SetDefaultTimeout(requestTimeout);
        }
    };

    using TNodePtr = TIntrusivePtr<TNode>;

    std::vector<TNodePtr> Nodes_;

    TError Error_;

    std::atomic<bool> IsCloseDemanded_ = false;

    struct TRecord
        : public TRefCounted
    {
        i64 Index = -1;

        std::vector<TSharedRef> ReplicaParts;

        TPromise<void> QuorumFlushedPromise = NewPromise<void>();

        int FlushedReplicaCount = 0;
    };

    using TRecordPtr = TIntrusivePtr<TRecord>;

    std::deque<TRecordPtr> PendingRecords_;
    i64 FirstPendingRecordIndex_ = 0;

    TDelayedExecutorCookie CurrentRecordsFlushCookie_;

    i64 NextRecordIndex_ = 0;

    bool Closed_ = false;

    void DoOpen()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        try {
            GuardedDoOpen();
        } catch (const std::exception& ex) {
            auto error = TError("Failed to open chunk") << ex;
            OnFailed(error);

            THROW_ERROR_EXCEPTION(error);
        }
    }

    void GuardedDoOpen()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto writeTargets = AllocateWriteTargets();
        CreateNodes(writeTargets);
        StartChunkSessions();
        ConfirmChunk(writeTargets);
    }

    TChunkReplicaWithMediumList AllocateWriteTargets()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG("Allocating write targets (SessionId: %v, ReplicaCount: %v)",
            SessionId_,
            ReplicaCount_);

        auto replicas = NChunkClient::AllocateWriteTargets(
            Client_,
            SessionId_,
            ReplicaCount_,
            ReplicaCount_,
            /*replicationFactorOverride*/ std::nullopt,
            /*preferredReplica*/ std::nullopt,
            /*forbiddenAddresses*/ {},
            /*allocatedAddresses*/ {},
            Logger);

        YT_VERIFY(std::ssize(replicas) == ReplicaCount_);
        if (Options_->ErasureCodec != NErasure::ECodec::None) {
            for (int index = 0; index < ReplicaCount_; ++index) {
                replicas[index] = TChunkReplicaWithMedium(replicas[index].GetNodeId(), index, replicas[index].GetMediumIndex());
            }
        }

        YT_LOG_DEBUG("Write targets allocated (Targets: %v)", replicas);

        return replicas;
    }

    void CreateNodes(const TChunkReplicaWithMediumList& replicas)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        Nodes_.reserve(ReplicaCount_);

        for (int index = 0; index < std::ssize(replicas); ++index) {
            auto replica = replicas[index];
            const auto& nodeDirectory = Client_->GetNativeConnection()->GetNodeDirectory();
            const auto& descriptor = nodeDirectory->GetDescriptor(replica);
            auto lightChannel = Client_->GetChannelFactory()->CreateChannel(descriptor);
            auto heavyChannel = CreateRetryingChannel(
                Config_->NodeChannel,
                lightChannel,
                BIND([] (const TError& error) {
                    return static_cast<bool>(error.FindMatching(NChunkClient::EErrorCode::WriteThrottlingActive));
                }));
            auto node = New<TNode>(
                index,
                descriptor,
                std::move(lightChannel),
                std::move(heavyChannel),
                Config_->NodeRpcTimeout);
            Nodes_.push_back(std::move(node));
        }
    }

    void StartChunkSessions()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG("Starting chunk sessions at nodes (SessionId: %v)",
            SessionId_);

        std::vector<TFuture<void>> futures;
        futures.reserve(Nodes_.size());
        for (const auto& node : Nodes_) {
            auto req = node->LightProxy.StartChunk();
            SetRequestWorkloadDescriptor(req, Config_->WorkloadDescriptor);
            ToProto(req->mutable_session_id(), GetSessionIdForNode(node));
            req->set_enable_multiplexing(Options_->EnableMultiplexing);

            futures.push_back(req->Invoke().Apply(
                BIND(&TJournalChunkWriter::OnChunkSessionStarted, MakeStrong(this), node)
                    .AsyncVia(Invoker_)));
        }

        auto result = WaitFor(AllSucceeded(std::move(futures)));
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error starting chunk sessions");

        YT_LOG_DEBUG("Chunk sessions started at nodes (SessionId: %v)",
            SessionId_);
    }

    void OnChunkSessionStarted(
        const TNodePtr& node,
        const TDataNodeServiceProxy::TErrorOrRspStartChunkPtr& rspOrError)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (rspOrError.IsOK()) {
            const auto& rsp = rspOrError.Value();
            if (rsp->has_location_uuid()) {
                node->TargetLocationUuid = FromProto<TChunkLocationUuid>(rspOrError.Value()->location_uuid());
            }

            node->PingExecutor = New<TPeriodicExecutor>(
                Invoker_,
                BIND(&TJournalChunkWriter::PingSession, MakeWeak(this), MakeWeak(node)),
                Config_->NodePingPeriod);
            node->PingExecutor->Start();

            YT_LOG_DEBUG("Chunk session started at node (Address: %v, TargetLocationUuid: %v)",
                node->Descriptor.GetDefaultAddress(),
                node->TargetLocationUuid);
        } else {
            auto error = TError("Failed to start chunk session at %v",
                node->Descriptor.GetDefaultAddress())
                << rspOrError;
            YT_LOG_DEBUG(error);
            THROW_ERROR_EXCEPTION(error);
        }
    }

    void ConfirmChunk(const TChunkReplicaWithMediumList& replicas)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG("Confirming chunk");

        YT_VERIFY(!replicas.empty());
        YT_VERIFY(Nodes_.size() == replicas.size());

        auto batchReq = CreateExecuteBatchRequest();
        auto* req = batchReq->add_confirm_chunk_subrequests();
        ToProto(req->mutable_chunk_id(), ChunkId_);
        req->mutable_chunk_info();
        ToProto(req->mutable_legacy_replicas(), replicas);

        req->set_location_uuids_supported(true);
        for (int index = 0; index < std::ssize(replicas); ++index) {
            auto* replicaInfo = req->add_replicas();
            replicaInfo->set_replica(ToProto<ui64>(replicas[index]));
            ToProto(replicaInfo->mutable_location_uuid(), Nodes_[index]->TargetLocationUuid);
        }

        auto* meta = req->mutable_chunk_meta();
        meta->set_type(ToProto<int>(EChunkType::Journal));
        meta->set_format(ToProto<int>(EChunkFormat::JournalDefault));
        NChunkClient::NProto::TMiscExt miscExt;
        SetProtoExtension(meta->mutable_extensions(), miscExt);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error confirming chunk %v",
            ChunkId_);

        YT_LOG_DEBUG("Chunk confirmed");
    }

    void PingSession(const TWeakPtr<TNode>& weakNode)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto node = weakNode.Lock();
        if (!node) {
            return;
        }

        YT_LOG_DEBUG("Sending ping (Address: %v, SessionId: %v)",
            node->Descriptor.GetDefaultAddress(),
            SessionId_);

        auto req = node->LightProxy.PingSession();
        ToProto(req->mutable_session_id(), GetSessionIdForNode(node));
        req->Invoke().Subscribe(
            BIND(&TJournalChunkWriter::OnPingSent, MakeWeak(this), node)
                .Via(Invoker_));
    }

    void OnPingSent(
        const TNodePtr& node,
        TDataNodeServiceProxy::TErrorOrRspPingSessionPtr rspOrError)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (rspOrError.IsOK()) {
            YT_LOG_DEBUG("Ping succeeded (Address: %v, SessionId: %v)",
                node->Descriptor.GetDefaultAddress(),
                SessionId_);

            const auto& rsp = rspOrError.Value();
            if (rsp->close_demanded()) {
                OnCloseDemanded();
            }
        } else {
            auto error = TError("Failed to ping journal chunk replica") << rspOrError;
            OnFailed(error);
        }
    }

    TFuture<void> DoWriteRecord(std::vector<TSharedRef> recordParts, bool alreadyEncoded)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!Error_.IsOK()) {
            return MakeFuture<void>(Error_);
        }

        if (Closed_) {
            auto error = TError("Journal chunk writer was closed");
            return MakeFuture<void>(error);
        }

        PendingRecords_.push_back(CreateRecord(std::move(recordParts), alreadyEncoded));

        MaybeFlushNodes();

        return PendingRecords_.back()->QuorumFlushedPromise.ToFuture();
    }

    void MaybeFlushNodes()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (std::ssize(PendingRecords_) >= Config_->MaxBatchRowCount) {
            DoFlushNodes();
        }

        if (Config_->MaxBatchDelay == TDuration::Zero()) {
            DoFlushNodes();
        } else if (!CurrentRecordsFlushCookie_) {
            CurrentRecordsFlushCookie_ = TDelayedExecutor::Submit(
                BIND(&TJournalChunkWriter::DoFlushNodes, MakeWeak(this))
                    .Via(Invoker_),
                Config_->MaxBatchDelay);
        }
    }

    void DoFlushNodes()
    {
        TDelayedExecutor::CancelAndClear(CurrentRecordsFlushCookie_);

        for (const auto& node : Nodes_) {
            MaybeFlushNode(node);
        }
    }

    void MaybeFlushNode(const TNodePtr& node)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (node->IsFlushing) {
            return;
        }

        DoFlushNode(node);
    }

    void DoFlushNode(const TNodePtr& node)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(!node->IsFlushing);

        node->IsFlushing = true;

        auto req = node->HeavyProxy.PutBlocks();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        ToProto(req->mutable_session_id(), GetSessionIdForNode(node));
        req->set_flush_blocks(true);
        req->set_first_block_index(node->FirstUnflushedRecordIndex);

        i64 flushRecordCount = 0;
        i64 flushDataSize = 0;

        i64 lastPendingRecordIndex = FirstPendingRecordIndex_ + std::ssize(PendingRecords_) - 1;
        for (
            int recordIndex = node->FirstUnflushedRecordIndex;
            recordIndex <= lastPendingRecordIndex;
            ++recordIndex)
        {
            const auto& record = GetPendingRecord(recordIndex);
            const auto& replicaPart = record->ReplicaParts[node->Index];

            if (FirstPendingRecordIndex_ + Options_->ReplicaLagLimit < recordIndex) {
                break;
            }

            if (flushRecordCount > 0 && flushRecordCount + 1 > Config_->MaxFlushRowCount) {
                break;
            }

            if (flushDataSize > 0 && flushDataSize + std::ssize(replicaPart) > Config_->MaxFlushDataSize) {
                break;
            }

            ++flushRecordCount;
            flushDataSize += std::ssize(replicaPart);

            req->Attachments().push_back(replicaPart);
        }

        if (flushRecordCount == 0) {
            node->IsFlushing = false;
            return;
        }

        YT_LOG_DEBUG("Writing journal replica (Address: %v, Records: %v-%v, DataSize: %v)",
            node->Descriptor.GetDefaultAddress(),
            node->FirstUnflushedRecordIndex,
            node->FirstUnflushedRecordIndex + flushRecordCount - 1,
            flushDataSize);

        req->Invoke().Subscribe(
            BIND_NO_PROPAGATE(&TJournalChunkWriter::OnRecordsFlushed, MakeWeak(this), node, flushRecordCount)
                .Via(Invoker_));
    }

    void OnRecordsFlushed(
        const TNodePtr& node,
        i64 recordCount,
        const TDataNodeServiceProxy::TErrorOrRspPutBlocksPtr& rspOrError)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        node->IsFlushing = false;

        if (!rspOrError.IsOK()) {
            auto error = TError("Failed to flush records to replica %v", node->Descriptor.GetDefaultAddress())
                << rspOrError;
            OnFailed(error);
            return;
        }

        auto firstRecordIndex = node->FirstUnflushedRecordIndex;
        auto lastRecordIndex = firstRecordIndex + recordCount - 1;

        node->FirstUnflushedRecordIndex += recordCount;

        YT_LOG_DEBUG("Journal replica written (Address: %v, Records: %v-%v)",
            node->Descriptor.GetDefaultAddress(),
            firstRecordIndex,
            lastRecordIndex);

        std::vector<TPromise<void>> fulfilledPromises;
        for (i64 recordIndex = firstRecordIndex; recordIndex <= lastRecordIndex; ++recordIndex) {
            auto record = GetPendingRecord(recordIndex);
            auto flushedReplicaCount = ++record->FlushedReplicaCount;

            if (flushedReplicaCount == Options_->WriteQuorum) {
                YT_LOG_DEBUG("Record is flushed to quorum (Record: %v)", recordIndex);

                fulfilledPromises.push_back(record->QuorumFlushedPromise);
            }

            if (flushedReplicaCount == ReplicaCount_) {
                YT_LOG_DEBUG("Record is flushed to all replicas (Record: %v)", recordIndex);

                YT_VERIFY(recordIndex == FirstPendingRecordIndex_);
                ++FirstPendingRecordIndex_;
                PendingRecords_.pop_front();
            }
        }

        for (const auto& promise : fulfilledPromises) {
            promise.TrySet();
        }

        MaybeFlushNodes();
    }

    TSessionId GetSessionIdForNode(const TNodePtr& node)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto chunkId = Options_->ErasureCodec == NErasure::ECodec::None
            ? ChunkId_
            : EncodeChunkId(TChunkIdWithIndex(ChunkId_, node->Index));
        return TSessionId(chunkId, SessionId_.MediumIndex);
    }

    static int GetReplicaCount(const TJournalChunkWriterOptionsPtr& options)
    {
        if (options->ErasureCodec == NErasure::ECodec::None) {
            return options->ReplicationFactor;
        } else {
            auto* codec = NErasure::GetCodec(options->ErasureCodec);
            return codec->GetTotalPartCount();
        }
    }

    TChunkServiceProxy::TReqExecuteBatchPtr CreateExecuteBatchRequest()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto cellTag = CellTagFromId(ChunkId_);
        auto masterChannel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
        TChunkServiceProxy proxy(masterChannel);

        auto batchReq = proxy.ExecuteBatch();
        GenerateMutationId(batchReq);
        SetSuppressUpstreamSync(&batchReq->Header(), true);
        // COMPAT(shakurov): prefer proto ext (above).
        batchReq->set_suppress_upstream_sync(true);

        return batchReq;
    }

    void OnFailed(const TError& error)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_LOG_INFO(error, "Journal chunk writer failed");
        Error_ = error;

        for (const auto& record : PendingRecords_) {
            record->QuorumFlushedPromise.TrySet(error);
        }

        OnWriterFinished();
    }

    void OnCloseDemanded()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!IsCloseDemanded_.exchange(true)) {
            YT_LOG_DEBUG("Journal chunk writer close demanded");
        }
    }

    void DoClose()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG("Closing journal chunk writer");

        OnWriterFinished();
    }

    void OnWriterFinished()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        for (auto& node : Nodes_) {
            if (node->PingExecutor) {
                Y_UNUSED(node->PingExecutor->Stop());
                node->PingExecutor.Reset();
            }
        }

        YT_LOG_DEBUG("Journal chunk writer finished");
    }

    TRecordPtr CreateRecord(std::vector<TSharedRef> recordParts, bool alreadyEncoded)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto record = New<TRecord>();
        record->Index = NextRecordIndex_++;

        record->ReplicaParts.reserve(ReplicaCount_);
        if (Options_->ErasureCodec == NErasure::ECodec::None) {
            YT_VERIFY(recordParts.size() == 1);
            for (int index = 0; index < ReplicaCount_; ++index) {
                record->ReplicaParts.push_back(recordParts[0]);
            }
        } else {
            std::vector<TSharedRef> encodedParts;
            if (alreadyEncoded) {
                YT_VERIFY(ReplicaCount_ == std::ssize(recordParts));
                encodedParts = std::move(recordParts);
            } else {
                YT_VERIFY(recordParts.size() == 1);
                auto encodingResult = EncodeErasureJournalRows(
                    NErasure::GetCodec(Options_->ErasureCodec),
                    recordParts);
                YT_VERIFY(std::ssize(encodingResult) == 1);
                encodedParts = std::move(encodingResult[0]);
            }

            record->ReplicaParts = std::move(encodedParts);
        }

        return record;
    }

    const TRecordPtr& GetPendingRecord(int index)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(
            index >= FirstPendingRecordIndex_ &&
            index < FirstPendingRecordIndex_ + std::ssize(PendingRecords_));

        const auto& record = PendingRecords_[index - FirstPendingRecordIndex_];
        YT_VERIFY(record->Index == index);

        return record;
    }
};

////////////////////////////////////////////////////////////////////////////////

IJournalChunkWriterPtr CreateJournalChunkWriter(
    NApi::NNative::IClientPtr client,
    TSessionId sessionId,
    TJournalChunkWriterOptionsPtr options,
    TJournalChunkWriterConfigPtr config,
    const NLogging::TLogger& logger)
{
    return New<TJournalChunkWriter>(
        std::move(client),
        sessionId,
        std::move(options),
        std::move(config),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
