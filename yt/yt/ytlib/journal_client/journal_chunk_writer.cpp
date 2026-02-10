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

#include <util/random/shuffle.h>

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
        TJournalWriterPerformanceCounters counters,
        const NLogging::TLogger& logger)
        : Client_(std::move(client))
        , SessionId_(sessionId)
        , ChunkId_(SessionId_.ChunkId)
        , Options_(std::move(options))
        , Config_(std::move(config))
        , Counters_(std::move(counters))
        , ReplicaCount_(GetReplicaCount(Options_))
        , Logger(logger.WithTag("ChunkId: %v", ChunkId_))
    { }

    TFuture<void> Open() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND(&TJournalChunkWriter::DoOpen, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    TFuture<void> Close() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND(&TJournalChunkWriter::DoClose, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run()
            .ToUncancelable();
    }

    TFuture<void> WriteRecord(TSharedRef record) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

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
        YT_ASSERT_THREAD_AFFINITY_ANY();

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
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return IsCloseDemanded_.load();
    }

private:
    const NApi::NNative::IClientPtr Client_;

    const TSessionId SessionId_;
    const TChunkId ChunkId_;

    const TJournalChunkWriterOptionsPtr Options_;
    const TJournalChunkWriterConfigPtr Config_;
    const TJournalWriterPerformanceCounters Counters_;

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
        TChunkLocationIndex TargetLocationIndex = InvalidChunkLocationIndex;

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

        TCpuInstant StartTime = GetCpuInstant();
    };

    using TRecordPtr = TIntrusivePtr<TRecord>;

    std::deque<TRecordPtr> PendingRecords_;
    i64 FirstPendingRecordIndex_ = 0;

    TDelayedExecutorCookie CurrentRecordsFlushCookie_;

    i64 NextRecordIndex_ = 0;

    TPromise<void> ClosingPromise_;


    void DoOpen()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

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
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto writeTargets = AllocateWriteTargets();
        CreateNodes(writeTargets);
        StartChunkSessions();
        ConfirmChunk(writeTargets);
    }

    TChunkReplicaWithMediumList AllocateWriteTargets()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG("Allocating write targets (SessionId: %v, ReplicaCount: %v)",
            SessionId_,
            ReplicaCount_);

        TEventTimerGuard timingGuard(Counters_.AllocateWriteTargetsTimer);

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

        Shuffle(replicas.begin(), replicas.end());

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
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

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
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG("Starting chunk sessions at nodes (SessionId: %v)",
            SessionId_);

        TEventTimerGuard timingGuard(Counters_.StartNodeSessionTimer);

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
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (rspOrError.IsOK()) {
            const auto& rsp = rspOrError.Value();
            if (rsp->has_location_uuid()) {
                node->TargetLocationUuid = FromProto<TChunkLocationUuid>(rspOrError.Value()->location_uuid());
            }
            if (rsp->has_location_index()) {
                node->TargetLocationIndex = FromProto<TChunkLocationIndex>(rspOrError.Value()->location_index());
            }

            node->PingExecutor = New<TPeriodicExecutor>(
                Invoker_,
                BIND(&TJournalChunkWriter::PingSession, MakeWeak(this), MakeWeak(node)),
                Config_->NodePingPeriod);
            node->PingExecutor->Start();

            YT_LOG_DEBUG("Chunk session started at node (Address: %v, TargetLocationUuid: %v, TargetLocationIndex: %v)",
                node->Descriptor.GetDefaultAddress(),
                node->TargetLocationUuid,
                node->TargetLocationIndex);
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
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG("Confirming chunk");

        TEventTimerGuard timingGuard(Counters_.ConfirmChunkTimer);

        YT_VERIFY(!replicas.empty());
        YT_VERIFY(Nodes_.size() == replicas.size());

        auto cellTag = CellTagFromId(ChunkId_);
        auto masterChannel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
        TChunkServiceProxy proxy(masterChannel);

        auto req = proxy.ConfirmChunk();
        GenerateMutationId(req);

        ToProto(req->mutable_chunk_id(), ChunkId_);
        req->mutable_chunk_info();

        req->set_location_uuids_supported(true);
        for (int index = 0; index < std::ssize(replicas); ++index) {
            auto* replicaInfo = req->add_replicas();
            replicaInfo->set_replica(ToProto(replicas[index]));
            ToProto(replicaInfo->mutable_location_uuid(), Nodes_[index]->TargetLocationUuid);
            replicaInfo->set_location_index(ToProto<ui32>(Nodes_[index]->TargetLocationIndex));
        }

        auto* meta = req->mutable_chunk_meta();
        meta->set_type(ToProto(EChunkType::Journal));
        meta->set_format(ToProto(EChunkFormat::JournalDefault));
        NChunkClient::NProto::TMiscExt miscExt;
        SetProtoExtension(meta->mutable_extensions(), miscExt);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error confirming chunk %v",
            ChunkId_);

        YT_LOG_DEBUG("Chunk confirmed");
    }

    void PingSession(const TWeakPtr<TNode>& weakNode)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

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
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

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
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (!Error_.IsOK()) {
            return MakeFuture<void>(Error_);
        }

        if (ClosingPromise_) {
            auto error = TError("Journal chunk writer was closed");
            return MakeFuture<void>(error);
        }

        PendingRecords_.push_back(CreateRecord(std::move(recordParts), alreadyEncoded));

        MaybeFlushNodes();

        return PendingRecords_.back()->QuorumFlushedPromise.ToFuture();
    }

    void MaybeFlushNodes()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (!Error_.IsOK()) {
            return;
        }

        UpdateReplicaLagTimes();

        if (Config_->MaxBatchDelay == TDuration::Zero() ||
            std::ssize(PendingRecords_) >= Config_->MaxBatchRowCount)
        {
            DoFlushNodes();
        }

        if (Config_->MaxBatchDelay != TDuration::Zero() && !CurrentRecordsFlushCookie_) {
            CurrentRecordsFlushCookie_ = TDelayedExecutor::Submit(
                BIND(&TJournalChunkWriter::DoFlushNodes, MakeWeak(this))
                    .Via(Invoker_),
                Config_->MaxBatchDelay);
        }
    }

    void UpdateReplicaLagTimes()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto now = GetCpuInstant();

        auto lastPendingRecordIndex = GetLastPendingRecordIndex();
        std::vector<std::pair<NProfiling::TCpuDuration, int>> replicaLagTimes;
        for (int index = 0; index < std::ssize(Nodes_); ++index) {
            const auto& node = Nodes_[index];
            auto nodeLagTime = node->FirstUnflushedRecordIndex == lastPendingRecordIndex + 1
                ? 0
                : now - GetPendingRecord(node->FirstUnflushedRecordIndex)->StartTime;

            replicaLagTimes.emplace_back(nodeLagTime, index);
        }

        std::sort(replicaLagTimes.begin(), replicaLagTimes.end());

        Counters_.WriteQuorumLag.Record(CpuDurationToDuration(replicaLagTimes[Options_->WriteQuorum - 1].first));
        Counters_.MaxReplicaLag.Record(CpuDurationToDuration(replicaLagTimes.back().first));

        YT_LOG_DEBUG("Hunk journal replicas lag updated (Replicas: %v)",
            MakeFormattableView(replicaLagTimes, [&] (auto* builder, const auto& replicaInfo) {
                builder->AppendFormat("%v=>%v",
                    Nodes_[replicaInfo.second]->Descriptor.GetDefaultAddress(),
                    CpuDurationToDuration(replicaInfo.first));
            }));
    }

    void DoFlushNodes()
    {
        TDelayedExecutor::CancelAndClear(CurrentRecordsFlushCookie_);

        if (!Error_.IsOK()) {
            return;
        }

        for (const auto& node : Nodes_) {
            MaybeFlushNode(node);
        }
    }

    void MaybeFlushNode(const TNodePtr& node)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (node->IsFlushing) {
            return;
        }

        DoFlushNode(node);
    }

    void DoFlushNode(const TNodePtr& node)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(!node->IsFlushing);

        node->IsFlushing = true;

        auto req = node->HeavyProxy.PutBlocks();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(EMultiplexingBand::Journal);
        ToProto(req->mutable_session_id(), GetSessionIdForNode(node));
        req->set_flush_blocks(true);
        req->set_first_block_index(node->FirstUnflushedRecordIndex);

        i64 flushRecordCount = 0;
        i64 flushDataSize = 0;

        i64 lastPendingRecordIndex = GetLastPendingRecordIndex();
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
            BIND_NO_PROPAGATE(&TJournalChunkWriter::OnRecordsFlushed,
                MakeWeak(this),
                node,
                flushRecordCount,
                flushDataSize)
                .Via(Invoker_));
    }

    void OnRecordsFlushed(
        const TNodePtr& node,
        i64 recordCount,
        i64 flushDataSize,
        const TDataNodeServiceProxy::TErrorOrRspPutBlocksPtr& rspOrError)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        node->IsFlushing = false;

        if (!rspOrError.IsOK()) {
            auto error = TError("Failed to flush records to replica %v", node->Descriptor.GetDefaultAddress())
                << rspOrError;
            OnFailed(error);
            return;
        }

        const auto& rsp = rspOrError.Value();
        Counters_.JournalWrittenBytes.Increment(flushDataSize);
        Counters_.MediumWrittenBytes.Increment(rsp->statistics().data_bytes_written_to_medium());
        Counters_.IORequestCount.Increment(rsp->statistics().io_requests());

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

                if (PendingRecords_.empty() && ClosingPromise_) {
                    ClosingPromise_.TrySet();
                }
            }
        }

        for (const auto& promise : fulfilledPromises) {
            promise.TrySet();
        }

        MaybeFlushNodes();
    }

    TSessionId GetSessionIdForNode(const TNodePtr& node)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

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

    void OnFailed(const TError& innerError)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto error = TError("Journal chunk writer failed")
            << innerError;
        YT_LOG_ERROR(error);
        Error_ = error;

        for (const auto& record : PendingRecords_) {
            record->QuorumFlushedPromise.TrySet(error);
        }

        if (ClosingPromise_) {
            ClosingPromise_.TrySet(error);
        }

        OnWriterFinished();
    }

    void OnCloseDemanded()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (!IsCloseDemanded_.exchange(true)) {
            YT_LOG_DEBUG("Journal chunk writer close demanded");
        }
    }

    TFuture<void> DoClose()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (!Error_.IsOK()) {
            return MakeFuture(Error_);
        }

        YT_LOG_DEBUG("Closing journal chunk writer");

        ClosingPromise_ = NewPromise<void>();
        if (PendingRecords_.empty()) {
            ClosingPromise_.TrySet();
        }

        DoFlushNodes();

        std::vector<TFuture<void>> quorumFlushFutures;
        quorumFlushFutures.reserve(PendingRecords_.size());
        for (const auto& record : PendingRecords_) {
            quorumFlushFutures.push_back(record->QuorumFlushedPromise);
        }

        return AllSucceeded(std::move(quorumFlushFutures)).Apply(BIND([
            =,
            this,
            this_ = MakeStrong(this)
        ] {
            YT_LOG_DEBUG("Will gracefully wait before finalizing journal chunk writer close (MaxWaitPeriod: %v)",
                Config_->ChunkCloseGracePeriod);

            // NB: We wait for the grace period to let replicas remaining after quorum flush
            // to finish as well so subsequent seal will not trigger undesireable recovery.
            return ClosingPromise_.ToFuture()
                .WithTimeout(Config_->ChunkCloseGracePeriod)
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                    OnWriterFinished();
                    if (!error.IsOK()) {
                        THROW_ERROR(error);
                    }
                })
                .AsyncVia(Invoker_));
        })
            .AsyncVia(Invoker_));
    }

    void OnWriterFinished()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        for (auto& node : Nodes_) {
            if (node->PingExecutor) {
                Y_UNUSED(node->PingExecutor->Stop());
                node->PingExecutor.Reset();
            }
        }

        TDelayedExecutor::CancelAndClear(CurrentRecordsFlushCookie_);

        YT_LOG_DEBUG("Journal chunk writer finished");
    }

    TRecordPtr CreateRecord(std::vector<TSharedRef> recordParts, bool alreadyEncoded)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

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
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(
            index >= FirstPendingRecordIndex_ &&
            index <= GetLastPendingRecordIndex());

        const auto& record = PendingRecords_[index - FirstPendingRecordIndex_];
        YT_VERIFY(record->Index == index);

        return record;
    }

    i64 GetLastPendingRecordIndex() const
    {
        return FirstPendingRecordIndex_ + std::ssize(PendingRecords_) - 1;
    }
};

////////////////////////////////////////////////////////////////////////////////

IJournalChunkWriterPtr CreateJournalChunkWriter(
    NApi::NNative::IClientPtr client,
    TSessionId sessionId,
    TJournalChunkWriterOptionsPtr options,
    TJournalChunkWriterConfigPtr config,
    TJournalWriterPerformanceCounters counters,
    const NLogging::TLogger& logger)
{
    return New<TJournalChunkWriter>(
        std::move(client),
        sessionId,
        std::move(options),
        std::move(config),
        std::move(counters),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
