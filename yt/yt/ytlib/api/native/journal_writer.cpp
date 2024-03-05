#include "journal_writer.h"
#include "private.h"
#include "config.h"
#include "transaction.h"
#include "connection.h"

#include <yt/yt/client/api/journal_writer.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/journal_client/journal_ypath_proxy.h>
#include <yt/yt/ytlib/journal_client/helpers.h>
#include <yt/yt/ytlib/journal_client/proto/format.pb.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/ytlib/transaction_client/transaction_listener.h>
#include <yt/yt/ytlib/transaction_client/helpers.h>
#include <yt/yt/ytlib/transaction_client/config.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/nonblocking_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/library/tracing/async_queue_trace.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <deque>
#include <queue>

namespace NYT::NApi::NNative {

using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NJournalClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient::NProto;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NTracing;

using NYT::TRange;

// Suppress ambiguity with NProto::TSessionId.
using NChunkClient::TSessionId;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJournalWriterChunkSessionState,
    (Allocating)
    (Allocated)
    (Current)
    (Discarded)
);

class TJournalWriter
    : public IJournalWriter
{
public:
    TJournalWriter(
        IClientPtr client,
        TYPath path,
        TJournalWriterOptions options)
        : Impl_(New<TImpl>(
            std::move(client),
            std::move(path),
            std::move(options)))
    { }

    ~TJournalWriter()
    {
        Impl_->Cancel();
    }

    TFuture<void> Open() override
    {
        return Impl_->Open();
    }

    TFuture<void> Write(TRange<TSharedRef> rows) override
    {
        return Impl_->Write(rows);
    }

    TFuture<void> Close() override
    {
        return Impl_->Close();
    }

private:
    // NB: PImpl is used to enable external lifetime control (see TJournalWriter::dtor and TImpl::Cancel).
    class TImpl
        : public TTransactionListener
    {
    public:
        TImpl(
            IClientPtr client,
            const TYPath& path,
            const TJournalWriterOptions& options)
            : Client_(client)
            , Path_(path)
            , Options_(options)
            , Config_(options.Config ? options.Config : New<TJournalWriterConfig>())
            , Counters_(options.Counters)
            , Logger(ApiLogger.WithTag("Path: %v, TransactionId: %v",
                Path_,
                Options_.TransactionId))
        {
            if (Config_->MaxBatchRowCount > options.ReplicaLagLimit) {
                THROW_ERROR_EXCEPTION("\"max_batch_row_count\" cannot be greater than \"replica_lag_limit\"")
                    << TErrorAttribute("max_batch_row_count", Config_->MaxBatchRowCount)
                    << TErrorAttribute("replica_lag_limit", options.ReplicaLagLimit);
            }

            if (Options_.TransactionId) {
                TTransactionAttachOptions attachOptions{
                    .Ping = true
                };
                Transaction_ = Client_->AttachTransaction(Options_.TransactionId, attachOptions);
            }

            for (auto transactionId : Options_.PrerequisiteTransactionIds) {
                TTransactionAttachOptions attachOptions{
                    .Ping = false
                };
                auto transaction = Client_->AttachTransaction(transactionId, attachOptions);
                StartProbeTransaction(transaction, Config_->PrerequisiteTransactionProbePeriod);
            }

            // Spawn the actor.
            YT_UNUSED_FUTURE(BIND(&TImpl::ActorMain, MakeStrong(this))
                .AsyncVia(Invoker_)
                .Run());

            if (Transaction_) {
                StartListenTransaction(Transaction_);
            }
        }

        TFuture<void> Open()
        {
            return OpenedPromise_;
        }

        TFuture<void> Write(TRange<TSharedRef> rows)
        {
            auto guard = Guard(CurrentBatchSpinLock_);

            if (!Error_.IsOK()) {
                return MakeFuture(Error_);
            }

            YT_VERIFY(!Closing_);

            auto result = VoidFuture;
            int payloadBytes = 0;

            for (const auto& row : rows) {
                YT_VERIFY(row);
                auto batch = EnsureCurrentBatch();
                // NB: We can form a handful of batches but since flushes are monotonic,
                // the last one will do.
                result = AppendToBatch(batch, row);

                payloadBytes += row.Size();
            }

            if (auto writeObserver = Counters_.JournalWritesObserver) {
                result.Subscribe(BIND([writeObserver, payloadBytes] (TError error) {
                    if (!error.IsOK()) {
                        return;
                    }
                    writeObserver->RegisterPayloadWrite(payloadBytes);
                }));
            }

            QueueTrace_.Join(CurrentRowIndex_);
            return result;
        }

        TFuture<void> Close()
        {
            if (Config_->DontClose) {
                return VoidFuture;
            }

            EnqueueCommand(TCloseCommand());
            return ClosedPromise_;
        }

        void Cancel()
        {
            EnqueueCommand(TCancelCommand());
        }

    private:
        const IClientPtr Client_;
        const TYPath Path_;
        const TJournalWriterOptions Options_;
        const TJournalWriterConfigPtr Config_;
        const TJournalWriterPerformanceCounters Counters_;
        const NLogging::TLogger Logger;

        const IInvokerPtr Invoker_ = CreateSerializedInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker());


        struct TBatch
            : public TRefCounted
        {
            i64 FirstRowIndex = -1;
            i64 RowCount = 0;
            i64 DataSize = 0;

            std::vector<TSharedRef> Rows;
            std::vector<std::vector<TSharedRef>> ErasureRows;

            const TPromise<void> FlushedPromise = NewPromise<void>();
            int FlushedReplicas = 0;

            TCpuInstant StartTime;
        };

        using TBatchPtr = TIntrusivePtr<TBatch>;

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CurrentBatchSpinLock_);
        TError Error_;
        TBatchPtr CurrentBatch_;
        TDelayedExecutorCookie CurrentBatchFlushCookie_;

        bool Opened_ = false;
        TPromise<void> OpenedPromise_ = NewPromise<void>();

        bool Closing_ = false;
        TPromise<void> ClosedPromise_ = NewPromise<void>();

        NApi::ITransactionPtr Transaction_;
        NApi::ITransactionPtr UploadTransaction_;

        NErasure::ECodec ErasureCodec_ = NErasure::ECodec::None;
        int ReplicationFactor_ = -1;
        int ReplicaCount_ = -1;
        int ReadQuorum_ = -1;
        int WriteQuorum_ = -1;
        TString Account_;
        TString PrimaryMedium_;

        TObjectId ObjectId_;
        TCellTag NativeCellTag_ = InvalidCellTag;
        TCellTag ExternalCellTag_ = InvalidCellTag;

        TChunkListId ChunkListId_;
        IChannelPtr UploadMasterChannel_;

        struct TNode
            : public TRefCounted
        {
            const int Index;
            const TNodeDescriptor Descriptor;

            TDataNodeServiceProxy LightProxy;
            TDataNodeServiceProxy HeavyProxy;
            TPeriodicExecutorPtr PingExecutor;

            bool Started = false;
            TChunkLocationUuid TargetLocationUuid = InvalidChunkLocationUuid;

            i64 FirstPendingBlockIndex = 0;
            i64 FirstPendingRowIndex = -1;

            std::queue<TBatchPtr> PendingBatches;
            std::vector<TBatchPtr> InFlightBatches;

            TCpuDuration LagTime = 0;

            TNode(
                int index,
                TNodeDescriptor descriptor,
                IChannelPtr lightChannel,
                IChannelPtr heavyChannel,
                TDuration rpcTimeout)
                : Index(index)
                , Descriptor(std::move(descriptor))
                , LightProxy(std::move(lightChannel))
                , HeavyProxy(std::move(heavyChannel))
            {
                LightProxy.SetDefaultTimeout(rpcTimeout);
                HeavyProxy.SetDefaultTimeout(rpcTimeout);
            }
        };

        using TNodePtr = TIntrusivePtr<TNode>;
        using TNodeWeakPtr = TWeakPtr<TNode>;

        using EChunkSessionState = EJournalWriterChunkSessionState;

        struct TChunkSession
            : public TRefCounted
        {
            explicit TChunkSession(int index)
                : Index(index)
            { }

            const int Index;

            TSessionId Id;
            std::vector<TNodePtr> Nodes;

            EChunkSessionState State = EChunkSessionState::Allocating;
            bool SwitchScheduled = false;
            bool SealScheduled = false;

            //! Row is called completed iff it is written to all the replicas.
            i64 ReplicationFactorFlushedRowCount = 0;

            //! Row is called flushed iff it is written to #WriteQuorum replicas.
            i64 QuorumFlushedRowCount = 0;
            i64 QuorumFlushedDataSize = 0;

            std::optional<i64> FirstRowIndex;

            TSharedRef HeaderRow;
        };

        using TChunkSessionPtr = TIntrusivePtr<TChunkSession>;
        using TChunkSessionWeakPtr = TWeakPtr<TChunkSession>;

        TChunkSessionPtr CurrentChunkSession_;

        int NextChunkSessionIndex_ = 0;

        TPromise<TChunkSessionPtr> AllocatedChunkSessionPromise_;
        int AllocatedChunkSessionIndex_ = -1;

        i64 CurrentRowIndex_ = 0;

        std::deque<TBatchPtr> QuorumUnflushedBatches_;
        std::deque<TBatchPtr> ReplicationFactorUnflushedBatches_;

        //! Number of flushed batches that are not still written to quorum.
        std::atomic<int> QuorumUnflushedBatchCount_ = 0;

        struct TBatchCommand
        {
            TBatchPtr Batch;
        };

        struct TCloseCommand
        { };

        struct TCancelCommand
        { };

        struct TSwitchChunkCommand
        {
            TChunkSessionPtr Session;
        };

        struct TFailCommand
        {
            TError Error;
        };

        struct TCompleteCommand
        { };

        using TCommand = std::variant<
            TBatchCommand,
            TCloseCommand,
            TCancelCommand,
            TSwitchChunkCommand,
            TFailCommand,
            TCompleteCommand
        >;

        TNonblockingQueue<TCommand> CommandQueue_;

        THashMap<TString, TInstant> BannedNodeToDeadline_;

        bool SealInProgress_ = false;
        int FirstUnsealedSessionIndex_ = 0;
        std::map<int, TChunkSessionPtr> IndexToChunkSessionToSeal_;

        TAsyncQueueTrace QueueTrace_;

        void EnqueueCommand(TCommand command)
        {
            CommandQueue_.Enqueue(std::move(command));
        }

        TCommand DequeueCommand()
        {
            return WaitFor(CommandQueue_.Dequeue())
                .ValueOrThrow();
        }


        void BanNode(const TString& address)
        {
            if (BannedNodeToDeadline_.emplace(address, TInstant::Now() + Config_->NodeBanTimeout).second) {
                YT_LOG_DEBUG("Node banned (Address: %v)", address);
            }
        }

        std::vector<TString> GetBannedNodes()
        {
            std::vector<TString> result;
            auto now = TInstant::Now();
            auto it = BannedNodeToDeadline_.begin();
            while (it != BannedNodeToDeadline_.end()) {
                auto jt = it++;
                if (jt->second < now) {
                    YT_LOG_DEBUG("Node unbanned (Address: %v)", jt->first);
                    BannedNodeToDeadline_.erase(jt);
                } else {
                    result.push_back(jt->first);
                }
            }
            return result;
        }

        void OpenJournal()
        {
            TUserObject userObject(Path_);

            {
                TEventTimerGuard timingGuard(Counters_.GetBasicAttributesTimer);

                GetUserObjectBasicAttributes(
                    Client_,
                    {&userObject},
                    Transaction_ ? Transaction_->GetId() : NullTransactionId,
                    Logger,
                    EPermission::Write);
            }

            ObjectId_ = userObject.ObjectId;
            NativeCellTag_ = CellTagFromId(ObjectId_);
            ExternalCellTag_ = userObject.ExternalCellTag;

            auto objectIdPath = FromObjectId(ObjectId_);

            if (userObject.Type != EObjectType::Journal) {
                THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                    Path_,
                    EObjectType::Journal,
                    userObject.Type);
            }

            UploadMasterChannel_ = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, ExternalCellTag_);

            {
                TEventTimerGuard timingGuard(Counters_.BeginUploadTimer);

                YT_LOG_DEBUG("Starting journal upload");

                auto proxy = CreateObjectServiceWriteProxy(Client_, NativeCellTag_);
                auto batchReq = proxy.ExecuteBatch();

                {
                    auto* prerequisitesExt = batchReq->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
                    for (auto id : Options_.PrerequisiteTransactionIds) {
                        auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
                        ToProto(prerequisiteTransaction->mutable_transaction_id(), id);
                    }
                }

                {
                    auto req = TJournalYPathProxy::BeginUpload(objectIdPath);
                    req->set_update_mode(ToProto<int>(EUpdateMode::Append));
                    req->set_lock_mode(ToProto<int>(ELockMode::Exclusive));
                    req->set_upload_transaction_title(Format("Upload to %v", Path_));
                    req->set_upload_transaction_timeout(ToProto<i64>(Client_->GetNativeConnection()->GetConfig()->UploadTransactionTimeout));
                    ToProto(req->mutable_upload_prerequisite_transaction_ids(), Options_.PrerequisiteTransactionIds);
                    GenerateMutationId(req);
                    SetTransactionId(req, Transaction_);
                    batchReq->AddRequest(req, "begin_upload");
                }

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    GetCumulativeError(batchRspOrError),
                    "Error starting upload to journal %v",
                    Path_);
                const auto& batchRsp = batchRspOrError.Value();

                {
                    auto rsp = batchRsp->GetResponse<TJournalYPathProxy::TRspBeginUpload>("begin_upload").Value();
                    auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());

                    TTransactionAttachOptions options;
                    options.PingAncestors = Options_.PingAncestors;
                    options.AutoAbort = true;
                    UploadTransaction_ = Client_->AttachTransaction(uploadTransactionId, options);
                    StartListenTransaction(UploadTransaction_);

                    YT_LOG_DEBUG("Journal upload started (UploadTransactionId: %v)",
                        uploadTransactionId);
                }
            }

            {
                TEventTimerGuard timingGuard(Counters_.GetExtendedAttributesTimer);

                YT_LOG_DEBUG("Requesting extended journal attributes");

                auto proxy = CreateObjectServiceReadProxy(
                    Client_,
                    EMasterChannelKind::Follower,
                    NativeCellTag_);
                auto req = TYPathProxy::Get(objectIdPath + "/@");
                AddCellTagToSyncWith(req, ObjectId_);
                SetTransactionId(req, UploadTransaction_);
                std::vector<TString> attributeKeys{
                    "type",
                    "erasure_codec",
                    "replication_factor",
                    "read_quorum",
                    "write_quorum",
                    "account",
                    "primary_medium"
                };
                ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

                auto rspOrError = WaitFor(proxy.Execute(req));
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    rspOrError,
                    "Error requesting extended attributes of journal %v",
                    Path_);

                auto rsp = rspOrError.Value();
                auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
                ErasureCodec_ = attributes->Get<NErasure::ECodec>("erasure_codec");
                ReplicationFactor_ = attributes->Get<int>("replication_factor");
                ReplicaCount_ = ErasureCodec_ == NErasure::ECodec::None
                    ? ReplicationFactor_
                    : NErasure::GetCodec(ErasureCodec_)->GetTotalPartCount();
                ReadQuorum_ = attributes->Get<int>("read_quorum");
                WriteQuorum_ = attributes->Get<int>("write_quorum");
                Account_ = attributes->Get<TString>("account");
                PrimaryMedium_ = attributes->Get<TString>("primary_medium");

                YT_LOG_DEBUG("Extended journal attributes received (ErasureCodec: %v, ReplicationFactor: %v, ReplicaCount: %v, "
                    "WriteQuorum: %v, Account: %v, PrimaryMedium: %v)",
                    ErasureCodec_,
                    ReplicationFactor_,
                    ReplicaCount_,
                    WriteQuorum_,
                    Account_,
                    PrimaryMedium_);
            }

            {
                TEventTimerGuard timingGuard(Counters_.GetUploadParametersTimer);

                YT_LOG_DEBUG("Requesting journal upload parameters");

                auto proxy = CreateObjectServiceReadProxy(
                    Client_,
                    EMasterChannelKind::Follower,
                    ExternalCellTag_);
                auto req = TJournalYPathProxy::GetUploadParams(objectIdPath);
                SetTransactionId(req, UploadTransaction_);

                auto rspOrError = WaitFor(proxy.Execute(req));
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    rspOrError,
                    "Error requesting upload parameters for journal %v",
                    Path_);

                const auto& rsp = rspOrError.Value();
                ChunkListId_ = FromProto<TChunkListId>(rsp->chunk_list_id());
                CurrentRowIndex_ = rsp->row_count();

                YT_LOG_DEBUG("Journal upload parameters received (ChunkListId: %v, RowCount: %v)",
                    ChunkListId_,
                    CurrentRowIndex_);
            }
        }

        void CloseJournal()
        {
            YT_LOG_DEBUG("Closing journal");

            TEventTimerGuard timingGuard(Counters_.EndUploadTimer);

            auto objectIdPath = FromObjectId(ObjectId_);

            auto proxy = CreateObjectServiceWriteProxy(Client_, NativeCellTag_);
            auto batchReq = proxy.ExecuteBatch();

            {
                auto* prerequisitesExt = batchReq->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
                for (auto id : Options_.PrerequisiteTransactionIds) {
                    auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
                    ToProto(prerequisiteTransaction->mutable_transaction_id(), id);
                }
            }

            StopListenTransaction(UploadTransaction_);

            {
                auto req = TJournalYPathProxy::EndUpload(objectIdPath);
                SetTransactionId(req, UploadTransaction_);
                GenerateMutationId(req);
                batchReq->AddRequest(req, "end_upload");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                "Error finishing upload to journal %v",
                Path_);

            UploadTransaction_->Detach();

            if (AllocatedChunkSessionPromise_) {
                AllocatedChunkSessionPromise_.ToFuture()
                    .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TChunkSessionPtr>& sessionOrError) {
                        if (sessionOrError.IsOK()) {
                            const auto& session = sessionOrError.Value();
                            ScheduleChunkSessionSeal(session);
                        }
                    }).Via(Invoker_));
            }

            ClosedPromise_.TrySet(TError());

            YT_LOG_DEBUG("Journal closed");
        }

        TChunkSessionPtr TryOpenChunkSession(int sessionIndex)
        {
            auto session = New<TChunkSession>(sessionIndex);

            TEventTimerGuard timingGuard(Counters_.OpenSessionTimer);
            TWallTimer timer;

            YT_LOG_DEBUG("Creating chunk");

            {
                TEventTimerGuard timingGuard(Counters_.CreateChunkTimer);

                auto batchReq = CreateExecuteBatchRequest();
                if (AreOverlayedChunksEnabled()) {
                    batchReq->RequireServerFeature(EMasterFeature::OverlayedJournals);
                }

                // NB: It is too dangerous to throttle journals.
                // Hence we omit setting logical_request_weight.
                // And set user to "root".
                auto* req = batchReq->add_create_chunk_subrequests();
                req->set_type(ToProto<int>(ErasureCodec_ == NErasure::ECodec::None ? EObjectType::JournalChunk : EObjectType::ErasureJournalChunk));
                req->set_account(Account_);
                ToProto(req->mutable_transaction_id(), UploadTransaction_->GetId());
                req->set_replication_factor(ReplicationFactor_);
                req->set_medium_name(PrimaryMedium_);
                req->set_erasure_codec(ToProto<int>(ErasureCodec_));
                req->set_read_quorum(ReadQuorum_);
                req->set_write_quorum(WriteQuorum_);
                req->set_movable(true);
                req->set_vital(true);
                req->set_overlayed(AreOverlayedChunksEnabled());
                req->set_replica_lag_limit(Options_.ReplicaLagLimit);

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    GetCumulativeError(batchRspOrError),
                    "Error creating chunk");

                const auto& batchRsp = batchRspOrError.Value();
                const auto& rsp = batchRsp->create_chunk_subresponses(0);

                session->Id = FromProto<TSessionId>(rsp.session_id());
            }

            YT_LOG_DEBUG("Chunk created (SessionId: %v, ElapsedTime: %v)",
                session->Id,
                timer.GetElapsedTime());

            auto chunkId = session->Id.ChunkId;

            TChunkReplicaWithMediumList replicas;
            try {
                TEventTimerGuard timingGuard(Counters_.AllocateWriteTargetsTimer);

                // TODO(gritukan): Pass host name from tablet node.
                auto preferredReplica = Config_->PreferLocalHost
                    ? std::make_optional(NNet::GetLocalHostName())
                    : std::nullopt;

                replicas = AllocateWriteTargets(
                    Client_,
                    session->Id,
                    ReplicaCount_,
                    ReplicaCount_,
                    /*replicationFactorOverride*/ std::nullopt,
                    preferredReplica,
                    GetBannedNodes(),
                    /*allocatedAddresses*/ {},
                    Logger);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(TError(ex));
                return nullptr;
            }

            YT_VERIFY(std::ssize(replicas) == ReplicaCount_);
            if (ErasureCodec_ != NErasure::ECodec::None) {
                for (int index = 0; index < ReplicaCount_; ++index) {
                    replicas[index] = TChunkReplicaWithMedium(replicas[index].GetNodeId(), index, replicas[index].GetMediumIndex());
                }
            }

            const auto& nodeDirectory = Client_->GetNativeConnection()->GetNodeDirectory();

            for (int index = 0; index < std::ssize(replicas); ++index) {
                auto replica = replicas[index];
                const auto& descriptor = nodeDirectory->GetDescriptor(replica);
                auto lightChannel = Client_->GetChannelFactory()->CreateChannel(descriptor);
                auto heavyChannel = CreateRetryingChannel(
                    Config_->NodeChannel,
                    lightChannel,
                    BIND([] (const TError& error) {
                        return error.FindMatching(NChunkClient::EErrorCode::WriteThrottlingActive).operator bool();
                    }));
                auto node = New<TNode>(
                    index,
                    descriptor,
                    std::move(lightChannel),
                    std::move(heavyChannel),
                    Config_->NodeRpcTimeout);
                session->Nodes.push_back(node);
            }

            YT_LOG_DEBUG("Starting chunk session at nodes (SessionId: %v, ElapsedTime: %v)",
                session->Id,
                timer.GetElapsedTime());

            try {
                TEventTimerGuard timingGuard(Counters_.StartNodeSessionTimer);

                std::vector<TFuture<void>> futures;
                for (const auto& node : session->Nodes) {
                    auto req = node->LightProxy.StartChunk();
                    SetRequestWorkloadDescriptor(req, Config_->WorkloadDescriptor);
                    ToProto(req->mutable_session_id(), GetSessionIdForNode(session, node));
                    req->set_enable_multiplexing(Options_.EnableMultiplexing);

                    futures.push_back(req->Invoke().Apply(
                        BIND(&TImpl::OnChunkStarted, MakeStrong(this), session, node)
                            .AsyncVia(Invoker_)));
                }

                auto result = WaitFor(AllSucceeded(
                    futures,
                    TFutureCombinerOptions{.CancelInputOnShortcut = false}));
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error starting chunk sessions");
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(TError(ex));
                return nullptr;
            }

            YT_LOG_DEBUG("Chunk session started at nodes (SessionId: %v, ElapsedTime: %v)",
                session->Id,
                timer.GetElapsedTime());

            for (const auto& node : session->Nodes) {
                node->PingExecutor = New<TPeriodicExecutor>(
                    Invoker_,
                    BIND(&TImpl::SendPing, MakeWeak(this), MakeWeak(session), MakeWeak(node)),
                    Config_->NodePingPeriod);
                node->PingExecutor->Start();
            }

            YT_LOG_DEBUG("Confirming chunk (SessionId: %v, ElapsedTime: %v)",
                session->Id,
                timer.GetElapsedTime());

            {
                TEventTimerGuard timingGuard(Counters_.ConfirmChunkTimer);

                auto batchReq = CreateExecuteBatchRequest();
                YT_VERIFY(!replicas.empty());
                YT_VERIFY(session->Nodes.size() == replicas.size());
                auto* req = batchReq->add_confirm_chunk_subrequests();
                ToProto(req->mutable_chunk_id(), chunkId);
                req->mutable_chunk_info();
                ToProto(req->mutable_legacy_replicas(), replicas);

                req->set_location_uuids_supported(true);

                bool useLocationUuids = std::all_of(session->Nodes.begin(), session->Nodes.end(), [] (const auto& node) {
                    return node->TargetLocationUuid != InvalidChunkLocationUuid;
                });

                if (useLocationUuids) {
                    for (int i = 0; i < std::ssize(replicas); ++i) {
                        auto* replicaInfo = req->add_replicas();
                        replicaInfo->set_replica(ToProto<ui64>(replicas[i]));
                        ToProto(replicaInfo->mutable_location_uuid(), session->Nodes[i]->TargetLocationUuid);
                    }
                }

                auto* meta = req->mutable_chunk_meta();
                meta->set_type(ToProto<int>(EChunkType::Journal));
                meta->set_format(ToProto<int>(EChunkFormat::JournalDefault));
                TMiscExt miscExt;
                SetProtoExtension(meta->mutable_extensions(), miscExt);

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    GetCumulativeError(batchRspOrError),
                    "Error confirming chunk %v",
                    chunkId);
            }
            YT_LOG_DEBUG("Chunk confirmed (SessionId: %v, ElapsedTime: %v)",
                session->Id,
                timer.GetElapsedTime());

            YT_LOG_DEBUG("Attaching chunk (SessionId: %v, ElapsedTime: %v)",
                session->Id,
                timer.GetElapsedTime());
            {
                TEventTimerGuard timingGuard(Counters_.AttachChunkTimer);

                auto batchReq = CreateExecuteBatchRequest();
                auto* req = batchReq->add_attach_chunk_trees_subrequests();
                ToProto(req->mutable_parent_id(), ChunkListId_);
                ToProto(req->add_child_ids(), chunkId);

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    GetCumulativeError(batchRspOrError),
                    "Error attaching chunk %v",
                    chunkId);
            }
            YT_LOG_DEBUG("Chunk attached (SessionId: %v, ElapsedTime: %v)",
                session->Id,
                timer.GetElapsedTime());

            // First successfully opened chunk session indicates that the whole writer is now open.
            if (!std::exchange(Opened_, true)) {
                if (auto openDelay = Config_->OpenDelay) {
                    TDelayedExecutor::WaitForDuration(*openDelay);
                }

                if (OpenedPromise_.TrySet(TError())) {
                    YT_LOG_DEBUG("Journal opened");
                }
            }

            return session;
        }

        void ScheduleChunkSessionAllocation()
        {
            if (AllocatedChunkSessionPromise_) {
                return;
            }

            AllocatedChunkSessionIndex_ = NextChunkSessionIndex_++;
            AllocatedChunkSessionPromise_ = NewPromise<TChunkSessionPtr>();

            YT_LOG_DEBUG("Scheduling chunk session allocation (SessionIndex: %v)",
                AllocatedChunkSessionIndex_);

            ScheduleAllocateChunkSession(AllocatedChunkSessionPromise_, AllocatedChunkSessionIndex_);
        }

        void ScheduleAllocateChunkSession(TPromise<TChunkSessionPtr> promise, int sessionIndex)
        {
            BIND(&TImpl::TryOpenChunkSession, MakeStrong(this), sessionIndex)
                .AsyncVia(Invoker_)
                .Run()
                .Subscribe(
                    BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TChunkSessionPtr>& sessionOrError) {
                        if (!sessionOrError.IsOK()) {
                            YT_LOG_WARNING(sessionOrError, "Failed to allocate chunk session (SessionIndex: %v)",
                                sessionIndex);
                            promise.TrySet(sessionOrError);
                            return;
                        }

                        const auto& session = sessionOrError.Value();
                        if (!session) {
                            YT_LOG_DEBUG("Failed to allocate chunk session; backing off and retrying (SessionIndex: %v, BackoffTime: %v)",
                                sessionIndex,
                                Config_->OpenSessionBackoffTime);

                            TDelayedExecutor::Submit(
                                BIND(&TImpl::ScheduleAllocateChunkSession, MakeWeak(this), promise, sessionIndex)
                                    .Via(Invoker_),
                                Config_->OpenSessionBackoffTime);
                            return;
                        }

                        // NB: Avoid overwriting EChunkSessionState::Discarded state.
                        if (session->State == EChunkSessionState::Allocating) {
                            session->State = EChunkSessionState::Allocated;
                        }

                        if (session->State == EChunkSessionState::Discarded) {
                            ScheduleChunkSessionSeal(session);
                        }

                        YT_LOG_DEBUG("Chunk session allocated (SessionIndex: %v, SessionId: %v, SessionState: %v)",
                            sessionIndex,
                            session->Id,
                            session->State);
                        promise.TrySet(session);
                    }).Via(Invoker_));
        }

        TChunkSessionPtr OpenChunkSession()
        {
            while (true) {
                ScheduleChunkSessionAllocation();

                auto future = AllocatedChunkSessionPromise_.ToFuture();

                AllocatedChunkSessionIndex_ = -1;
                AllocatedChunkSessionPromise_.Reset();

                auto session = WaitFor(future)
                    .ValueOrThrow();

                if (IsChunkPreallocationEnabled()) {
                    ScheduleChunkSessionAllocation();
                }

                if (session->State == EChunkSessionState::Allocated) {
                    YT_VERIFY(session->State == EChunkSessionState::Allocated);
                    session->State = EChunkSessionState::Current;
                    return session;
                }

                YT_LOG_DEBUG("Skipping chunk session due to invalid state (SessionId: %v, SessionState: %v)",
                    session->Id,
                    session->State);
            }
        }

        void OpenChunk()
        {
            CurrentChunkSession_ = OpenChunkSession();

            YT_LOG_DEBUG("Current chunk session updated (SessionId: %v)",
                CurrentChunkSession_->Id);

            ReplicationFactorUnflushedBatches_.clear();

            if (!QuorumUnflushedBatches_.empty()) {
                const auto& firstBatch = QuorumUnflushedBatches_.front();
                const auto& lastBatch = QuorumUnflushedBatches_.back();
                YT_LOG_DEBUG("Batches re-enqueued (SessionId: %v, Rows: %v-%v)",
                    CurrentChunkSession_->Id,
                    firstBatch->FirstRowIndex,
                    lastBatch->FirstRowIndex + lastBatch->RowCount - 1);

                for (const auto& batch : QuorumUnflushedBatches_) {
                    ReplicationFactorUnflushedBatches_.push_back(batch);
                    EnqueueBatchToCurrentChunkSession(batch);
                }
            }

            TDelayedExecutor::Submit(
                BIND(&TImpl::OnSessionTimeout, MakeWeak(this), MakeWeak(CurrentChunkSession_)),
                Config_->MaxChunkSessionDuration);
        }

        void OnSessionTimeout(const TWeakPtr<TChunkSession>& session_)
        {
            auto session = session_.Lock();
            if (!session) {
                return;
            }

            YT_LOG_DEBUG("Session timeout; requesting chunk switch");
            ScheduleChunkSessionSwitch(session);
        }

        void WriteChunk()
        {
            while (!IsCompleted()) {
                ValidateAborted();
                auto command = DequeueCommand();
                auto switchChunk = false;
                Visit(command,
                    [&] (TCloseCommand) {
                        HandleClose();
                    },
                    [&] (TCancelCommand) {
                        HandleCancel();
                    },
                    [&] (const TBatchCommand& typedCommand) {
                        const auto& batch = typedCommand.Batch;

                        YT_LOG_DEBUG("Batch enqueued (Rows: %v-%v)",
                            batch->FirstRowIndex,
                            batch->FirstRowIndex + batch->RowCount - 1);

                        HandleBatch(batch);
                    },
                    [&] (const TSwitchChunkCommand& typedCommand) {
                        if (typedCommand.Session != CurrentChunkSession_) {
                            return;
                        }
                        switchChunk = true;
                    },
                    [&] (const TFailCommand& typedCommand) {
                        THROW_ERROR(typedCommand.Error);
                    },
                    [&] (TCompleteCommand) {
                        YT_VERIFY(IsCompleted());
                    });

                if (switchChunk) {
                    YT_LOG_DEBUG("Switching chunk");
                    break;
                }
            }
        }

        void HandleClose()
        {
            if (Closing_) {
                return;
            }

            YT_LOG_DEBUG("Closing journal writer");
            Closing_ = true;

            {
                auto guard = Guard(CurrentBatchSpinLock_);
                FlushCurrentBatch();
            }
        }

        [[noreturn]] void HandleCancel()
        {
            if (AllocatedChunkSessionPromise_) {
                AllocatedChunkSessionPromise_.TrySet(TError(NYT::EErrorCode::Canceled, "Writer canceled"));
            }
            throw TFiberCanceledException();
        }

        void HandleBatch(const TBatchPtr& batch)
        {
            if (ErasureCodec_ != NErasure::ECodec::None) {
                batch->ErasureRows = EncodeErasureJournalRows(NErasure::GetCodec(ErasureCodec_), batch->Rows);
                batch->Rows.clear();
            }
            QuorumUnflushedBatches_.push_back(batch);
            ReplicationFactorUnflushedBatches_.push_back(batch);
            EnqueueBatchToCurrentChunkSession(batch);
        }

        void EnqueueBatchToCurrentChunkSession(const TBatchPtr& batch)
        {
            // Check flushed replica count: this batch might have already been
            // flushed (partially) by the previous (failed) session.
            if (batch->FlushedReplicas > 0) {
                YT_LOG_DEBUG("Resetting flushed replica counter (Rows: %v-%v, FlushCounter: %v)",
                    batch->FirstRowIndex,
                    batch->FirstRowIndex + batch->RowCount - 1,
                    batch->FlushedReplicas);
                batch->FlushedReplicas = 0;
            }

            if (!CurrentChunkSession_->FirstRowIndex) {
                auto firstRowIndex = batch->FirstRowIndex;

                YT_LOG_DEBUG("Initializing first row index of chunk session (SessionId: %v, FirstRowIndex: %v)",
                    CurrentChunkSession_->Id,
                    firstRowIndex);

                CurrentChunkSession_->FirstRowIndex = firstRowIndex;
                for (const auto& node : CurrentChunkSession_->Nodes) {
                    node->FirstPendingRowIndex = firstRowIndex;
                }

                NJournalClient::NProto::TOverlayedJournalChunkHeader header;
                header.set_first_row_index(firstRowIndex);

                CurrentChunkSession_->HeaderRow = SerializeProtoToRef(header);
            }

            for (const auto& node : CurrentChunkSession_->Nodes) {
                node->PendingBatches.push(batch);
                MaybeFlushBlocks(CurrentChunkSession_, node);
            }
        }

        void CloseChunk()
        {
            // Release the current session to prevent writing more rows
            // or detecting failed pings.
            auto session = CurrentChunkSession_;
            CurrentChunkSession_.Reset();

            session->State = EChunkSessionState::Discarded;

            auto sessionId = session->Id;

            YT_LOG_DEBUG("Finishing chunk session");

            for (const auto& node : session->Nodes) {
                auto req = node->LightProxy.FinishChunk();
                ToProto(req->mutable_session_id(), GetSessionIdForNode(session, node));
                req->Invoke().Subscribe(
                    BIND(&TImpl::OnChunkFinished, MakeStrong(this), node)
                        .Via(Invoker_));
                if (node->PingExecutor) {
                    YT_UNUSED_FUTURE(node->PingExecutor->Stop());
                    node->PingExecutor.Reset();
                }
            }

            if (IsChunkPreallocationEnabled()) {
                ScheduleChunkSessionSeal(session);
            } else {
                if (Config_->DontSeal && Config_->DontPreallocate) {
                    YT_LOG_WARNING("Client-side chunk seal is disabled, skipping chunk session seal (SessionId: %v)",
                        session->Id);
                    return;
                }

                TEventTimerGuard timingGuard(Counters_.SealChunkTimer);

                YT_LOG_DEBUG("Sealing chunk (SessionId: %v, RowCount: %v)",
                    sessionId,
                    session->QuorumFlushedRowCount);

                auto batchReq = CreateExecuteBatchRequest();
                auto* req = batchReq->add_seal_chunk_subrequests();
                ToProto(req->mutable_chunk_id(), sessionId.ChunkId);
                req->mutable_info()->set_row_count(session->QuorumFlushedRowCount);
                req->mutable_info()->set_uncompressed_data_size(session->QuorumFlushedDataSize);
                req->mutable_info()->set_compressed_data_size(session->QuorumFlushedDataSize);
                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    GetCumulativeError(batchRspOrError),
                    "Error sealing chunk %v",
                    sessionId);

                YT_LOG_DEBUG("Chunk sealed (SessionId: %v)",
                    sessionId);
            }
        }


        void ActorMain()
        {
            try {
                GuardedActorMain();
            } catch (const std::exception& ex) {
                try {
                    PumpFailed(ex);
                } catch (const std::exception& ex) {
                    YT_LOG_ERROR(ex, "Error pumping journal writer command queue");
                }
            }
        }

        void GuardedActorMain()
        {
            OpenJournal();
            do {
                OpenChunk();
                WriteChunk();
                CloseChunk();
            } while (!IsCompleted());
            CloseJournal();
        }

        void PumpFailed(const TError& error)
        {
            YT_LOG_WARNING(error, "Journal writer failed");

            {
                auto guard = Guard(CurrentBatchSpinLock_);
                Error_ = error;
                if (CurrentBatch_) {
                    auto promise = CurrentBatch_->FlushedPromise;
                    CurrentBatch_.Reset();
                    guard.Release();
                    promise.Set(error);
                }
            }

            OpenedPromise_.TrySet(error);
            ClosedPromise_.TrySet(error);

            for (const auto& batch : QuorumUnflushedBatches_) {
                batch->FlushedPromise.Set(error);
            }
            QuorumUnflushedBatches_.clear();
            QuorumUnflushedBatchCount_ = 0;
            ReplicationFactorUnflushedBatches_.clear();

            bool cancel = false;
            while (!cancel) {
                auto command = DequeueCommand();
                Visit(command,
                    [&] (const TBatchCommand& typedCommand) {
                        const auto& batch = typedCommand.Batch;
                        batch->FlushedPromise.Set(error);
                    },
                    [&] (TCancelCommand) { cancel = true; },
                    [&] (const auto&) { /*ignore*/ });
            }

            if (CurrentChunkSession_) {
                ScheduleChunkSessionSeal(CurrentChunkSession_);
            }

            if (AllocatedChunkSessionPromise_) {
                AllocatedChunkSessionPromise_.ToFuture()
                    .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TChunkSessionPtr>& sessionOrError) {
                        if (sessionOrError.IsOK()) {
                            const auto& session = sessionOrError.Value();
                            ScheduleChunkSessionSeal(session);
                        }
                    }).Via(Invoker_));
            }

            throw TFiberCanceledException();
        }


        TFuture<void> AppendToBatch(const TBatchPtr& batch, const TSharedRef& row)
        {
            YT_ASSERT(row);
            batch->Rows.push_back(row);
            batch->RowCount += 1;
            batch->DataSize += row.Size();
            ++CurrentRowIndex_;
            return batch->FlushedPromise;
        }

        TBatchPtr EnsureCurrentBatch()
        {
            VERIFY_SPINLOCK_AFFINITY(CurrentBatchSpinLock_);
            YT_VERIFY(!Closing_);

            if (CurrentBatch_ &&
                (CurrentBatch_->RowCount >= Config_->MaxBatchRowCount ||
                 CurrentBatch_->DataSize >= Config_->MaxBatchDataSize))
            {
                FlushCurrentBatch();
            }

            if (!CurrentBatch_) {
                CurrentBatch_ = New<TBatch>();
                CurrentBatch_->StartTime = GetCpuInstant();
                CurrentBatch_->FirstRowIndex = CurrentRowIndex_;
                CurrentBatchFlushCookie_ = TDelayedExecutor::Submit(
                    BIND(&TImpl::OnBatchTimeout, MakeWeak(this), CurrentBatch_)
                        .Via(Invoker_),
                    Config_->MaxBatchDelay);
            }

            return CurrentBatch_;
        }

        void OnBatchTimeout(const TBatchPtr& batch)
        {
            auto guard = Guard(CurrentBatchSpinLock_);
            if (CurrentBatch_ == batch) {
                FlushCurrentBatch();
            }
        }

        void FlushCurrentBatch()
        {
            VERIFY_SPINLOCK_AFFINITY(CurrentBatchSpinLock_);

            if (!CurrentBatch_) {
                return;
            }

            TDelayedExecutor::CancelAndClear(CurrentBatchFlushCookie_);

            YT_LOG_DEBUG("Flushing batch (Rows: %v-%v, DataSize: %v)",
                CurrentBatch_->FirstRowIndex,
                CurrentBatch_->FirstRowIndex + CurrentBatch_->RowCount - 1,
                CurrentBatch_->DataSize);

            ++QuorumUnflushedBatchCount_;
            EnqueueCommand(TBatchCommand{CurrentBatch_});
            CurrentBatch_.Reset();
        }


        void SendPing(
            const TChunkSessionWeakPtr& weakSession,
            const TNodeWeakPtr& weakNode)
        {
            auto session = weakSession.Lock();
            if (!session) {
                return;
            }

            auto node = weakNode.Lock();
            if (!node) {
                return;
            }

            if (!node->Started) {
                return;
            }

            YT_LOG_DEBUG("Sending ping (Address: %v, SessionId: %v)",
                node->Descriptor.GetDefaultAddress(),
                session->Id);

            auto req = node->LightProxy.PingSession();
            ToProto(req->mutable_session_id(), GetSessionIdForNode(session, node));
            req->Invoke().Subscribe(
                BIND(&TImpl::OnPingSent, MakeWeak(this), session, node)
                    .Via(Invoker_));
        }

        void OnPingSent(
            const TChunkSessionPtr& session,
            const TNodePtr& node,
            TDataNodeServiceProxy::TErrorOrRspPingSessionPtr rspOrError)
        {
            if (SimulateReplicaFailure(node, session)) {
                return;
            }
            if (rspOrError.IsOK()) {
                YT_LOG_DEBUG("Ping succeeded (Address: %v, SessionId: %v)",
                    node->Descriptor.GetDefaultAddress(),
                    session->Id);

                const auto& rsp = rspOrError.Value();
                if (rsp->close_demanded()) {
                    OnReplicaCloseDemanded(node, session);
                }
            } else {
                OnReplicaFailure(rspOrError, node, session);
            }
        }


        void OnChunkStarted(
            const TChunkSessionPtr& session,
            const TNodePtr& node,
            const TDataNodeServiceProxy::TErrorOrRspStartChunkPtr& rspOrError)
        {
            if (rspOrError.IsOK()) {
                YT_LOG_DEBUG("Chunk session started at node (Address: %v)",
                    node->Descriptor.GetDefaultAddress());
                if (rspOrError.Value()->has_location_uuid()) {
                    node->TargetLocationUuid = FromProto<TChunkLocationUuid>(rspOrError.Value()->location_uuid());
                }
                node->Started = true;
                if (CurrentChunkSession_ == session) {
                    MaybeFlushBlocks(CurrentChunkSession_, node);
                }
            } else {
                YT_LOG_WARNING(rspOrError, "Session has failed to start at node; requesting chunk switch (SessionId: %v, Address: %v)",
                    session->Id,
                    node->Descriptor.GetDefaultAddress());
                ScheduleChunkSessionSwitch(session);
                BanNode(node->Descriptor.GetDefaultAddress());
                THROW_ERROR_EXCEPTION("Error starting session at %v",
                    node->Descriptor.GetDefaultAddress())
                    << rspOrError;
            }
        }

        void OnChunkFinished(
            const TNodePtr& node,
            const TDataNodeServiceProxy::TErrorOrRspFinishChunkPtr& rspOrError)
        {
            if (rspOrError.IsOK()) {
                YT_LOG_DEBUG("Chunk session finished at node (Address: %v)",
                    node->Descriptor.GetDefaultAddress());
            } else {
                BanNode(node->Descriptor.GetDefaultAddress());
                YT_LOG_WARNING(rspOrError, "Chunk session has failed to finish at node (Address: %v)",
                    node->Descriptor.GetDefaultAddress());
            }
        }


        void MaybeFlushBlocks(const TChunkSessionPtr& session, const TNodePtr& node)
        {
            if (!node->Started) {
                return;
            }

            if (session->SwitchScheduled) {
                return;
            }

            YT_VERIFY(node->FirstPendingRowIndex >= 0);

            if (!node->InFlightBatches.empty()) {
                auto lagTime = GetCpuInstant() - node->InFlightBatches.front()->StartTime;
                UpdateReplicaLag(session, node, lagTime);
                return;
            }

            if (node->PendingBatches.empty()) {
                UpdateReplicaLag(session, node, 0);
                return;
            }

            auto lagTime = GetCpuInstant() - node->PendingBatches.front()->StartTime;
            UpdateReplicaLag(session, node, lagTime);

            i64 flushRowCount = 0;
            i64 flushDataSize = 0;

            auto req = node->HeavyProxy.PutBlocks();
            req->SetResponseHeavy(true);
            req->SetMultiplexingBand(EMultiplexingBand::Heavy);
            ToProto(req->mutable_session_id(), GetSessionIdForNode(CurrentChunkSession_, node));
            req->set_flush_blocks(true);

            if (AreOverlayedChunksEnabled()) {
                if (node->FirstPendingBlockIndex == 0) {
                    req->set_first_block_index(0);
                    req->Attachments().push_back(CurrentChunkSession_->HeaderRow);
                } else {
                    req->set_first_block_index(node->FirstPendingBlockIndex + 1);
                }
            } else {
                req->set_first_block_index(node->FirstPendingBlockIndex);
            }

            YT_VERIFY(node->InFlightBatches.empty());
            while (!node->PendingBatches.empty()) {
                auto batch = node->PendingBatches.front();

                i64 replicaRowCountAfterFlush =
                    node->FirstPendingBlockIndex + // rows flushed to node
                    flushRowCount +                // rows already in flush session
                    batch->RowCount;               // rows in current batch
                if (session->ReplicationFactorFlushedRowCount + Options_.ReplicaLagLimit < replicaRowCountAfterFlush) {
                    break;
                }

                if (flushRowCount > 0 && flushRowCount + batch->RowCount > Config_->MaxFlushRowCount) {
                    break;
                }
                if (flushDataSize > 0 && flushDataSize + batch->DataSize > Config_->MaxFlushDataSize) {
                    break;
                }

                node->PendingBatches.pop();

                const auto& rows = ErasureCodec_ == NErasure::ECodec::None
                    ? batch->Rows
                    : batch->ErasureRows[node->Index];
                req->Attachments().insert(req->Attachments().end(), rows.begin(), rows.end());

                flushRowCount += batch->RowCount;
                flushDataSize += GetByteSize(rows);

                node->InFlightBatches.push_back(batch);
            }

            if (node->InFlightBatches.empty()) {
                return;
            }

            YT_LOG_DEBUG("Writing journal replica (Address: %v, BlockIds: %v:%v-%v, Rows: %v-%v, DataSize: %v, LagTime: %v)",
                node->Descriptor.GetDefaultAddress(),
                CurrentChunkSession_->Id,
                node->FirstPendingBlockIndex,
                node->FirstPendingBlockIndex + flushRowCount - 1,
                node->FirstPendingRowIndex,
                node->FirstPendingRowIndex + flushRowCount - 1,
                flushDataSize,
                CpuDurationToDuration(lagTime));

            if (SimulateReplicaTimeout(node, session, flushRowCount)) {
                return;
            }

            auto traceGuard = QueueTrace_.CreateTraceGuard("JournalWriter:FlushBlocks", node->FirstPendingRowIndex, {});

            req->Invoke().Subscribe(
                BIND_NO_PROPAGATE(&TImpl::OnBlocksWritten, MakeStrong(this), CurrentChunkSession_, node, flushRowCount, flushDataSize)
                    .Via(Invoker_));
        }

        void OnBlocksWritten(
            const TChunkSessionPtr& session,
            const TNodePtr& node,
            i64 flushRowCount,
            i64 flushDataSize,
            TDataNodeServiceProxy::TErrorOrRspPutBlocksPtr rspOrError)
        {
            if (session != CurrentChunkSession_) {
                return;
            }

            if (SimulateReplicaFailure(node, session)) {
                return;
            }

            if (!rspOrError.IsOK()) {
                OnReplicaFailure(rspOrError, node, session);
                return;
            }

            auto response = rspOrError.Value();

            Counters_.JournalWrittenBytes.Increment(flushDataSize);
            Counters_.MediumWrittenBytes.Increment(response->statistics().data_bytes_written_to_medium());
            Counters_.IORequestCount.Increment(response->statistics().io_requests());

            if (const auto& writeObserver = Counters_.JournalWritesObserver; writeObserver) {
                writeObserver->RegisterJournalWrite(flushDataSize, response->statistics().data_bytes_written_to_medium());
            }

            YT_LOG_DEBUG("Journal replica written (Address: %v, BlockIds: %v:%v-%v, Rows: %v-%v, "
                "MediumWrittenBytes: %v, JournalWrittenBytes: %v)",
                node->Descriptor.GetDefaultAddress(),
                session->Id,
                node->FirstPendingBlockIndex,
                node->FirstPendingBlockIndex + flushRowCount - 1,
                node->FirstPendingRowIndex,
                node->FirstPendingRowIndex + flushRowCount - 1,
                response->statistics().data_bytes_written_to_medium(),
                flushDataSize);

            for (const auto& batch : node->InFlightBatches) {
                ++batch->FlushedReplicas;
            }

            node->FirstPendingBlockIndex += flushRowCount;
            node->FirstPendingRowIndex += flushRowCount;
            node->InFlightBatches.clear();

            std::vector<TPromise<void>> fulfilledPromises;
            while (!QuorumUnflushedBatches_.empty()) {
                auto front = QuorumUnflushedBatches_.front();
                if (front->FlushedReplicas < WriteQuorum_) {
                    break;
                }

                fulfilledPromises.push_back(front->FlushedPromise);
                session->QuorumFlushedRowCount += front->RowCount;
                session->QuorumFlushedDataSize += front->DataSize;
                QuorumUnflushedBatches_.pop_front();
                YT_VERIFY(--QuorumUnflushedBatchCount_ >= 0);

                YT_LOG_DEBUG("Rows are written by quorum (Rows: %v-%v)",
                    front->FirstRowIndex,
                    front->FirstRowIndex + front->RowCount - 1);

                QueueTrace_.Commit(front->FirstRowIndex + front->RowCount - 1);
            }

            if (IsCompleted()) {
                EnqueueCommand(TCompleteCommand());
            }

            for (const auto& promise : fulfilledPromises) {
                promise.Set();
            }

            bool flushAllReplicas = false;
            while (!ReplicationFactorUnflushedBatches_.empty()) {
                auto batch = ReplicationFactorUnflushedBatches_.front();
                if (batch->FlushedReplicas < ReplicaCount_) {
                    break;
                }

                session->ReplicationFactorFlushedRowCount += batch->RowCount;
                ReplicationFactorUnflushedBatches_.pop_front();
                flushAllReplicas = true;

                YT_LOG_DEBUG("Rows are written to all replicas (Rows: %v-%v)",
                    batch->FirstRowIndex,
                    batch->FirstRowIndex + batch->RowCount - 1);
            }

            if (!session->SwitchScheduled && session->QuorumFlushedRowCount >= Config_->MaxChunkRowCount) {
                YT_LOG_DEBUG("Chunk row count limit exceeded; requesting chunk switch (RowCount: %v, SessionId: %v)",
                    session->QuorumFlushedRowCount,
                    session->Id);
                ScheduleChunkSessionSwitch(session);
            }

            if (!session->SwitchScheduled && session->QuorumFlushedDataSize >= Config_->MaxChunkDataSize) {
                YT_LOG_DEBUG("Chunk data size limit exceeded; requesting chunk switch (DataSize: %v, SessionId: %v)",
                    session->QuorumFlushedDataSize,
                    session->Id);
                ScheduleChunkSessionSwitch(session);
            }

            if (flushAllReplicas) {
                for (const auto& node : session->Nodes) {
                    MaybeFlushBlocks(CurrentChunkSession_, node);
                }
            } else {
                MaybeFlushBlocks(CurrentChunkSession_, node);
            }
        }

        bool SimulateReplicaFailure(
            const TNodePtr& node,
            const TChunkSessionPtr& session)
        {
            if (Config_->ReplicaFailureProbability == 0.0 ||
                RandomNumber<double>() >= Config_->ReplicaFailureProbability)
            {
                return false;
            }
            const auto& address = node->Descriptor.GetDefaultAddress();
            YT_LOG_WARNING("Simulated journal replica failure; requesting switch (Address: %v, SessionId: %v)",
                address,
                session->Id);
            ScheduleChunkSessionSwitch(session);
            return true;
        }

        bool SimulateReplicaTimeout(
            const TNodePtr& node,
            const TChunkSessionPtr& session,
            i64 flushRowCount)
        {
            if (!Config_->ReplicaRowLimits) {
                return false;
            }

            i64 replicaRowLimit = (*Config_->ReplicaRowLimits)[node->Index];
            if (node->FirstPendingBlockIndex + flushRowCount <= replicaRowLimit) {
                return false;
            }

            YT_LOG_WARNING("Simulating journal replica timeout (ReplicaIndex: %v, FlushRowCount: %v, ReplicaRowLimit: %v)",
                node->Index,
                flushRowCount,
                replicaRowLimit);

            TDelayedExecutor::Submit(
                BIND(&TImpl::OnReplicaFailure, MakeWeak(this), TError(NYT::EErrorCode::Timeout, "Fake timeout"), node, session)
                    .Via(Invoker_),
                Config_->ReplicaFakeTimeoutDelay);
            return true;
        }

        void OnReplicaFailure(
            const TError& error,
            const TNodePtr& node,
            const TChunkSessionPtr& session)
        {
            const auto& address = node->Descriptor.GetDefaultAddress();
            YT_LOG_WARNING(error, "Journal replica failure; requesting switch (Address: %v, SessionId: %v)",
                address,
                session->Id);
            ScheduleChunkSessionSwitch(session);
            BanNode(address);
        }

        static bool IsChunkSessionAlive(const TChunkSessionPtr& session)
        {
            return
                session &&
                (session->State == EChunkSessionState::Allocated || session->State == EChunkSessionState::Current) &&
                !session->SwitchScheduled;
        }

        bool IsSafeToSwitchSessionOnDemand()
        {
            if (IsChunkPreallocationEnabled()) {
                return true;
            }

            if (!IsChunkSessionAlive(CurrentChunkSession_)) {
                return false;
            }

            if (!AllocatedChunkSessionPromise_) {
                return false;
            }

            if (!AllocatedChunkSessionPromise_.IsSet()) {
                return false;
            }

            const auto& preallocatedSessionOrError = AllocatedChunkSessionPromise_.Get();
            if (!preallocatedSessionOrError.IsOK()) {
                return false;
            }

            const auto& preallocatedSession = preallocatedSessionOrError.Value();
            if (!IsChunkSessionAlive(preallocatedSession)) {
                return false;
            }

            return true;
        }

        void OnReplicaCloseDemanded(
            const TNodePtr& node,
            const TChunkSessionPtr& session)
        {
            const auto& address = node->Descriptor.GetDefaultAddress();
            BanNode(address);
            if (IsSafeToSwitchSessionOnDemand()) {
                YT_LOG_DEBUG("Journal replica has demanded to close the session; requesting switch (Address: %v, SessionId: %v)",
                    address,
                    session->Id);
                ScheduleChunkSessionSwitch(session);
            } else {
                YT_LOG_DEBUG("Journal replica has demanded to close the session but switching is not safe at the moment; ignoring (Address: %v, SessionId: %v)",
                    address,
                    session->Id);
            }
        }


        void ScheduleChunkSessionSwitch(const TChunkSessionPtr& session)
        {
            if (!IsChunkPreallocationEnabled() && session->State != EChunkSessionState::Current) {
                YT_LOG_DEBUG("Non-current chunk session cannot be switched (SessionId: %v)",
                    session->Id);
                return;
            }

            if (session->SwitchScheduled) {
                YT_LOG_DEBUG("Chunk session is already switched (SessionId: %v)",
                    session->Id);
                return;
            }
            session->SwitchScheduled = true;

            YT_LOG_DEBUG("Scheduling chunk session switch (SessionId: %v, SessionState: %v)",
                session->Id,
                session->State);

            switch (session->State) {
                case EChunkSessionState::Current:
                    EnqueueCommand(TSwitchChunkCommand{session});
                    break;

                case EChunkSessionState::Allocating:
                case EChunkSessionState::Allocated:
                    session->State = EChunkSessionState::Discarded;
                    if (AllocatedChunkSessionIndex_ == session->Index) {
                        YT_LOG_DEBUG("Resetting chunk session promise");
                        AllocatedChunkSessionIndex_ = -1;
                        AllocatedChunkSessionPromise_.Reset();
                        if (IsChunkPreallocationEnabled()) {
                            ScheduleChunkSessionAllocation();
                        }
                    }
                    break;

                case EChunkSessionState::Discarded:
                    break;

                default:
                    YT_ABORT();
            }
        }


        void UpdateReplicaLag(const TChunkSessionPtr& session, const TNodePtr& node, TCpuDuration lagTime)
        {
            node->LagTime = lagTime;

            std::vector<std::pair<NProfiling::TCpuDuration, int>> replicas;
            for (int index = 0; index < std::ssize(session->Nodes); ++index) {
                replicas.emplace_back(session->Nodes[index]->LagTime, index);
            }

            std::sort(replicas.begin(), replicas.end());

            Counters_.WriteQuorumLag.Record(CpuDurationToDuration(replicas[WriteQuorum_ - 1].first));
            Counters_.MaxReplicaLag.Record(CpuDurationToDuration(replicas.back().first));

            YT_LOG_DEBUG("Journal replicas lag updated (Replicas: %v)",
                MakeFormattableView(replicas, [&] (auto* builder, const auto& replica) {
                    builder->AppendFormat("%v=>%v",
                        session->Nodes[replica.second]->Descriptor.GetDefaultAddress(),
                        CpuDurationToDuration(replica.first));
                }));
        }

        TSessionId GetSessionIdForNode(const TChunkSessionPtr& session, const TNodePtr& node)
        {
            auto chunkId = ErasureCodec_ == NErasure::ECodec::None
                ? session->Id.ChunkId
                : EncodeChunkId(TChunkIdWithIndex(session->Id.ChunkId, node->Index));
            return TSessionId(chunkId, session->Id.MediumIndex);
        }

        void ScheduleChunkSessionSeal(const TChunkSessionPtr& session)
        {
            if (Config_->DontSeal) {
                YT_LOG_WARNING("Client-side chunk seal is disabled, skipping chunk session seal (SessionId: %v)",
                    session->Id);
                return;
            }

            if (std::exchange(session->SealScheduled, true)) {
                YT_LOG_DEBUG("Chunk seal is already scheduled (SessionId: %v)",
                    session->Id);
                return;
            }

            EmplaceOrCrash(IndexToChunkSessionToSeal_, session->Index, session);

            YT_LOG_DEBUG("Chunk seal scheduled (SessionId: %v, SessionIndex: %v, FirstRowIndex: %v, RowCount: %v, DataSize: %v)",
                session->Id,
                session->Index,
                session->FirstRowIndex,
                session->QuorumFlushedRowCount,
                session->QuorumFlushedDataSize);

            MaybeSealChunks();
        }

        void MaybeSealChunks()
        {
            if (SealInProgress_) {
                return;
            }

            if (IndexToChunkSessionToSeal_.empty()) {
                return;
            }

            if (IndexToChunkSessionToSeal_.begin()->first != FirstUnsealedSessionIndex_) {
                return;
            }

            auto batchReq = CreateExecuteBatchRequest();
            std::vector<TSessionId> sessionIds;
            while (!IndexToChunkSessionToSeal_.empty() && IndexToChunkSessionToSeal_.begin()->first == FirstUnsealedSessionIndex_) {
                ++FirstUnsealedSessionIndex_;
                auto session = std::move(IndexToChunkSessionToSeal_.begin()->second);
                IndexToChunkSessionToSeal_.erase(IndexToChunkSessionToSeal_.begin());
                sessionIds.push_back(session->Id);

                auto* req = batchReq->add_seal_chunk_subrequests();
                ToProto(req->mutable_chunk_id(), session->Id.ChunkId);
                if (session->FirstRowIndex) {
                    req->mutable_info()->set_first_overlayed_row_index(*session->FirstRowIndex);
                }
                req->mutable_info()->set_row_count(session->QuorumFlushedRowCount);
                req->mutable_info()->set_uncompressed_data_size(session->QuorumFlushedDataSize);
                req->mutable_info()->set_compressed_data_size(session->QuorumFlushedDataSize);
            }

            SealInProgress_ = true;

            YT_LOG_DEBUG("Sealing chunks (SessionIds: %v)",
                sessionIds);

            batchReq->Invoke().Subscribe(
                BIND(&TImpl::OnChunksSealed, MakeStrong(this))
                    .Via(Invoker_));
        }

        void OnChunksSealed(const TChunkServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
        {
            auto error = GetCumulativeError(batchRspOrError);
            if (!error.IsOK()) {
                auto wrappedError = TError("Error sealing chunks")
                    << error;
                EnqueueCommand(TFailCommand{error});
                // SealInProgress_ is left stuck.
                return;
            }

            YT_LOG_DEBUG("Chunks sealed successfully");

            SealInProgress_ = false;
            MaybeSealChunks();
        }

        bool IsCompleted() const
        {
            return Closing_ && QuorumUnflushedBatchCount_ == 0;
        }

        // COMPAT(babenko): make this always true
        bool AreOverlayedChunksEnabled() const
        {
            return Options_.EnableChunkPreallocation;
        }

        bool IsChunkPreallocationEnabled() const
        {
            return Options_.EnableChunkPreallocation && !Config_->DontPreallocate;
        }

        TChunkServiceProxy::TReqExecuteBatchPtr CreateExecuteBatchRequest()
        {
            TChunkServiceProxy proxy(UploadMasterChannel_);

            auto batchReq = proxy.ExecuteBatch();
            GenerateMutationId(batchReq);
            SetSuppressUpstreamSync(&batchReq->Header(), true);
            // COMPAT(shakurov): prefer proto ext (above).
            batchReq->set_suppress_upstream_sync(true);

            return batchReq;
        }
    };

    const TIntrusivePtr<TImpl> Impl_;
};

IJournalWriterPtr CreateJournalWriter(
    IClientPtr client,
    TYPath path,
    TJournalWriterOptions options)
{
    return New<TJournalWriter>(
        std::move(client),
        std::move(path),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
