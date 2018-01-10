#include "journal_writer.h"
#include "private.h"
#include "config.h"
#include "transaction.h"
#include "native_connection.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/session_id.h>
#include <yt/ytlib/chunk_client/medium_directory.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/journal_client/journal_ypath_proxy.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>
#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/ytlib/object_client/helpers.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/transaction_client/transaction_listener.h>
#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/ytlib/transaction_client/config.h>

#include <yt/ytlib/api/transaction.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/nonblocking_queue.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/variant.h>

#include <yt/core/rpc/helpers.h>
#include <yt/core/rpc/retrying_channel.h>

#include <yt/core/ytree/helpers.h>

#include <deque>
#include <queue>

namespace NYT {
namespace NApi {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NRpc;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTransactionClient;
using namespace NJournalClient;
using namespace NNodeTrackerClient;
using namespace NTransactionClient;

using NYT::TRange;

using NChunkClient::TSessionId; // Suppress ambiguity with NProto::TSessionId.

////////////////////////////////////////////////////////////////////////////////

class TJournalWriter
    : public IJournalWriter
{
public:
    TJournalWriter(
        INativeClientPtr client,
        const TYPath& path,
        const TJournalWriterOptions& options)
        : Impl_(New<TImpl>(client, path, options))
    { }

    ~TJournalWriter()
    {
        Impl_->Cancel();
    }

    virtual TFuture<void> Open() override
    {
        return Impl_->Open();
    }

    virtual TFuture<void> Write(const TRange<TSharedRef>& rows) override
    {
        return Impl_->Write(rows);
    }

    virtual TFuture<void> Close() override
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
            INativeClientPtr client,
            const TYPath& path,
            const TJournalWriterOptions& options)
            : Client_(client)
            , Path_(path)
            , Options_(options)
            , Config_(options.Config ? options.Config : New<TJournalWriterConfig>())
        {
            if (Options_.TransactionId) {
                Transaction_ = Client_->AttachTransaction(Options_.TransactionId);
            }

            Logger.AddTag("Path: %v, TransactionId: %v",
                Path_,
                Options_.TransactionId);

            // Spawn the actor.
            BIND(&TImpl::ActorMain, MakeStrong(this))
                .AsyncVia(Invoker_)
                .Run();

            if (Transaction_) {
                ListenTransaction(Transaction_);
            }
        }

        TFuture<void> Open()
        {
            return OpenedPromise_;
        }

        TFuture<void> Write(const TRange<TSharedRef>& rows)
        {
            TGuard<TSpinLock> guard(CurrentBatchSpinLock_);

            if (!Error_.IsOK()) {
                return MakeFuture(Error_);
            }

            TFuture<void> result = VoidFuture;
            for (const auto& row : rows) {
                YCHECK(!row.Empty());
                auto batch = EnsureCurrentBatch();
                // NB: We can form a handful of batches but since flushes are monotonic,
                // the last one will do.
                result = AppendToBatch(batch, row);
                if (IsBatchFull(batch)) {
                    FlushCurrentBatch();
                }
            }

            return result;
        }

        TFuture<void> Close()
        {
            EnqueueCommand(TCloseCommand());
            return ClosedPromise_;
        }

        void Cancel()
        {
            EnqueueCommand(TCancelCommand());
        }

    private:
        const INativeClientPtr Client_;
        const TYPath Path_;
        const TJournalWriterOptions Options_;
        const TJournalWriterConfigPtr Config_;

        const IInvokerPtr Invoker_ = NChunkClient::TDispatcher::Get()->GetWriterInvoker();

        NLogging::TLogger Logger = ApiLogger;

        struct TBatch
            : public TIntrinsicRefCounted
        {
            i64 FirstRowIndex = -1;
            i64 DataSize = 0;
            std::vector<TSharedRef> Rows;
            TPromise<void> FlushedPromise = NewPromise<void>();
            int FlushedReplicas = 0;
        };

        typedef TIntrusivePtr<TBatch> TBatchPtr;

        TSpinLock CurrentBatchSpinLock_;
        TError Error_;
        TBatchPtr CurrentBatch_;
        TDelayedExecutorCookie CurrentBatchFlushCookie_;

        TPromise<void> OpenedPromise_ = NewPromise<void>();

        bool Closing_ = false;
        TPromise<void> ClosedPromise_ = NewPromise<void>();

        ITransactionPtr Transaction_;
        ITransactionPtr UploadTransaction_;

        int ReplicationFactor_ = -1;
        int ReadQuorum_ = -1;
        int WriteQuorum_ = -1;
        TString Account_;
        TString PrimaryMedium_;

        TObjectId ObjectId_;
        TChunkListId ChunkListId_;
        IChannelPtr UploadMasterChannel_;

        struct TNode
            : public TRefCounted
        {
            const TNodeDescriptor Descriptor;

            TDataNodeServiceProxy LightProxy;
            TDataNodeServiceProxy HeavyProxy;
            TPeriodicExecutorPtr PingExecutor;

            bool Started = false;

            i64 FirstPendingBlockIndex = 0;
            i64 FirstPendingRowIndex = 0;

            std::queue<TBatchPtr> PendingBatches;
            std::vector<TBatchPtr> InFlightBatches;

            TNode(
                const TNodeDescriptor& descriptor,
                i64 firstPendingRowIndex,
                IChannelPtr lightChannel,
                IChannelPtr heavyChannel,
                TDuration rpcTimeout)
                : Descriptor(descriptor)
                , LightProxy(std::move(lightChannel))
                , HeavyProxy(std::move(heavyChannel))
                , FirstPendingRowIndex(firstPendingRowIndex)
            {
                LightProxy.SetDefaultTimeout(rpcTimeout);
                HeavyProxy.SetDefaultTimeout(rpcTimeout);
            }
        };

        typedef TIntrusivePtr<TNode> TNodePtr;
        typedef TWeakPtr<TNode> TNodeWeakPtr;

        TNodeDirectoryPtr NodeDirectory_ = New<TNodeDirectory>();

        struct TChunkSession
            : public TRefCounted
        {
            TSessionId Id;
            std::vector<TNodePtr> Nodes;
            i64 RowCount = 0;
            i64 DataSize = 0;
            i64 FlushedRowCount = 0;
            i64 FlushedDataSize = 0;
        };

        typedef TIntrusivePtr<TChunkSession> TChunkSessionPtr;
        typedef TWeakPtr<TChunkSession> TChunkSessionWeakPtr;

        i64 SealedRowCount_ = 0;
        TChunkSessionPtr CurrentSession_;

        i64 CurrentRowIndex_ = 0;
        std::deque<TBatchPtr> PendingBatches_;

        typedef TBatchPtr TBatchCommand;

        struct TCloseCommand { };

        struct TCancelCommand { };

        struct TSwitchChunkCommand
        {
            TChunkSessionPtr Session;
        };

        typedef TVariant<
            TBatchCommand,
            TCloseCommand,
            TCancelCommand,
            TSwitchChunkCommand
        > TCommand;

        TNonblockingQueue<TCommand> CommandQueue_;

        THashMap<TString, TInstant> BannedNodeToDeadline_;


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
            if (BannedNodeToDeadline_.find(address) == BannedNodeToDeadline_.end()) {
                BannedNodeToDeadline_.insert(std::make_pair(address, TInstant::Now() + Config_->NodeBanTimeout));
                LOG_INFO("Node banned (Address: %v)", address);
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
                    LOG_INFO("Node unbanned (Address: %v)", jt->first);
                    BannedNodeToDeadline_.erase(jt);
                } else {
                    result.push_back(jt->first);
                }
            }
            return result;
        }


        void OpenJournal()
        {

            TUserObject userObject;
            userObject.Path = Path_;

            GetUserObjectBasicAttributes(
                Client_,
                TMutableRange<TUserObject>(&userObject, 1),
                Transaction_ ? Transaction_->GetId() : NullTransactionId,
                Logger,
                EPermission::Write);

            const auto cellTag = userObject.CellTag;
            ObjectId_ = userObject.ObjectId;

            auto objectIdPath = FromObjectId(ObjectId_);

            if (userObject.Type != EObjectType::Journal) {
                THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                    Path_,
                    EObjectType::Journal,
                    userObject.Type);
            }

            UploadMasterChannel_ = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);

            {
                LOG_INFO("Requesting extended journal attributes");

                auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
                TObjectServiceProxy proxy(channel);

                auto req = TCypressYPathProxy::Get(objectIdPath + "/@");
                SetTransactionId(req, Transaction_);
                std::vector<TString> attributeKeys{
                    "type",
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
                ReplicationFactor_ = attributes->Get<int>("replication_factor");
                ReadQuorum_ = attributes->Get<int>("read_quorum");
                WriteQuorum_ = attributes->Get<int>("write_quorum");
                Account_ = attributes->Get<TString>("account");
                PrimaryMedium_ = attributes->Get<TString>("primary_medium");

                LOG_INFO("Extended journal attributes received (ReplicationFactor: %v, WriteQuorum: %v, Account: %v, "
                    "PrimaryMedium: %v)",
                    ReplicationFactor_,
                    WriteQuorum_,
                    Account_,
                    PrimaryMedium_);
            }

            {
                LOG_INFO("Starting journal upload");

                auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
                TObjectServiceProxy proxy(channel);

                auto batchReq = proxy.ExecuteBatch();

                {
                    auto* prerequisitesExt = batchReq->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
                    for (const auto& id : Options_.PrerequisiteTransactionIds) {
                        auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
                        ToProto(prerequisiteTransaction->mutable_transaction_id(), id);
                    }
                }

                {
                    auto req = TJournalYPathProxy::BeginUpload(objectIdPath);
                    req->set_update_mode(static_cast<int>(EUpdateMode::Append));
                    req->set_lock_mode(static_cast<int>(ELockMode::Exclusive));
                    req->set_upload_transaction_title(Format("Upload to %v", Path_));
                    req->set_upload_transaction_timeout(ToProto<i64>(Config_->UploadTransactionTimeout));
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
                    ListenTransaction(UploadTransaction_);

                    LOG_INFO("Journal upload started (UploadTransactionId: %v)",
                        uploadTransactionId);
                }
            }

            {
                LOG_INFO("Requesting journal upload parameters");

                auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Follower, cellTag);
                TObjectServiceProxy proxy(channel);

                auto req = TJournalYPathProxy::GetUploadParams(objectIdPath);
                SetTransactionId(req, UploadTransaction_);

                auto rspOrError = WaitFor(proxy.Execute(req));
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    rspOrError,
                    "Error requesting upload parameters for journal %v",
                    Path_);

                const auto& rsp = rspOrError.Value();
                ChunkListId_ = FromProto<TChunkListId>(rsp->chunk_list_id());

                LOG_INFO("Journal upload parameters received (ChunkListId: %v)",
                    ChunkListId_);
            }

            LOG_INFO("Journal opened");
            OpenedPromise_.Set(TError());
        }

        void CloseJournal()
        {
            LOG_INFO("Closing journal");

            auto objectIdPath = FromObjectId(ObjectId_);

            auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
            TObjectServiceProxy proxy(channel);

            auto batchReq = proxy.ExecuteBatch();

            {
                auto* prerequisitesExt = batchReq->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
                for (const auto& id : Options_.PrerequisiteTransactionIds) {
                    auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
                    ToProto(prerequisiteTransaction->mutable_transaction_id(), id);
                }
            }

            UploadTransaction_->Ping();
            UploadTransaction_->Detach();

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

            LOG_INFO("Journal closed");
            ClosedPromise_.TrySet(TError());
        }

        bool TryOpenChunk()
        {
            auto session = New<TChunkSession>();

            LOG_INFO("Creating chunk");

            {
                TChunkServiceProxy proxy(UploadMasterChannel_);

                auto batchReq = proxy.ExecuteBatch();
                GenerateMutationId(batchReq);
                batchReq->set_suppress_upstream_sync(true);

                auto* req = batchReq->add_create_chunk_subrequests();
                req->set_type(static_cast<int>(EObjectType::JournalChunk));
                req->set_account(Account_);
                ToProto(req->mutable_transaction_id(), UploadTransaction_->GetId());
                req->set_replication_factor(ReplicationFactor_);
                req->set_medium_name(PrimaryMedium_);
                req->set_read_quorum(ReadQuorum_);
                req->set_write_quorum(WriteQuorum_);
                req->set_movable(true);
                req->set_vital(true);
                req->set_erasure_codec(static_cast<int>(NErasure::ECodec::None));

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    GetCumulativeError(batchRspOrError),
                    "Error creating chunk");

                const auto& batchRsp = batchRspOrError.Value();
                const auto& rsp = batchRsp->create_chunk_subresponses(0);

                session->Id = FromProto<TSessionId>(rsp.session_id());
            }

            LOG_INFO("Chunk created (ChunkId: %v)",
                session->Id);

            auto replicas = AllocateWriteTargets(
                Client_,
                session->Id,
                ReplicationFactor_,
                WriteQuorum_,
                Null,
                Config_->PreferLocalHost,
                GetBannedNodes(),
                NodeDirectory_,
                Logger);

            std::vector<TNodeDescriptor> targets;
            for (auto replica : replicas) {
                const auto& descriptor = NodeDirectory_->GetDescriptor(replica);
                targets.push_back(descriptor);
            }

            const auto& networks = Client_->GetNativeConnection()->GetNetworks();
            for (const auto& target : targets) {
                auto address = target.GetAddress(networks);
                auto lightChannel = Client_->GetChannelFactory()->CreateChannel(address);
                auto heavyChannel = CreateRetryingChannel(
                    Config_->NodeChannel,
                    lightChannel,
                    BIND([] (const TError& error) {
                        return error.FindMatching(NChunkClient::EErrorCode::WriteThrottlingActive).HasValue();
                    }));
                auto node = New<TNode>(
                    target,
                    SealedRowCount_,
                    std::move(lightChannel),
                    std::move(heavyChannel),
                    Config_->NodeRpcTimeout);
                session->Nodes.push_back(node);
            }

            LOG_INFO("Starting chunk sessions");
            try {
                std::vector<TFuture<void>> asyncResults;
                for (const auto& node : session->Nodes) {
                    auto req = node->LightProxy.StartChunk();
                    ToProto(req->mutable_session_id(), session->Id);
                    ToProto(req->mutable_workload_descriptor(), Config_->WorkloadDescriptor);
                    req->set_enable_multiplexing(Options_.EnableMultiplexing);
                    auto asyncRsp = req->Invoke().Apply(
                        BIND(&TImpl::OnChunkStarted, MakeStrong(this), session, node)
                            .AsyncVia(Invoker_));
                    asyncResults.push_back(asyncRsp);
                }
                auto result = WaitFor(CombineQuorum(asyncResults, WriteQuorum_));
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error starting chunk sessions");
            } catch (const std::exception& ex) {
                LOG_WARNING(TError(ex));
                return false;
            }
            LOG_INFO("Chunk sessions started");

            for (const auto& node : session->Nodes) {
                node->PingExecutor = New<TPeriodicExecutor>(
                    Invoker_,
                    BIND(&TImpl::SendPing, MakeWeak(this), MakeWeak(session), MakeWeak(node)),
                    Config_->NodePingPeriod);
                node->PingExecutor->Start();
            }

            const auto& chunkId = session->Id.ChunkId;

            LOG_INFO("Confirming chunk");
            {
                TChunkServiceProxy proxy(UploadMasterChannel_);

                auto batchReq = proxy.ExecuteBatch();
                GenerateMutationId(batchReq);
                batchReq->set_suppress_upstream_sync(true);

                YCHECK(!replicas.empty());
                auto* req = batchReq->add_confirm_chunk_subrequests();
                ToProto(req->mutable_chunk_id(), chunkId);
                req->mutable_chunk_info();
                ToProto(req->mutable_replicas(), replicas);
                auto* meta = req->mutable_chunk_meta();
                meta->set_type(static_cast<int>(EChunkType::Journal));
                meta->set_version(0);
                TMiscExt miscExt;
                SetProtoExtension(meta->mutable_extensions(), miscExt);

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    GetCumulativeError(batchRspOrError),
                    "Error confirming chunk %v",
                    chunkId);
            }
            LOG_INFO("Chunk confirmed");

            LOG_INFO("Attaching chunk");
            {
                TChunkServiceProxy proxy(UploadMasterChannel_);
                auto batchReq = proxy.ExecuteBatch();
                GenerateMutationId(batchReq);
                batchReq->set_suppress_upstream_sync(true);

                auto* req = batchReq->add_attach_chunk_trees_subrequests();
                ToProto(req->mutable_parent_id(), ChunkListId_);
                ToProto(req->add_child_ids(), chunkId);

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    GetCumulativeError(batchRspOrError),
                    "Error attaching chunk %v",
                    chunkId);
            }
            LOG_INFO("Chunk attached");

            CurrentSession_ = session;

            for (const auto& batch : PendingBatches_) {
                EnqueueBatchToSession(batch);
            }

            TDelayedExecutor::Submit(
                BIND(&TImpl::OnSessionTimeout, MakeWeak(this), MakeWeak(session)),
                Config_->MaxChunkSessionDuration);

            return true;
        }

        void OnSessionTimeout(const TWeakPtr<TChunkSession>& session_)
        {
            auto session = session_.Lock();
            if (session) {
                EnqueueCommand(TSwitchChunkCommand{session});
            }
        }

        void OpenChunk()
        {
            for (int attempt = 0; attempt < Config_->MaxChunkOpenAttempts; ++attempt) {
                if (TryOpenChunk())
                    return;
            }
            THROW_ERROR_EXCEPTION("All %v attempts to open a chunk were unsuccessful",
                Config_->MaxChunkOpenAttempts);
        }

        void WriteChunk()
        {
            while (true) {
                ValidateAborted();
                auto command = DequeueCommand();
                if (command.Is<TCloseCommand>()) {
                    HandleClose();
                    break;
                } else if (command.Is<TCancelCommand>()) {
                    throw TFiberCanceledException();
                } else if (auto* typedCommand = command.TryAs<TBatchCommand>()) {
                    HandleBatch(*typedCommand);
                    if (IsSessionOverfull()) {
                        SwitchChunk();
                        break;
                    }
                } else if (auto* typedCommand = command.TryAs<TSwitchChunkCommand>()) {
                    if (typedCommand->Session == CurrentSession_) {
                        SwitchChunk();
                        break;
                    }
                }
            }
        }

        void HandleClose()
        {
            LOG_INFO("Closing journal writer");
            Closing_ = true;
        }

        void HandleBatch(TBatchPtr batch)
        {
            PendingBatches_.push_back(batch);
            EnqueueBatchToSession(batch);
        }

        bool IsSessionOverfull()
        {
            return
                CurrentSession_->RowCount > Config_->MaxChunkRowCount ||
                CurrentSession_->DataSize > Config_->MaxChunkDataSize;
        }

        void EnqueueBatchToSession(const TBatchPtr& batch)
        {
            // Reset flushed replica count: this batch might have already been
            // flushed (partially) by the previous (failed session).
            if (batch->FlushedReplicas > 0) {
                LOG_DEBUG("Resetting flushed replica counter (Rows: %v-%v, FlushCounter: %v)",
                    batch->FirstRowIndex,
                    batch->FirstRowIndex + batch->Rows.size() - 1,
                    batch->FlushedReplicas);
                batch->FlushedReplicas = 0;
            }

            CurrentSession_->RowCount += batch->Rows.size();
            CurrentSession_->DataSize += batch->DataSize;

            LOG_DEBUG("Batch enqueued (Rows: %v-%v)",
                batch->FirstRowIndex,
                batch->FirstRowIndex + batch->Rows.size() - 1);

            for (const auto& node : CurrentSession_->Nodes) {
                node->PendingBatches.push(batch);
                MaybeFlushBlocks(node);
            }
        }

        void SwitchChunk()
        {
            LOG_INFO("Switching chunk");
        }

        void CloseChunk()
        {
            // Release the current session to prevent writing more rows
            // or detecting failed pings.
            auto session = CurrentSession_;
            CurrentSession_.Reset();

            const auto& sessionId = session->Id;

            LOG_INFO("Finishing chunk sessions");
            for (const auto& node : session->Nodes) {
                auto req = node->LightProxy.FinishChunk();
                ToProto(req->mutable_session_id(), sessionId);
                req->Invoke().Subscribe(
                    BIND(&TImpl::OnChunkFinished, MakeStrong(this), node)
                        .Via(Invoker_));
                if (node->PingExecutor) {
                    node->PingExecutor->Stop();
                    node->PingExecutor.Reset();
                }
            }

            {
                LOG_INFO("Sealing chunk (ChunkId: %v, RowCount: %v)",
                    sessionId,
                    session->FlushedRowCount);

                TChunkServiceProxy proxy(UploadMasterChannel_);

                auto batchReq = proxy.ExecuteBatch();
                GenerateMutationId(batchReq);
                batchReq->set_suppress_upstream_sync(true);

                auto* req = batchReq->add_seal_chunk_subrequests();
                ToProto(req->mutable_chunk_id(), sessionId.ChunkId);
                auto* miscExt = req->mutable_misc();
                miscExt->set_sealed(true);
                miscExt->set_row_count(session->FlushedRowCount);
                miscExt->set_uncompressed_data_size(session->FlushedDataSize);
                miscExt->set_compressed_data_size(session->FlushedDataSize);

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    GetCumulativeError(batchRspOrError),
                    "Error sealing chunk %v",
                    sessionId);

                LOG_INFO("Chunk sealed");

                SealedRowCount_ += session->FlushedRowCount;
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
                    LOG_ERROR(ex, "Error pumping journal writer command queue");
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
            } while (!Closing_ || !PendingBatches_.empty());
            CloseJournal();
        }

        void PumpFailed(const TError& error)
        {
            LOG_WARNING(error, "Journal writer failed");

            {
                TGuard<TSpinLock> guard(CurrentBatchSpinLock_);
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

            for (const auto& batch : PendingBatches_) {
                batch->FlushedPromise.Set(error);
            }
            PendingBatches_.clear();

            while (true) {
                auto command = DequeueCommand();
                if (auto* typedCommand = command.TryAs<TBatchCommand>()) {
                    (*typedCommand)->FlushedPromise.Set(error);
                } else if (command.Is<TCancelCommand>()) {
                    throw TFiberCanceledException();
                } else {
                    // Ignore.
                }
            }
        }


        TFuture<void> AppendToBatch(const TBatchPtr& batch, const TSharedRef& row)
        {
            Y_ASSERT(row);
            batch->Rows.push_back(row);
            batch->DataSize += row.Size();
            ++CurrentRowIndex_;
            return batch->FlushedPromise;
        }

        bool IsBatchFull(const TBatchPtr& batch)
        {
            return
                batch->DataSize > Config_->MaxBatchDataSize ||
                batch->Rows.size() > Config_->MaxBatchRowCount;
        }


        TBatchPtr EnsureCurrentBatch()
        {
            VERIFY_SPINLOCK_AFFINITY(CurrentBatchSpinLock_);

            if (!CurrentBatch_) {
                CurrentBatch_ = New<TBatch>();
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
            TGuard<TSpinLock> guard(CurrentBatchSpinLock_);
            if (CurrentBatch_ == batch) {
                FlushCurrentBatch();
            }
        }

        void FlushCurrentBatch()
        {
            VERIFY_SPINLOCK_AFFINITY(CurrentBatchSpinLock_);

            TDelayedExecutor::CancelAndClear(CurrentBatchFlushCookie_);

            LOG_DEBUG("Flushing batch (Rows: %v-%v)",
                CurrentBatch_->FirstRowIndex,
                CurrentBatch_->FirstRowIndex + CurrentBatch_->Rows.size() - 1);

            EnqueueCommand(TBatchCommand(CurrentBatch_));
            CurrentBatch_.Reset();
        }


        void SendPing(
            const TChunkSessionWeakPtr& session_,
            const TNodeWeakPtr& node_)
        {
            auto session = session_.Lock();
            if (!session)
                return;

            auto node = node_.Lock();
            if (!node)
                return;

            LOG_DEBUG("Sending ping (Address: %v, ChunkId: %v)",
                node->Descriptor.GetDefaultAddress(),
                session->Id);

            auto req = node->LightProxy.PingSession();
            ToProto(req->mutable_session_id(), session->Id);
            req->Invoke().Subscribe(
                BIND(&TImpl::OnPingSent, MakeWeak(this), session, node)
                    .Via(Invoker_));
        }

        void OnPingSent(
            const TChunkSessionPtr& session,
            const TNodePtr& node,
            const TDataNodeServiceProxy::TErrorOrRspPingSessionPtr& rspOrError)
        {
            if (session != CurrentSession_) {
                return;
            }

            if (!rspOrError.IsOK()) {
                OnReplicaFailed(rspOrError, node, session);
                return;
            }

            LOG_DEBUG("Ping succeeded (Address: %v, ChunkId: %v)",
                node->Descriptor.GetDefaultAddress(),
                session->Id);
        }


        void OnChunkStarted(
            const TChunkSessionPtr& session,
            const TNodePtr& node,
            const TDataNodeServiceProxy::TErrorOrRspStartChunkPtr& rspOrError)
        {
            if (rspOrError.IsOK()) {
                LOG_DEBUG("Chunk session started (Address: %v)",
                    node->Descriptor.GetDefaultAddress());
                node->Started = true;
                if (CurrentSession_ == session) {
                    MaybeFlushBlocks(node);
                }
            } else {
                BanNode(node->Descriptor.GetDefaultAddress());
                EnqueueCommand(TSwitchChunkCommand{session});
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
                LOG_DEBUG("Chunk session finished (Address: %v)",
                    node->Descriptor.GetDefaultAddress());
            } else {
                BanNode(node->Descriptor.GetDefaultAddress());
                LOG_WARNING(rspOrError, "Chunk session has failed to finish (Address: %v)",
                    node->Descriptor.GetDefaultAddress());
            }
        }


        void MaybeFlushBlocks(const TNodePtr& node)
        {
            if (!node->Started) {
                return;
            }

            if (!node->InFlightBatches.empty()) {
                return;
            }

            if (node->PendingBatches.empty()) {
                return;
            }

            i64 flushRowCount = 0;
            i64 flushDataSize = 0;

            auto req = node->HeavyProxy.PutBlocks();
            req->SetMultiplexingBand(DefaultHeavyMultiplexingBand);
            ToProto(req->mutable_session_id(), CurrentSession_->Id);
            req->set_first_block_index(node->FirstPendingBlockIndex);
            req->set_flush_blocks(true);

            Y_ASSERT(node->InFlightBatches.empty());
            while (flushRowCount <= Config_->MaxFlushRowCount &&
                   flushDataSize <= Config_->MaxFlushDataSize &&
                   !node->PendingBatches.empty())
            {
                auto batch = node->PendingBatches.front();
                node->PendingBatches.pop();

                req->Attachments().insert(req->Attachments().end(), batch->Rows.begin(), batch->Rows.end());

                flushRowCount += batch->Rows.size();
                flushDataSize += batch->DataSize;

                node->InFlightBatches.push_back(batch);
            }

            LOG_DEBUG("Flushing journal replica (Address: %v, BlockIds: %v:%v-%v, Rows: %v-%v)",
                node->Descriptor.GetDefaultAddress(),
                CurrentSession_->Id,
                node->FirstPendingBlockIndex,
                node->FirstPendingBlockIndex + flushRowCount - 1,
                node->FirstPendingRowIndex,
                node->FirstPendingRowIndex + flushRowCount - 1);

            req->Invoke().Subscribe(
                BIND(&TImpl::OnBlocksFlushed, MakeWeak(this), CurrentSession_, node, flushRowCount)
                    .Via(Invoker_));
        }

        void OnBlocksFlushed(
            const TChunkSessionPtr& session,
            const TNodePtr& node,
            i64 flushRowCount,
            const TDataNodeServiceProxy::TErrorOrRspPutBlocksPtr& rspOrError)
        {
            if (session != CurrentSession_)
                return;

            if (!rspOrError.IsOK()) {
                OnReplicaFailed(rspOrError, node, session);
                return;
            }

            LOG_DEBUG("Journal replica flushed (Address: %v, BlockIds: %v:%v-%v, Rows: %v-%v)",
                node->Descriptor.GetDefaultAddress(),
                session->Id,
                node->FirstPendingBlockIndex,
                node->FirstPendingBlockIndex + flushRowCount - 1,
                node->FirstPendingRowIndex,
                node->FirstPendingRowIndex + flushRowCount - 1);

            for (const auto& batch : node->InFlightBatches) {
                ++batch->FlushedReplicas;
            }

            node->FirstPendingBlockIndex += flushRowCount;
            node->FirstPendingRowIndex += flushRowCount;
            node->InFlightBatches.clear();

            std::vector<TPromise<void>> fulfilledPromises;
            while (!PendingBatches_.empty()) {
                auto front = PendingBatches_.front();
                if (front->FlushedReplicas <  WriteQuorum_)
                    break;

                fulfilledPromises.push_back(front->FlushedPromise);
                session->FlushedRowCount += front->Rows.size();
                session->FlushedDataSize += front->DataSize;
                PendingBatches_.pop_front();

                LOG_DEBUG("Rows are flushed by quorum (Rows: %v-%v)",
                    front->FirstRowIndex,
                    front->FirstRowIndex + front->Rows.size() - 1);
            }

            MaybeFlushBlocks(node);

            for (auto& promise : fulfilledPromises) {
                promise.Set();
            }
        }

        void OnReplicaFailed(
            const TError& error,
            const TNodePtr& node,
            const TChunkSessionPtr& session)
        {
            const auto& address = node->Descriptor.GetDefaultAddress();
            LOG_WARNING(error, "Journal replica failed (Address: %v, SessionId: %v)",
                address,
                session->Id);

            BanNode(address);
            EnqueueCommand(TSwitchChunkCommand{session});
        }
    };


    const TIntrusivePtr<TImpl> Impl_;

};

IJournalWriterPtr CreateJournalWriter(
    INativeClientPtr client,
    const TYPath& path,
    const TJournalWriterOptions& options)
{
    return New<TJournalWriter>(client, path, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
