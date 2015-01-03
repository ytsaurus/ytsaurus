#include "stdafx.h"
#include "journal_writer.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/scheduler.h>
#include <core/concurrency/delayed_executor.h>
#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/nonblocking_queue.h>
#include <core/concurrency/parallel_collector.h>

#include <core/misc/address.h>
#include <core/misc/variant.h>

#include <core/ytree/attribute_helpers.h>

#include <core/rpc/helpers.h>

#include <core/logging/log.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/chunk_client/private.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <ytlib/chunk_client/data_node_service_proxy.h>
#include <ytlib/chunk_client/chunk_ypath_proxy.h>
#include <ytlib/chunk_client/chunk_list_ypath_proxy.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_service_proxy.h>

#include <ytlib/journal_client/journal_ypath_proxy.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction_listener.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <queue>
#include <deque>

namespace NYT {
namespace NApi {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYPath;
using namespace NRpc;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTransactionClient;
using namespace NJournalClient;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

class TJournalWriter
    : public IJournalWriter
{
public:
    TJournalWriter(
        IClientPtr client,
        const TYPath& path,
        const TJournalWriterOptions& options,
        TJournalWriterConfigPtr config)
        : Impl_(New<TImpl>(
            client,
            path,
            options,
            config))
    { }

    ~TJournalWriter()
    {
        Impl_->Cancel();
    }

    virtual TAsyncError Open() override
    {
        return Impl_->Open();
    }

    virtual TAsyncError Write(const std::vector<TSharedRef>& rows) override
    {
        return Impl_->Write(rows);
    }

    virtual TAsyncError Close() override
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
            const TJournalWriterOptions& options,
            TJournalWriterConfigPtr config)
            : Client_(client)
            , Path_(path)
            , Options_(options)
            , Config_(config ? config : New<TJournalWriterConfig>())
            , ObjectProxy_(Client_->GetMasterChannel())
        {
            if (Options_.TransactionId != NullTransactionId) {
                auto transactionManager = Client_->GetTransactionManager();
                TTransactionAttachOptions attachOptions(Options_.TransactionId);
                attachOptions.AutoAbort = false;
                Transaction_ = transactionManager->Attach(attachOptions);
            }

            Logger.AddTag("Path: %v, TransactionId: %v",
                Path_,
                Options_.TransactionId);

            // Spawn the actor.
            BIND(&TImpl::ActorMain, MakeStrong(this))
                // TODO(babenko): another invoker?
                .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
                .Run();

            if (Transaction_) {
                ListenTransaction(Transaction_);
            }
        }

        TAsyncError Open()
        {
            return OpenedPromise_;
        }

        TAsyncError Write(const std::vector<TSharedRef>& rows)
        {
            TGuard<TSpinLock> guard(CurrentBatchSpinLock_);

            if (!Error_.IsOK()) {
                return MakeFuture(Error_);
            }

            auto batch = EnsureCurrentBatch();
            for (const auto& row : rows) {
                AppendToBatch(batch, row);
                if (IsBatchFull(batch)) {
                    FlushCurrentBatch();
                    batch = EnsureCurrentBatch();
                }
            }

            // NB: We can form a handful of batches but since flushes are monotonic,
            // the last one will do.
            return batch->FlushedPromise;
        }

        TAsyncError Close()
        {
            EnqueueCommand(TCloseCommand());
            return ClosedPromise_;
        }

        void Cancel()
        {
            EnqueueCommand(TCancelCommand());
        }

    private:
        IClientPtr Client_;
        TYPath Path_;
        TJournalWriterOptions Options_;
        TJournalWriterConfigPtr Config_;

        IInvokerPtr Invoker_;
        TObjectServiceProxy ObjectProxy_;

        NLog::TLogger Logger = ApiLogger;

        struct TBatch
            : public TIntrinsicRefCounted
        {
            i64 FirstRowIndex = -1;
            i64 DataSize = 0;
            std::vector<TSharedRef> Rows;
            TAsyncErrorPromise FlushedPromise = NewPromise<TError>();
            int FlushedReplicas = 0;
        };

        typedef TIntrusivePtr<TBatch> TBatchPtr;

        TSpinLock CurrentBatchSpinLock_;
        TError Error_;
        TBatchPtr CurrentBatch_;
        TDelayedExecutorCookie CurrentBatchFlushCookie_;

        TAsyncErrorPromise OpenedPromise_ = NewPromise<TError>();

        bool Closing_ = false;
        TAsyncErrorPromise ClosedPromise_ = NewPromise<TError>();

        TTransactionPtr Transaction_;
        TTransactionPtr UploadTransaction_;
        
        int ReplicationFactor_ = -1;
        int ReadQuorum_ = -1;
        int WriteQuorum_ = -1;
        Stroka Account_;

        TChunkListId ChunkListId_;

        struct TNode
            : public TRefCounted
        {
            TNodeDescriptor Descriptor;
            TDataNodeServiceProxy LightProxy;
            TDataNodeServiceProxy HeavyProxy;
            TPeriodicExecutorPtr PingExecutor;

            i64 FirstPendingBlockIndex = 0;
            i64 FirstPendingRowIndex = 0;

            std::queue<TBatchPtr> PendingBatches;
            std::vector<TBatchPtr> InFlightBatches;

            explicit TNode(const TNodeDescriptor& descriptor)
                : Descriptor(descriptor)
                , LightProxy(LightNodeChannelFactory->CreateChannel(descriptor.GetDefaultAddress()))
                , HeavyProxy(HeavyNodeChannelFactory->CreateChannel(descriptor.GetDefaultAddress()))
            { }
        };

        typedef TIntrusivePtr<TNode> TNodePtr;
        typedef TWeakPtr<TNode> TNodeWeakPtr;

        TNodeDirectoryPtr NodeDirectory_ = New<TNodeDirectory>();

        struct TChunkSession
            : public TRefCounted
        {
            TChunkId ChunkId;
            std::vector<TNodePtr> Nodes;
            i64 RowCount = 0;
            i64 DataSize = 0;
            i64 FlushedRowCount = 0;
            i64 FlushedDataSize = 0;
        };

        typedef TIntrusivePtr<TChunkSession> TChunkSessionPtr;
        typedef TWeakPtr<TChunkSession> TChunkSessionWeakPtr;

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

        yhash_map<Stroka, TInstant> BannedNodeToDeadline_;


        void EnqueueCommand(TCommand command)
        {
            CommandQueue_.Enqueue(std::move(command));
        }
        
        TCommand DequeueCommand()
        {
            return WaitFor(CommandQueue_.Dequeue());
        }


        void BanNode(const Stroka& address)
        {
            if (BannedNodeToDeadline_.find(address) == BannedNodeToDeadline_.end()) {
                BannedNodeToDeadline_.insert(std::make_pair(address, TInstant::Now() + Config_->NodeBanTimeout));
                LOG_INFO("Node banned (Address: %v)", address);
            }
        }

        std::vector<Stroka> GetBannedNodes()
        {
            std::vector<Stroka> result;
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
            LOG_INFO("Creating upload transaction");
    
            {
                NTransactionClient::TTransactionStartOptions options;
                options.ParentId = Transaction_ ? Transaction_->GetId() : NullTransactionId;
                options.EnableUncommittedAccounting = false;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Format("Journal upload to %v", Path_));
                options.Attributes = attributes.get();

                auto transactionManager = Client_->GetTransactionManager();
                auto transactionOrError = WaitFor(transactionManager->Start(
                    ETransactionType::Master,
                    options));
                THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError, "Error creating upload transaction");
                UploadTransaction_ = transactionOrError.Value();
            }

            LOG_INFO("Upload transaction created (TransactionId: %v)", UploadTransaction_->GetId());
            
            ListenTransaction(UploadTransaction_);

            LOG_INFO("Opening journal");

            auto batchReq = CreateMasterBatchRequest();

            {
                auto req = TCypressYPathProxy::Get(Path_);
                SetTransactionId(req, UploadTransaction_->GetId());
                TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
                attributeFilter.Keys.push_back("type");
                attributeFilter.Keys.push_back("replication_factor");
                attributeFilter.Keys.push_back("read_quorum");
                attributeFilter.Keys.push_back("write_quorum");
                attributeFilter.Keys.push_back("account");
                ToProto(req->mutable_attribute_filter(), attributeFilter);
                batchReq->AddRequest(req, "get_attributes");
            }

            {
                auto req = TJournalYPathProxy::PrepareForUpdate(Path_);
                req->set_mode(static_cast<int>(EUpdateMode::Append));
                GenerateMutationId(req);
                SetTransactionId(req, UploadTransaction_->GetId());
                batchReq->AddRequest(req, "prepare_for_update");
            }

            auto batchRsp = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error opening journal");

            {
                auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting journal attributes");

                auto node = ConvertToNode(TYsonString(rsp->value()));
                const auto& attributes = node->Attributes();

                auto type = attributes.Get<EObjectType>("type");
                if (type != EObjectType::Journal) {
                    THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                        Path_,
                        EObjectType::Journal,
                        type);
                }

                ReplicationFactor_ = attributes.Get<int>("replication_factor");
                ReadQuorum_ = attributes.Get<int>("read_quorum");
                WriteQuorum_ = attributes.Get<int>("write_quorum");
                Account_ = attributes.Get<Stroka>("account");
            }

            {
                auto rsp = batchRsp->GetResponse<TJournalYPathProxy::TRspPrepareForUpdate>("prepare_for_update");
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error preparing journal for update");
                ChunkListId_ = FromProto<TChunkListId>(rsp->chunk_list_id());
            }

            LOG_INFO("Journal opened (ReplicationFactor: %v, WriteQuorum: %v, Account: %v, ChunkListId: %v)",
                ReplicationFactor_,
                WriteQuorum_,
                Account_,
                ChunkListId_);

            LOG_INFO("Journal writer opened");
            OpenedPromise_.Set(TError());
        }

        void CloseJournal()
        {
            LOG_INFO("Journal writer closed");
            ClosedPromise_.TrySet(TError());
        }

        bool TryOpenChunk()
        {
            CurrentSession_ = New<TChunkSession>();

            LOG_INFO("Creating chunk");

            {
                auto req = TMasterYPathProxy::CreateObjects();
                req->set_type(static_cast<int>(EObjectType::JournalChunk));
                req->set_account(Account_);
                ToProto(req->mutable_transaction_id(), UploadTransaction_->GetId());

                auto* reqExt = req->MutableExtension(TReqCreateChunkExt::create_chunk_ext);
                reqExt->set_replication_factor(ReplicationFactor_);
                reqExt->set_read_quorum(ReadQuorum_);
                reqExt->set_write_quorum(WriteQuorum_);
                reqExt->set_movable(true);
                reqExt->set_vital(true);
                reqExt->set_erasure_codec(static_cast<int>(NErasure::ECodec::None));

                auto rsp = WaitFor(ObjectProxy_.Execute(req));
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error creating chunk");
                CurrentSession_->ChunkId = FromProto<TChunkId>(rsp->object_ids(0));
            }

            LOG_INFO("Chunk created (ChunkId: %v)",
                CurrentSession_->ChunkId);

            std::vector<TChunkReplica> replicas;
            std::vector<TNodeDescriptor> targets;
            {
                TChunkServiceProxy chunkProxy(Client_->GetMasterChannel());
                auto req = chunkProxy.AllocateWriteTargets();
                ToProto(req->mutable_chunk_id(), CurrentSession_->ChunkId);
                ToProto(req->mutable_forbidden_addresses(), GetBannedNodes());
                if (Config_->PreferLocalHost) {
                    req->set_preferred_host_name(TAddressResolver::Get()->GetLocalHostName());
                }
                req->set_target_count(ReplicationFactor_);

                auto rsp = WaitFor(req->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error allocating write targets");

                NodeDirectory_->MergeFrom(rsp->node_directory());

                replicas = NYT::FromProto<TChunkReplica>(rsp->replicas());
                for (auto replica : replicas) {
                    const auto& descriptor = NodeDirectory_->GetDescriptor(replica);
                    targets.push_back(descriptor);
                }
            }

            LOG_INFO("Write targets allocated (Targets: [%v])",
                JoinToString(targets));

            for (int index = 0; index < ReplicationFactor_; ++index) {
                auto node = New<TNode>(targets[index]);
                node->LightProxy.SetDefaultTimeout(Config_->NodeRpcTimeout);
                node->HeavyProxy.SetDefaultTimeout(Config_->NodeRpcTimeout);
                CurrentSession_->Nodes.push_back(node);
            }

            LOG_INFO("Starting chunk sessions");
            try {
                auto collector = New<TParallelCollector<void>>();
                for (auto node : CurrentSession_->Nodes) {
                    auto req = node->LightProxy.StartChunk();
                    ToProto(req->mutable_chunk_id(), CurrentSession_->ChunkId);
                    req->set_session_type(static_cast<int>(EWriteSessionType::User));
                    req->set_optimize_for_latency(true);
                    auto asyncRsp = req->Invoke().Apply(
                        BIND(&TImpl::OnChunkStarted, MakeStrong(this), node)
                            .AsyncVia(GetCurrentInvoker()));
                    collector->Collect(asyncRsp);
                }
                auto result = WaitFor(collector->Complete());
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error starting chunk sessions");
            } catch (const std::exception& ex) {
                LOG_WARNING(ex, "Chunk open attempt failed");
                CurrentSession_.Reset();
                return false;
            }
            LOG_INFO("Chunk sessions started");

            for (auto node : CurrentSession_->Nodes) {
                node->PingExecutor = New<TPeriodicExecutor>(
                    GetCurrentInvoker(),
                    BIND(&TImpl::SendPing, MakeWeak(this), MakeWeak(CurrentSession_), MakeWeak(node)),
                    Config_->NodePingPeriod);
                node->PingExecutor->Start();
            }

            LOG_INFO("Attaching chunk");
            {
                auto batchReq = CreateMasterBatchRequest();

                {
                    auto req = TChunkYPathProxy::Confirm(FromObjectId(CurrentSession_->ChunkId));
                    req->mutable_chunk_info();
                    ToProto(req->mutable_replicas(), replicas);
                    auto* meta = req->mutable_chunk_meta();
                    meta->set_type(static_cast<int>(EChunkType::Journal));
                    meta->set_version(0);
                    TMiscExt miscExt;
                    SetProtoExtension(meta->mutable_extensions(), miscExt);
                    GenerateMutationId(req);
                    batchReq->AddRequest(req, "confirm");
                }
                {
                    auto req = TChunkListYPathProxy::Attach(FromObjectId(ChunkListId_));
                    ToProto(req->add_children_ids(), CurrentSession_->ChunkId);
                    GenerateMutationId(req);
                    batchReq->AddRequest(req, "attach");
                }

                auto batchRsp = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError(), "Error attaching chunk");
            }
            LOG_INFO("Chunk attached");
        
            for (auto batch : PendingBatches_) {
                EnqueueBatchToSession(batch);
            }

            return true;
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
                CheckAborted();
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
            i64 rowCount = batch->Rows.size();

            LOG_DEBUG("Batch ready (Rows: %v-%v)",
                CurrentRowIndex_,
                CurrentRowIndex_ + rowCount - 1);

            batch->FirstRowIndex = CurrentRowIndex_;
            CurrentRowIndex_ += rowCount;

            PendingBatches_.push_back(batch);

            EnqueueBatchToSession(batch);
        }

        bool IsSessionOverfull()
        {
            return
                CurrentSession_->RowCount > Config_->MaxChunkRowCount ||
                CurrentSession_->DataSize > Config_->MaxChunkDataSize;
        }

        void EnqueueBatchToSession(TBatchPtr batch)
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

            for (auto node : CurrentSession_->Nodes) {
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

            LOG_INFO("Finishing chunk sessions");
            for (auto node : session->Nodes) {
                auto req = node->LightProxy.FinishChunk();
                ToProto(req->mutable_chunk_id(), session->ChunkId);
                req->Invoke().Subscribe(
                    BIND(&TImpl::OnChunkFinished, MakeStrong(this), node)
                        .Via(GetCurrentInvoker()));
                if (node->PingExecutor) {
                    node->PingExecutor->Stop();
                    node->PingExecutor.Reset();
                }
            }

            LOG_INFO("Sealing chunk (ChunkId: %v, RowCount: %v)",
                session->ChunkId,
                session->FlushedRowCount);
            {
                auto req = TChunkYPathProxy::Seal(FromObjectId(session->ChunkId));
                auto* info = req->mutable_info();
                info->set_sealed(true);
                info->set_row_count(session->FlushedRowCount);
                info->set_uncompressed_data_size(session->FlushedDataSize);
                info->set_compressed_data_size(session->FlushedDataSize);
                auto rsp = WaitFor(ObjectProxy_.Execute(req));
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error sealing chunk %v",
                    session->ChunkId);
            }
            LOG_INFO("Chunk sealed");
        }


        void ActorMain()
        {
            try {
                GuardedActorMain();
            } catch (const std::exception& ex) {
                PumpFailed(ex);
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

            for (auto batch : PendingBatches_) {
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


        static void AppendToBatch(const TBatchPtr& batch, const TSharedRef& row)
        {
            YASSERT(row);
            batch->Rows.push_back(row);
            batch->DataSize += row.Size();
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
                CurrentBatchFlushCookie_ = TDelayedExecutor::Submit(
                    BIND(&TImpl::OnBatchTimeout, MakeWeak(this), CurrentBatch_)
                        .Via(GetCurrentInvoker()),
                    Config_->MaxBatchDelay);
            }

            return CurrentBatch_;
        }

        void OnBatchTimeout(TBatchPtr batch)
        {
            TGuard<TSpinLock> guard(CurrentBatchSpinLock_);
            if (CurrentBatch_ == batch) {
                FlushCurrentBatch();
            }
        }

        void FlushCurrentBatch()
        {
            VERIFY_SPINLOCK_AFFINITY(CurrentBatchSpinLock_);

            if (CurrentBatchFlushCookie_) {
                TDelayedExecutor::CancelAndClear(CurrentBatchFlushCookie_);
            }

            EnqueueCommand(TBatchCommand(CurrentBatch_));
            CurrentBatch_.Reset();
        }
  

        void SendPing(TChunkSessionWeakPtr session_, TNodeWeakPtr node_)
        {
            auto session = session_.Lock();
            if (!session)
                return;

            auto node = node_.Lock();
            if (!node)
                return;

            LOG_DEBUG("Sending ping (Address: %v, ChunkId: %v)",
                node->Descriptor.GetDefaultAddress(),
                session->ChunkId);

            auto req = node->LightProxy.PingSession();
            ToProto(req->mutable_chunk_id(), session->ChunkId);
            req->Invoke().Subscribe(
                BIND(&TImpl::OnPingSent, MakeWeak(this), session, node)
                    .Via(GetCurrentInvoker()));
        }

        void OnPingSent(TChunkSessionPtr session, TNodePtr node, TDataNodeServiceProxy::TRspPingSessionPtr rsp)
        {
            if (session != CurrentSession_)
                return;

            if (!rsp->IsOK()) {
                OnReplicaFailed(rsp->GetError(), node, session);
                return;
            }

            LOG_DEBUG("Ping succeeded (Address: %v, ChunkId: %v)",
                node->Descriptor.GetDefaultAddress(),
                session->ChunkId);
        }


        TError OnChunkStarted(TNodePtr node, TDataNodeServiceProxy::TRspStartChunkPtr rsp)
        {
            if (rsp->IsOK()) {
                LOG_DEBUG("Chunk session started (Address: %v)",
                    node->Descriptor.GetDefaultAddress());
                return TError();
            } else {
                BanNode(node->Descriptor.GetDefaultAddress());
                return TError("Error starting session at %v",
                    node->Descriptor.GetDefaultAddress())
                    << *rsp;
            }
        }

        void OnChunkFinished(TNodePtr node, TDataNodeServiceProxy::TRspFinishChunkPtr rsp)
        {
            if (rsp->IsOK()) {
                LOG_DEBUG("Chunk session finished (Address: %v)",
                    node->Descriptor.GetDefaultAddress());
            } else {
                BanNode(node->Descriptor.GetDefaultAddress());
                LOG_WARNING(*rsp, "Chunk session has failed to finish (Address: %v)",
                    node->Descriptor.GetDefaultAddress());
            }
        }


        void MaybeFlushBlocks(TNodePtr node)
        {
            if (!node->InFlightBatches.empty() || node->PendingBatches.empty())
                return;

            i64 flushRowCount = 0;
            i64 flushDataSize = 0;

            auto req = node->HeavyProxy.PutBlocks();
            ToProto(req->mutable_chunk_id(), CurrentSession_->ChunkId);
            req->set_first_block_index(node->FirstPendingBlockIndex);
            req->set_flush_blocks(true);

            YASSERT(node->InFlightBatches.empty());
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
                CurrentSession_->ChunkId,
                node->FirstPendingBlockIndex,
                node->FirstPendingBlockIndex + flushRowCount - 1,
                node->FirstPendingRowIndex,
                node->FirstPendingRowIndex + flushRowCount - 1);

            req->Invoke().Subscribe(
                BIND(&TImpl::OnBlocksFlushed, MakeWeak(this), CurrentSession_, node, flushRowCount)
                    .Via(GetCurrentInvoker()));
        }

        void OnBlocksFlushed(
            TChunkSessionPtr session,
            TNodePtr node,
            i64 flushRowCount,
            TDataNodeServiceProxy::TRspPutBlocksPtr rsp)
        {
            if (session != CurrentSession_)
                return;

            if (!rsp->IsOK()) {
                OnReplicaFailed(rsp->GetError(), node, session);
                return;
            }

            LOG_DEBUG("Journal replica flushed (Address: %v, BlockIds: %v:%v-%v, Rows: %v-%v)",
                node->Descriptor.GetDefaultAddress(),
                session->ChunkId,
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

            while (!PendingBatches_.empty()) {
                auto front = PendingBatches_.front();
                if (front->FlushedReplicas <  WriteQuorum_)
                    break;

                front->FlushedPromise.Set(TError());
                session->FlushedRowCount += front->Rows.size();
                session->FlushedDataSize += front->DataSize;
                PendingBatches_.pop_front();

                LOG_DEBUG("Rows are flushed by quorum (Rows: %v-%v)",
                    front->FirstRowIndex,
                    front->FirstRowIndex + front->Rows.size() - 1);
            }

            MaybeFlushBlocks(node);
        }


        void OnReplicaFailed(const TError& error, TNodePtr node, TChunkSessionPtr session)
        {
            const auto& address = node->Descriptor.GetDefaultAddress();

            LOG_WARNING(error, "Journal replica failed (Address: %v, ChunkId: %v)",
                address,
                session->ChunkId);

            BanNode(address);

            TSwitchChunkCommand command;
            command.Session = session;
            EnqueueCommand(command);
        }


        TObjectServiceProxy::TReqExecuteBatchPtr CreateMasterBatchRequest()
        {
            TObjectServiceProxy proxy(Client_->GetMasterChannel());
            auto batchReq = proxy.ExecuteBatch();
            for (const auto& id : Options_.PrerequisiteTransactionIds) {
                batchReq->PrerequisiteTransactions().push_back(TObjectServiceProxy::TPrerequisiteTransaction(id));
            }
            return batchReq;
        }

    };


    TIntrusivePtr<TImpl> Impl_;

};

IJournalWriterPtr CreateJournalWriter(
    IClientPtr client,
    const TYPath& path,
    const TJournalWriterOptions& options,
    TJournalWriterConfigPtr config)
{
    return New<TJournalWriter>(
        client,
        path,
        options,
        config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
