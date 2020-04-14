#include "transaction_impl.h"
#include "client_impl.h"
#include "helpers.h"
#include "config.h"
#include "private.h"

#include <yt/client/transaction_client/helpers.h>

#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/client/api/transaction.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NCypressClient;
using namespace NApi;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcProxyClientLogger;

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(
    TConnectionPtr connection,
    TClientPtr client,
    NRpc::IChannelPtr channel,
    TTransactionId id,
    TTimestamp startTimestamp,
    ETransactionType type,
    EAtomicity atomicity,
    EDurability durability,
    TDuration timeout,
    std::optional<TDuration> pingPeriod,
    bool sticky)
    : Connection_(std::move(connection))
    , Client_(std::move(client))
    , Channel_(std::move(channel))
    , Id_(id)
    , StartTimestamp_(startTimestamp)
    , Type_(type)
    , Atomicity_(atomicity)
    , Durability_(durability)
    , Timeout_(timeout)
    , PingPeriod_(pingPeriod)
    , Sticky_(sticky)
{
    // TODO(babenko): "started" is only correct as long as we do not support attaching to existing transactions
    YT_LOG_DEBUG("Transaction started (TransactionId: %v, Type: %v, StartTimestamp: %llx, Atomicity: %v, "
        "Durability: %v, Timeout: %v, PingPeriod: %v, Sticky: %v)",
        Id_,
        Type_,
        StartTimestamp_,
        Atomicity_,
        Durability_,
        Timeout_,
        PingPeriod_,
        Sticky_);

    // TODO(babenko): don't run periodic pings if client explicitly disables them in options
    RunPeriodicPings();
}

IConnectionPtr TTransaction::GetConnection()
{
    return Connection_;
}

IClientPtr TTransaction::GetClient() const
{
    return Client_;
}

TTransactionId TTransaction::GetId() const
{
    return Id_;
}

TTimestamp TTransaction::GetStartTimestamp() const
{
    return StartTimestamp_;
}

ETransactionType TTransaction::GetType() const
{
    return Type_;
}

EAtomicity TTransaction::GetAtomicity() const
{
    return Atomicity_;
}

EDurability TTransaction::GetDurability() const
{
    return Durability_;
}

TDuration TTransaction::GetTimeout() const
{
    return Timeout_;
}

TApiServiceProxy TTransaction::CreateApiServiceProxy()
{
    TApiServiceProxy proxy(Channel_);
    auto config = Connection_->GetConfig();
    proxy.SetDefaultRequestCodec(config->RequestCodec);
    proxy.SetDefaultResponseCodec(config->ResponseCodec);
    proxy.SetDefaultEnableLegacyRpcCodecs(config->EnableLegacyRpcCodecs);
    return proxy;
}

TFuture<void> TTransaction::Ping(const NApi::TTransactionPingOptions& /*options*/)
{
    return SendPing();
}

void TTransaction::Detach()
{
    {
        auto guard = Guard(SpinLock_);
        switch (State_) {
            case ETransactionState::Committed:
                THROW_ERROR_EXCEPTION("Transaction %v is already committed",
                    Id_);

            case ETransactionState::Aborted:
                THROW_ERROR_EXCEPTION("Transaction %v is already aborted",
                    Id_);

            case ETransactionState::Active:
                State_ = ETransactionState::Detached;
                break;

            case ETransactionState::Detached:
                return;

            default:
                YT_ABORT();
        }
    }

    YT_LOG_DEBUG("Transaction detached (TransactionId: %v)",
        Id_);
}

TFuture<TTransactionPrepareResult> TTransaction::Prepare()
{
    YT_UNIMPLEMENTED();
}

TFuture<TTransactionFlushResult> TTransaction::Flush()
{
    YT_UNIMPLEMENTED();
}

void TTransaction::SubscribeCommitted(const TCallback<void()>& handler)
{
    Committed_.Subscribe(handler);
}

void TTransaction::UnsubscribeCommitted(const TCallback<void()>& handler)
{
    Committed_.Unsubscribe(handler);
}

void TTransaction::SubscribeAborted(const TCallback<void()>& handler)
{
    Aborted_.Subscribe(handler);
}

void TTransaction::UnsubscribeAborted(const TCallback<void()>& handler)
{
    Aborted_.Unsubscribe(handler);
}

TFuture<TTransactionCommitResult> TTransaction::Commit(const TTransactionCommitOptions&)
{
    YT_LOG_DEBUG("Committing transaction (TransactionId: %v)",
        Id_);

    std::vector<TFuture<void>> asyncResults;
    {
        auto guard = Guard(SpinLock_);
        if (!Error_.IsOK()) {
            return MakeFuture<TTransactionCommitResult>(Error_);
        }
        switch (State_) {
            case ETransactionState::Committing:
                return MakeFuture<TTransactionCommitResult>(TError("Transaction %v is already being committed",
                    Id_));

            case ETransactionState::Committed:
                return MakeFuture<TTransactionCommitResult>(TError("Transaction %v is already committed",
                    Id_));

            case ETransactionState::Aborted:
                return MakeFuture<TTransactionCommitResult>(TError("Transaction %v is already aborted",
                    Id_));

            case ETransactionState::Detached:
                return MakeFuture<TTransactionCommitResult>(TError("Transaction %v is detached",
                    Id_));

            case ETransactionState::Active:
                State_ = ETransactionState::Committing;
                asyncResults = std::move(AsyncResults_);
                break;

            default:
                YT_ABORT();
        }
    }

    {
        auto guard = Guard(BatchModifyRowsRequestLock_);
        if (BatchModifyRowsRequest_) {
            asyncResults.emplace_back(InvokeBatchModifyRowsRequest());
        }
    }

    return Combine(asyncResults).Apply(
        BIND([this, this_ = MakeStrong(this)] () {
            {
                auto guard = Guard(SpinLock_);
                if (!Error_.IsOK()) {
                    return MakeFuture<TTransactionCommitResult>(Error_);
                }
            }

            const auto& config = Connection_->GetConfig();

            auto proxy = CreateApiServiceProxy();

            auto req = proxy.CommitTransaction();
            req->SetTimeout(config->RpcTimeout);

            ToProto(req->mutable_transaction_id(), Id_);
            req->set_sticky(Sticky_);

            return req->Invoke().Apply(
                BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TApiServiceProxy::TRspCommitTransactionPtr>& rspOrError) -> TErrorOr<TTransactionCommitResult> {
                    if (rspOrError.IsOK()) {
                        const auto& rsp = rspOrError.Value();
                        TTransactionCommitResult result{
                            FromProto<NHiveClient::TTimestampMap>(rsp->commit_timestamps())
                        };
                        auto error = SetCommitted(result);
                        if (!error.IsOK()) {
                            return error;
                        }
                        return result;
                    } else {
                        auto error = TError("Error committing transaction %v ",
                            Id_) << rspOrError;
                        OnFailure(error);
                        return error;
                    }
                }));
        }));
}

TFuture<void> TTransaction::Abort(const TTransactionAbortOptions& /*options*/)
{
    // TODO(babenko): options are ignored
    YT_LOG_DEBUG("Transaction abort requested (TransactionId: %v)",
        Id_);
    SetAborted(TError("Transaction aborted by user request"));

    return SendAbort();
}

void TTransaction::ModifyRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TRowModification> modifications,
    const TModifyRowsOptions& options)
{
    ValidateActive();
    ValidateTabletTransactionId(GetId());

    for (const auto& modification : modifications) {
        // TODO(sandello): handle versioned rows
        YT_VERIFY(
            modification.Type == ERowModificationType::Write ||
            modification.Type == ERowModificationType::Delete ||
            modification.Type == ERowModificationType::ReadLockWrite);
    }

    const auto& config = Connection_->GetConfig();

    auto proxy = CreateApiServiceProxy();
    auto req = proxy.ModifyRows();
    req->SetTimeout(config->RpcTimeout);

    auto reqSequenceNumber = ModifyRowsRequestSequenceCounter_++;

    req->set_sequence_number(reqSequenceNumber);

    ToProto(req->mutable_transaction_id(), Id_);
    req->set_path(path);

    req->set_require_sync_replica(options.RequireSyncReplica);
    ToProto(req->mutable_upstream_replica_id(), options.UpstreamReplicaId);

    std::vector<TUnversionedRow> rows;
    rows.reserve(modifications.Size());

    bool usedStrongLocks = false;
    for (const auto& modification : modifications) {
        auto mask = modification.Locks;
        for (int index = 0; index < TLockMask::MaxCount; ++index) {
            usedStrongLocks |= mask.Get(index) == ELockType::SharedStrong;
        }
    }

    if (usedStrongLocks) {
        req->Header().set_protocol_version_minor(YTRpcModifyRowsStrongLocksVersion);
    }

    for (const auto& modification : modifications) {
        rows.emplace_back(modification.Row);
        req->add_row_modification_types(static_cast<NProto::ERowModificationType>(modification.Type));
        if (usedStrongLocks) {
            req->add_row_locks(modification.Locks);
        } else {
            ui32 mask = 0;
            for (int index = 0; index < TLockMask::MaxCount; ++index) {
                if (modification.Locks.Get(index) == ELockType::SharedWeak) {
                    mask |= 1u << index;
                }
            }
            req->add_row_read_locks(mask);
        }
    }

    req->Attachments() = SerializeRowset(
        nameTable,
        MakeRange(rows),
        req->mutable_rowset_descriptor());

    TFuture<void> asyncResult;
    if (config->ModifyRowsBatchCapacity == 0) {
        asyncResult = req->Invoke().As<void>();
    } else {
        YT_LOG_DEBUG("Pushing a subrequest into a BatchModifyRows rows request (SubrequestAttachmentCount: 1+%v)",
            req->Attachments().size());

        auto guard = Guard(BatchModifyRowsRequestLock_);
        if (!BatchModifyRowsRequest_) {
            BatchModifyRowsRequest_ = CreateBatchModifyRowsRequest();
        }
        auto reqBody = SerializeProtoToRef(*req);
        BatchModifyRowsRequest_->Attachments().push_back(reqBody);
        BatchModifyRowsRequest_->Attachments().insert(
            BatchModifyRowsRequest_->Attachments().end(),
            req->Attachments().begin(),
            req->Attachments().end());
        BatchModifyRowsRequest_->add_part_counts(req->Attachments().size());
        if (BatchModifyRowsRequest_->part_counts_size() == config->ModifyRowsBatchCapacity) {
            asyncResult = InvokeBatchModifyRowsRequest();
        }
    }

    if (asyncResult) {
        asyncResult
            .Subscribe(BIND([=, this_ = MakeStrong(this)](const TError& error) {
                if (!error.IsOK()) {
                    OnFailure(error);
                }
            }));

        {
            auto guard = Guard(SpinLock_);
            AsyncResults_.emplace_back(std::move(asyncResult));
        }
    }
}

TFuture<ITransactionPtr> TTransaction::StartTransaction(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    ValidateActive();
    return Client_->StartTransaction(
        type,
        PatchTransactionId(options));
}

TFuture<IUnversionedRowsetPtr> TTransaction::LookupRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<TKey>& keys,
    const TLookupRowsOptions& options)
{
    ValidateActive();
    return Client_->LookupRows(
        path,
        std::move(nameTable),
        keys,
        PatchTransactionTimestamp(options));
}

TFuture<IVersionedRowsetPtr> TTransaction::VersionedLookupRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<TKey>& keys,
    const TVersionedLookupRowsOptions& options)
{
    ValidateActive();
    return Client_->VersionedLookupRows(
        path,
        std::move(nameTable),
        keys,
        PatchTransactionTimestamp(options));
}

TFuture<TSelectRowsResult> TTransaction::SelectRows(
    const TString& query,
    const TSelectRowsOptions& options)
{
    ValidateActive();
    return Client_->SelectRows(
        query,
        PatchTransactionTimestamp(options));
}

TFuture<NYson::TYsonString> TTransaction::Explain(
    const TString& query,
    const TExplainOptions& options)
{
    ValidateActive();
    return Client_->Explain(
        query,
        PatchTransactionTimestamp(options));
}

TFuture<ITableReaderPtr> TTransaction::CreateTableReader(
    const TRichYPath& path,
    const NApi::TTableReaderOptions& options)
{
    ValidateActive();
    return Client_->CreateTableReader(
        path,
        PatchTransactionId(options));
}

TFuture<ITableWriterPtr> TTransaction::CreateTableWriter(
    const TRichYPath& path,
    const NApi::TTableWriterOptions& options)
{
    ValidateActive();
    return Client_->CreateTableWriter(
        path,
        PatchTransactionId(options));
}

TFuture<NYson::TYsonString> TTransaction::GetNode(
    const TYPath& path,
    const TGetNodeOptions& options)
{
    ValidateActive();
    return Client_->GetNode(
        path,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::SetNode(
    const TYPath& path,
    const NYson::TYsonString& value,
    const TSetNodeOptions& options)
{
    ValidateActive();
    return Client_->SetNode(
        path,
        value,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::RemoveNode(
    const TYPath& path,
    const TRemoveNodeOptions& options)
{
    ValidateActive();
    return Client_->RemoveNode(
        path,
        PatchTransactionId(options));
}

TFuture<NYson::TYsonString> TTransaction::ListNode(
    const TYPath& path,
    const TListNodeOptions& options)
{
    ValidateActive();
    return Client_->ListNode(
        path,
        PatchTransactionId(options));
}

TFuture<TNodeId> TTransaction::CreateNode(
    const TYPath& path,
    EObjectType type,
    const TCreateNodeOptions& options)
{
    ValidateActive();
    return Client_->CreateNode(
        path,
        type,
        PatchTransactionId(options));
}

TFuture<TLockNodeResult> TTransaction::LockNode(
    const TYPath& path,
    ELockMode mode,
    const TLockNodeOptions& options)
{
    ValidateActive();
    return Client_->LockNode(
        path,
        mode,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::UnlockNode(
    const NYPath::TYPath& path,
    const NApi::TUnlockNodeOptions& options)
{
    ValidateActive();
    return Client_->UnlockNode(
        path,
        PatchTransactionId(options));
}

TFuture<TNodeId> TTransaction::CopyNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TCopyNodeOptions& options)
{
    ValidateActive();
    return Client_->CopyNode(
        srcPath,
        dstPath,
        PatchTransactionId(options));
}

TFuture<TNodeId> TTransaction::MoveNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TMoveNodeOptions& options)
{
    ValidateActive();
    return Client_->MoveNode(
        srcPath,
        dstPath,
        PatchTransactionId(options));
}

TFuture<TNodeId> TTransaction::LinkNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TLinkNodeOptions& options)
{
    ValidateActive();
    return Client_->LinkNode(
        srcPath,
        dstPath,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::ConcatenateNodes(
    const std::vector<TRichYPath>& srcPaths,
    const TRichYPath& dstPath,
    const TConcatenateNodesOptions& options)
{
    ValidateActive();
    return Client_->ConcatenateNodes(
        srcPaths,
        dstPath,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::ExternalizeNode(
    const TYPath& path,
    TCellTag cellTag,
    const TExternalizeNodeOptions& options)
{
    ValidateActive();
    return Client_->ExternalizeNode(
        path,
        cellTag,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::InternalizeNode(
    const TYPath& path,
    const TInternalizeNodeOptions& options)
{
    ValidateActive();
    return Client_->InternalizeNode(
        path,
        PatchTransactionId(options));
}

TFuture<bool> TTransaction::NodeExists(
    const TYPath& path,
    const TNodeExistsOptions& options)
{
    ValidateActive();
    return Client_->NodeExists(
        path,
        PatchTransactionId(options));
}

TFuture<TObjectId> TTransaction::CreateObject(
    EObjectType type,
    const TCreateObjectOptions& options)
{
    ValidateActive();
    return Client_->CreateObject(type, options);
}

TFuture<IFileReaderPtr> TTransaction::CreateFileReader(
    const TYPath& path,
    const TFileReaderOptions& options)
{
    ValidateActive();
    return Client_->CreateFileReader(
        path,
        PatchTransactionId(options));
}

IFileWriterPtr TTransaction::CreateFileWriter(
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    ValidateActive();
    return Client_->CreateFileWriter(
        path,
        PatchTransactionId(options));
}

IJournalReaderPtr TTransaction::CreateJournalReader(
    const TYPath& path,
    const TJournalReaderOptions& options)
{
    ValidateActive();
    return Client_->CreateJournalReader(
        path,
        PatchTransactionId(options));
}

IJournalWriterPtr TTransaction::CreateJournalWriter(
    const TYPath& path,
    const TJournalWriterOptions& options)
{
    ValidateActive();
    return Client_->CreateJournalWriter(
        path,
        PatchTransactionId(options));
}

ETransactionState TTransaction::GetState()
{
    auto guard = Guard(SpinLock_);
    return State_;
}

void TTransaction::FireCommitted()
{
    Committed_.Fire();
}

void TTransaction::FireAborted()
{
    Aborted_.Fire();
}

TError TTransaction::SetCommitted(const NApi::TTransactionCommitResult& result)
{
    {
        auto guard = Guard(SpinLock_);
        if (State_ != ETransactionState::Committing) {
            return Error_;
        }
        State_ = ETransactionState::Committed;
    }

    YT_LOG_DEBUG("Transaction committed (TransactionId: %v, CommitTimestamps: %v)",
        Id_,
        result.CommitTimestamps);

    FireCommitted();

    return TError();
}

bool TTransaction::SetAborted(const TError& error)
{
    {
        auto guard = Guard(SpinLock_);
        if (State_ == ETransactionState::Aborted) {
            return false;
        }
        State_ = ETransactionState::Aborted;
        Error_ = error;
    }

    FireAborted();

    return true;
}

void TTransaction::OnFailure(const TError& error)
{
    // Send abort request just once.
    if (SetAborted(error)) {
        // Best-effort, fire-and-forget.
        SendAbort();
    }
}

TFuture<void> TTransaction::SendAbort()
{
    YT_LOG_DEBUG(Error_, "Aborting transaction (TransactionId: %v)",
        Id_);

    const auto& config = Connection_->GetConfig();

    auto proxy = CreateApiServiceProxy();

    auto req = proxy.AbortTransaction();
    req->SetTimeout(config->RpcTimeout);

    ToProto(req->mutable_transaction_id(), Id_);
    req->set_sticky(Sticky_);

    return req->Invoke().Apply(
        BIND([id = Id_, Logger = Logger] (const TApiServiceProxy::TErrorOrRspAbortTransactionPtr& rspOrError) {
            if (rspOrError.IsOK()) {
                YT_LOG_DEBUG("Transaction aborted (TransactionId: %v)",
                    id);
                return TError();
            } else if (rspOrError.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction) {
                YT_LOG_DEBUG("Transaction has expired or was already aborted, ignored (TransactionId: %v)",
                    id);
                return TError();
            } else {
                YT_LOG_WARNING(rspOrError, "Error aborting transaction (TransactionId: %v)",
                    id);
                return TError("Error aborting transaction %v",
                    id)
                    << rspOrError;
            }
        }));
}

TFuture<void> TTransaction::SendPing()
{
    YT_LOG_DEBUG("Pinging transaction (TransactionId: %v)",
        Id_);

    const auto& config = Connection_->GetConfig();

    auto proxy = CreateApiServiceProxy();

    auto req = proxy.PingTransaction();
    req->SetTimeout(config->RpcTimeout);

    ToProto(req->mutable_transaction_id(), Id_);
    req->set_sticky(Sticky_);

    return req->Invoke().Apply(
        BIND([=, this_ = MakeStrong(this)] (const TApiServiceProxy::TErrorOrRspPingTransactionPtr& rspOrError) {
            if (rspOrError.IsOK()) {
                YT_LOG_DEBUG("Transaction pinged (TransactionId: %v)",
                    Id_);
                return TError();
            } else if (rspOrError.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction) {
                // Hard error.
                YT_LOG_DEBUG("Transaction has expired or was aborted (TransactionId: %v)",
                    Id_);
                auto error = TError(
                    NTransactionClient::EErrorCode::NoSuchTransaction,
                    "Transaction %v has expired or was aborted",
                    Id_);
                if (GetState() == ETransactionState::Active) {
                    OnFailure(error);
                }
                return error;
            } else {
                // Soft error.
                YT_LOG_DEBUG(rspOrError, "Error pinging transaction (TransactionId: %v)",
                    Id_);
                return TError("Failed to ping transaction %v",
                    Id_)
                    << rspOrError;
            }
        }));
}

void TTransaction::RunPeriodicPings()
{
    if (!PingPeriod_) {
        return;
    }

    if (!IsPingableState()) {
        return;
    }

    SendPing().Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
        if (!IsPingableState()) {
            return;
        }

        if (error.FindMatching(NYT::EErrorCode::Timeout)) {
            RunPeriodicPings();
            return;
        }

        YT_LOG_DEBUG("Transaction ping scheduled (TransactionId: %v)",
            Id_);

        TDelayedExecutor::Submit(
            BIND(&TTransaction::RunPeriodicPings, MakeWeak(this)),
            *PingPeriod_);
    }));
}

bool TTransaction::IsPingableState()
{
    auto state = GetState();
    // NB: We have to continue pinging the transaction while committing.
    return state == ETransactionState::Active || state == ETransactionState::Committing;
}

void TTransaction::ValidateActive()
{
    auto guard = Guard(SpinLock_);
    ValidateActive(guard);
}

void TTransaction::ValidateActive(TGuard<TSpinLock>&)
{
    if (State_ != ETransactionState::Active) {
        THROW_ERROR_EXCEPTION("Transaction %v is not active",
            Id_);
    }
}

TApiServiceProxy::TReqBatchModifyRowsPtr TTransaction::CreateBatchModifyRowsRequest()
{
    const auto& config = Connection_->GetConfig();
    auto proxy = CreateApiServiceProxy();
    auto req = proxy.BatchModifyRows();
    ToProto(req->mutable_transaction_id(), Id_);
    req->SetTimeout(config->RpcTimeout);
    return req;
}

TFuture<void> TTransaction::InvokeBatchModifyRowsRequest()
{
    VERIFY_SPINLOCK_AFFINITY(BatchModifyRowsRequestLock_);
    YT_VERIFY(BatchModifyRowsRequest_);
    TApiServiceProxy::TReqBatchModifyRowsPtr batchRequest;
    batchRequest.Swap(BatchModifyRowsRequest_);
    if (batchRequest->part_counts_size() == 0) {
        return VoidFuture;
    }
    YT_LOG_DEBUG("Invoking a batch modify rows request (Subrequests: %v)",
        batchRequest->part_counts_size());
    return batchRequest->Invoke().As<void>();
}

TTransactionStartOptions TTransaction::PatchTransactionId(const TTransactionStartOptions& options)
{
    auto copiedOptions = options;
    copiedOptions.ParentId = Id_;
    return copiedOptions;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

