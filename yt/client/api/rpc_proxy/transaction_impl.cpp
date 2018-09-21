#include "transaction_impl.h"
#include "client_impl.h"
#include "helpers.h"
#include "config.h"
#include "private.h"

#include <yt/client/tablet_client/helpers.h>

#include <yt/client/api/transaction.h>

namespace NYT {
namespace NApi {
namespace NRpcProxy {

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
    const TTransactionId& id,
    TTimestamp startTimestamp,
    ETransactionType type,
    EAtomicity atomicity,
    EDurability durability,
    TDuration timeout,
    TNullable<TDuration> pingPeriod,
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
    , ModifyRowsRequestSequenceCounter_(0)
{
    // TODO(babenko): "started" is only correct as long as we do not support attaching to existing transactions
    LOG_DEBUG("Transaction started (TransactionId: %v, Type: %v, StartTimestamp: %llx, Atomicity: %v, "
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

const TTransactionId& TTransaction::GetId() const
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
                Y_UNREACHABLE();
        }
    }

    LOG_DEBUG("Transaction detached (TransactionId: %v)",
        Id_);
}

TFuture<TTransactionPrepareResult> TTransaction::Prepare()
{
    Y_UNIMPLEMENTED();
}

TFuture<TTransactionFlushResult> TTransaction::Flush()
{
    Y_UNIMPLEMENTED();
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
    LOG_DEBUG("Committing transaction (TransactionId: %v)",
        Id_);

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
                break;

            default:
                Y_UNREACHABLE();
        }
    }

    std::vector<TFuture<void>> asyncResults;
    {
        auto guard = Guard(InFlightModifyRowsRequestsLock_);
        asyncResults.reserve(InFlightModifyRowsRequests_.size());
        std::transform(
            InFlightModifyRowsRequests_.begin(),
            InFlightModifyRowsRequests_.end(),
            std::back_inserter(asyncResults),
            [] (decltype(InFlightModifyRowsRequests_)::const_reference pair) {
                return pair.second;
            });
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

            TApiServiceProxy proxy(Channel_);

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
    LOG_DEBUG("Transaction abort requested (TransactionId: %v)",
        Id_);
    SetAborted(TError("Transaction aborted by user request"));

    return SendAbort();
}

void TTransaction::WriteRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TUnversionedRow> rows,
    const TModifyRowsOptions& options)
{
    ValidateTabletTransaction(Id_);

    std::vector<TRowModification> modifications;
    modifications.reserve(rows.Size());
    for (auto row : rows) {
        modifications.push_back({ERowModificationType::Write, row.ToTypeErasedRow()});
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), rows.GetHolder()),
        options);
}

void TTransaction::WriteRows(
    const NYPath::TYPath&,
    NTableClient::TNameTablePtr,
    TSharedRange<NTableClient::TVersionedRow>,
    const NApi::TModifyRowsOptions&)
{
    Y_UNIMPLEMENTED();
}

void TTransaction::DeleteRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TKey> keys,
    const TModifyRowsOptions& options)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(keys.Size());
    for (auto key : keys) {
        modifications.push_back({ERowModificationType::Delete, key.ToTypeErasedRow()});
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), keys.GetHolder()),
        options);
}

void TTransaction::ModifyRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TRowModification> modifications,
    const TModifyRowsOptions& options)
{
    const auto& config = Connection_->GetConfig();

    TApiServiceProxy proxy(Channel_);
    auto req = proxy.ModifyRows();
    req->SetTimeout(config->RpcTimeout);

    auto reqSequenceNumber = ModifyRowsRequestSequenceCounter_++;

    while (true) {
        TFuture<void> readyEvent;
        {
            auto guard = Guard(InFlightModifyRowsRequestsLock_);
            if (InFlightModifyRowsRequestMinimalSequenceNumber_ == std::numeric_limits<size_t>::max() ||
                reqSequenceNumber < InFlightModifyRowsRequestMinimalSequenceNumber_ + MaxInFlightModifyRowsRequestCount)
            {
                break;
            }
            readyEvent = InFlightModifyRowsRequests_[InFlightModifyRowsRequestMinimalSequenceNumber_];
        }

        if (readyEvent) {
            // Sending this request would exceed proxy's window size.
            // Wait until that window has been slid.
            auto result = WaitFor(readyEvent);
            Y_UNUSED(result); // to chunk clang up
        }
    }

    req->set_sequence_number(reqSequenceNumber);

    ToProto(req->mutable_transaction_id(), Id_);
    req->set_path(path);

    req->set_require_sync_replica(options.RequireSyncReplica);
    ToProto(req->mutable_upstream_replica_id(), options.UpstreamReplicaId);

    std::vector<TUnversionedRow> rows;
    rows.reserve(modifications.Size());
    for (const auto& modification : modifications) {
        // TODO(sandello): handle versioned rows
        YCHECK(
            modification.Type == ERowModificationType::Write ||
            modification.Type == ERowModificationType::Delete);
        rows.emplace_back(modification.Row);
        req->add_row_modification_types(static_cast<NProto::ERowModificationType>(modification.Type));
    }

    req->Attachments() = SerializeRowset(
        nameTable,
        MakeRange(rows),
        req->mutable_rowset_descriptor());

    auto asyncResult = req->Invoke().As<void>();

    {
        auto guard = Guard(InFlightModifyRowsRequestsLock_);
        InFlightModifyRowsRequests_.emplace(reqSequenceNumber, asyncResult);
    }

    asyncResult
        .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
            {
                auto guard = Guard(InFlightModifyRowsRequestsLock_);
                InFlightModifyRowsRequests_.erase(reqSequenceNumber);

                InFlightModifyRowsRequestMinimalSequenceNumber_ = std::numeric_limits<size_t>::max();
                for (const auto& pair : InFlightModifyRowsRequests_) {
                    if (pair.first < InFlightModifyRowsRequestMinimalSequenceNumber_)
                    {
                        InFlightModifyRowsRequestMinimalSequenceNumber_ = pair.first;
                    }
                }
            }

            if (!error.IsOK()) {
                OnFailure(error);
            }
        }));
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
    const std::vector<TYPath>& srcPaths,
    const TYPath& dstPath,
    const TConcatenateNodesOptions& options)
{
    ValidateActive();
    return Client_->ConcatenateNodes(
        srcPaths,
        dstPath,
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
    const TYPath& path,
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

    LOG_DEBUG("Transaction committed (TransactionId: %v, CommitTimestamps: %v)",
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
    LOG_DEBUG("Aborting transaction (TransactionId: %v)",
        Id_);

    const auto& config = Connection_->GetConfig();

    TApiServiceProxy proxy(Channel_);

    auto req = proxy.AbortTransaction();
    req->SetTimeout(config->RpcTimeout);

    ToProto(req->mutable_transaction_id(), Id_);
    req->set_sticky(Sticky_);

    return req->Invoke().Apply(
        BIND([id = Id_, Logger = Logger] (const TApiServiceProxy::TErrorOrRspAbortTransactionPtr& rspOrError) {
            if (rspOrError.IsOK()) {
                LOG_DEBUG("Transaction aborted (TransactionId: %v)",
                    id);
                return TError();
            } else if (rspOrError.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction) {
                LOG_DEBUG("Transaction has expired or was already aborted, ignored (TransactionId: %v)",
                    id);
                return TError();
            } else {
                LOG_WARNING(rspOrError, "Error aborting transaction (TransactionId: %v)",
                    id);
                return TError("Error aborting transaction %v",
                    id)
                    << rspOrError;
            }
        }));
}

TFuture<void> TTransaction::SendPing()
{
    LOG_DEBUG("Pinging transaction (TransactionId: %v)",
        Id_);

    const auto& config = Connection_->GetConfig();

    TApiServiceProxy proxy(Channel_);

    auto req = proxy.PingTransaction();
    req->SetTimeout(config->RpcTimeout);

    ToProto(req->mutable_transaction_id(), Id_);
    req->set_sticky(Sticky_);

    return req->Invoke().Apply(
        BIND([=, this_ = MakeStrong(this)] (const TApiServiceProxy::TErrorOrRspPingTransactionPtr& rspOrError) {
            if (rspOrError.IsOK()) {
                LOG_DEBUG("Transaction pinged (TransactionId: %v)",
                    Id_);
                return TError();
            } else if (rspOrError.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction) {
                // Hard error.
                LOG_DEBUG("Transaction has expired or was aborted (TransactionId: %v)",
                    Id_);
                auto error = TError(
                    NTransactionClient::EErrorCode::NoSuchTransaction,
                    "Transaction %v has expired or was aborted",
                    Id_);
                if (GetState() != ETransactionState::Active) {
                    OnFailure(error);
                }
                return error;
            } else {
                // Soft error.
                LOG_DEBUG(rspOrError, "Error pinging transaction (TransactionId: %v)",
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

        LOG_DEBUG("Transaction ping scheduled (TransactionId: %v)",
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

TTransactionStartOptions TTransaction::PatchTransactionId(const TTransactionStartOptions& options)
{
    auto copiedOptions = options;
    copiedOptions.ParentId = Id_;
    return copiedOptions;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT

