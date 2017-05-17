#include "rpc_proxy_transaction.h"
#include "helpers.h"
#include "private.h"

#include <yt/ytlib/api/transaction.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

TRpcProxyTransaction::TRpcProxyTransaction(
    TRpcProxyConnectionPtr connection,
    NRpc::IChannelPtr channel,
    NTransactionClient::TTransactionId transactionId,
    NTransactionClient::TTimestamp startTimestamp,
    bool sticky)
    : Connection_(std::move(connection))
    , Channel_(std::move(channel))
    , TransactionId_(transactionId)
    , StartTimestamp_(startTimestamp)
    , Sticky_(sticky)
{
    Connection_->RegisterTransaction(this);
}

TRpcProxyTransaction::~TRpcProxyTransaction()
{
    Connection_->UnregisterTransaction(this);
}

TRpcProxyConnectionPtr TRpcProxyTransaction::GetRpcProxyConnection()
{
    return Connection_;
}

NRpc::IChannelPtr TRpcProxyTransaction::GetChannel()
{
    return Channel_;
}

const NTransactionClient::TTransactionId& TRpcProxyTransaction::GetId() const
{
    return TransactionId_;
}

NTransactionClient::TTimestamp TRpcProxyTransaction::GetStartTimestamp() const
{
    return StartTimestamp_;
}

TFuture<void> TRpcProxyTransaction::Ping()
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.PingTransaction();
    ToProto(req->mutable_transaction_id(), TransactionId_);
    req->set_sticky(Sticky_);

    return req->Invoke().As<void>();
}

TFuture<TTransactionCommitResult> TRpcProxyTransaction::Commit(const TTransactionCommitOptions& options)
{
    return Combine(AsyncRequests_).Apply(
        BIND([this, this_ = MakeStrong(this), options] () {
            TApiServiceProxy proxy(GetChannel());

            auto req = proxy.CommitTransaction();
            ToProto(req->mutable_transaction_id(), TransactionId_);
            req->set_sticky(Sticky_);

            return req->Invoke().Apply(
                BIND([] (const TErrorOr<TApiServiceProxy::TRspCommitTransactionPtr>& rspOrError) -> TTransactionCommitResult {
                    rspOrError.ValueOrThrow();
                    return TTransactionCommitResult{};
                }));
        }));
}

TFuture<void> TRpcProxyTransaction::Abort(const TTransactionAbortOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.PingTransaction();
    ToProto(req->mutable_transaction_id(), TransactionId_);
    req->set_sticky(Sticky_);

    return req->Invoke().As<void>();
}

void TRpcProxyTransaction::WriteRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TUnversionedRow> rows,
    const TModifyRowsOptions& options)
{
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

void TRpcProxyTransaction::DeleteRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TKey> keys,
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

void TRpcProxyTransaction::ModifyRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<TRowModification> modifications,
    const TModifyRowsOptions& options = TModifyRowsOptions())
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.ModifyRows();
    ToProto(req->mutable_transaction_id(), TransactionId_);
    req->set_path(path);

    std::vector<TUnversionedRow> rows;
    rows.reserve(modifications.Size());
    for (auto& modification : modifications) {
        // TODO(sandello): handle versioned rows
        rows.push_back(TUnversionedRow(std::move(modification.Row)));
        req->add_row_modification_types(NProto::ERowModificationType(modification.Type));
    }

    req->Attachments() = SerializeRowset(nameTable, MakeRange(rows), req->mutable_rowset_descriptor());

    AsyncRequests_.push_back(req->Invoke().As<void>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT

