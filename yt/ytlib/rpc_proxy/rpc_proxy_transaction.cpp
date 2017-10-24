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

class TRpcProxyTransaction::TClient
    : public TRpcProxyClientBase
{
public:
    explicit TClient(TRpcProxyTransaction* owner)
        : Owner_(owner)
    { }

protected:
    virtual TRpcProxyConnectionPtr GetRpcProxyConnection() override
    {
        return Owner_->Connection_;
    }

    virtual NRpc::IChannelPtr GetChannel() override
    {
        return Owner_->Channel_;
    }

private:
    TRpcProxyTransaction* Owner_;
};

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
    , Client_(New<TClient>(this))
{
    Connection_->RegisterTransaction(this);
}

TRpcProxyTransaction::~TRpcProxyTransaction()
{
    Connection_->UnregisterTransaction(this);
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
    TApiServiceProxy proxy(Channel_);

    auto req = proxy.PingTransaction();
    ToProto(req->mutable_transaction_id(), TransactionId_);
    req->set_sticky(Sticky_);

    return req->Invoke().As<void>();
}

TFuture<TTransactionCommitResult> TRpcProxyTransaction::Commit(const TTransactionCommitOptions& options)
{
    return Combine(AsyncRequests_).Apply(
        BIND([this, this_ = MakeStrong(this), options] () {
            TApiServiceProxy proxy(Channel_);

            auto req = proxy.CommitTransaction();
            ToProto(req->mutable_transaction_id(), TransactionId_);
            req->set_sticky(Sticky_);

            return req->Invoke().Apply(
                BIND([] (const TErrorOr<TApiServiceProxy::TRspCommitTransactionPtr>& rspOrError) -> TTransactionCommitResult {
                    const auto& result = rspOrError.ValueOrThrow();
                    return TTransactionCommitResult{FromProto<NHiveClient::TTimestampMap>(result->commit_timestamps())};
                }));
        }));
}

TFuture<void> TRpcProxyTransaction::Abort(const TTransactionAbortOptions& options)
{
    TApiServiceProxy proxy(Channel_);

    auto req = proxy.AbortTransaction();
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
    const TModifyRowsOptions& options)
{
    TApiServiceProxy proxy(Channel_);

    auto req = proxy.ModifyRows();
    ToProto(req->mutable_transaction_id(), TransactionId_);
    req->set_path(path);

    req->set_require_sync_replica(options.RequireSyncReplica);
    ToProto(req->mutable_upstream_replica_id(), options.UpstreamReplicaId);

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

TFuture<ITransactionPtr> TRpcProxyTransaction::StartTransaction(
    NTransactionClient::ETransactionType type,
    const TTransactionStartOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.ParentId = GetId();
    return Client_->StartTransaction(type, optionsCopy);
}

TFuture<IUnversionedRowsetPtr> TRpcProxyTransaction::LookupRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const TLookupRowsOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.Timestamp = GetStartTimestamp();
    return Client_->LookupRows(path, nameTable, keys, options);
}

TFuture<IVersionedRowsetPtr>
TRpcProxyTransaction::VersionedLookupRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const TVersionedLookupRowsOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.Timestamp = GetStartTimestamp();
    return Client_->VersionedLookupRows(path, nameTable, keys, optionsCopy);
}

TFuture<TSelectRowsResult> TRpcProxyTransaction::SelectRows(
    const TString& query,
    const TSelectRowsOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.Timestamp = GetStartTimestamp();
    return Client_->SelectRows(query, optionsCopy);
}

TFuture<ISchemalessMultiChunkReaderPtr> TRpcProxyTransaction::CreateTableReader(
    const NYPath::TRichYPath& path,
    const NApi::TTableReaderOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->CreateTableReader(path, optionsCopy);
}

TFuture<ISchemalessWriterPtr> TRpcProxyTransaction::CreateTableWriter(
    const NYPath::TRichYPath& path,
    const NApi::TTableWriterOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->CreateTableWriter(path, optionsCopy);
}

TFuture<NYson::TYsonString> TRpcProxyTransaction::GetNode(
    const NYPath::TYPath& path,
    const TGetNodeOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->GetNode(path, optionsCopy);
}

TFuture<void> TRpcProxyTransaction::SetNode(
    const NYPath::TYPath& path,
    const NYson::TYsonString& value,
    const TSetNodeOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->SetNode(path, value, optionsCopy);
}

TFuture<void> TRpcProxyTransaction::RemoveNode(
    const NYPath::TYPath& path,
    const TRemoveNodeOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->RemoveNode(path, optionsCopy);
}

TFuture<NYson::TYsonString> TRpcProxyTransaction::ListNode(
    const NYPath::TYPath& path,
    const TListNodeOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->ListNode(path, optionsCopy);
}

TFuture<NCypressClient::TNodeId>
TRpcProxyTransaction::CreateNode(
    const NYPath::TYPath& path,
    NObjectClient::EObjectType type,
    const TCreateNodeOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->CreateNode(path, type, optionsCopy);
}

TFuture<TLockNodeResult> TRpcProxyTransaction::LockNode(
    const NYPath::TYPath& path,
    NCypressClient::ELockMode mode,
    const TLockNodeOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->LockNode(path, mode, optionsCopy);
}

TFuture<NCypressClient::TNodeId>
TRpcProxyTransaction::CopyNode(
    const NYPath::TYPath& srcPath,
    const NYPath::TYPath& dstPath,
    const TCopyNodeOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->CopyNode(srcPath, dstPath, optionsCopy);
}

TFuture<NCypressClient::TNodeId>
TRpcProxyTransaction::MoveNode(
    const NYPath::TYPath& srcPath,
    const NYPath::TYPath& dstPath,
    const TMoveNodeOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->MoveNode(srcPath, dstPath, optionsCopy);
}

TFuture<NCypressClient::TNodeId>
TRpcProxyTransaction::LinkNode(
    const NYPath::TYPath& srcPath,
    const NYPath::TYPath& dstPath,
    const TLinkNodeOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->LinkNode(srcPath, dstPath, optionsCopy);
}

TFuture<void>
TRpcProxyTransaction::ConcatenateNodes(
    const std::vector<NYPath::TYPath>& srcPaths,
    const NYPath::TYPath& dstPath,
    const TConcatenateNodesOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->ConcatenateNodes(srcPaths, dstPath, optionsCopy);
}

TFuture<bool> TRpcProxyTransaction::NodeExists(
    const NYPath::TYPath& path,
    const TNodeExistsOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->NodeExists(path, optionsCopy);
}

TFuture<NObjectClient::TObjectId> TRpcProxyTransaction::CreateObject(
    NObjectClient::EObjectType type,
    const TCreateObjectOptions& options)
{
    return Client_->CreateObject(type, options);
}

TFuture<IAsyncZeroCopyInputStreamPtr> TRpcProxyTransaction::CreateFileReader(
    const NYPath::TYPath& path,
    const TFileReaderOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->CreateFileReader(path, optionsCopy);
}

IFileWriterPtr TRpcProxyTransaction::CreateFileWriter(
    const NYPath::TYPath& path,
    const TFileWriterOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->CreateFileWriter(path, optionsCopy);
}

IJournalReaderPtr TRpcProxyTransaction::CreateJournalReader(
    const NYPath::TYPath& path,
    const TJournalReaderOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->CreateJournalReader(path, optionsCopy);
}

IJournalWriterPtr TRpcProxyTransaction::CreateJournalWriter(
    const NYPath::TYPath& path,
    const TJournalWriterOptions& options)
{
    auto optionsCopy = options;
    optionsCopy.TransactionId = GetId();
    return Client_->CreateJournalWriter(path, optionsCopy);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT

