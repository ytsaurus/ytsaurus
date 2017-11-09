#pragma once

#include "rpc_proxy_client_base.h"

#include <yt/ytlib/api/transaction.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyTransaction
    : public NApi::ITransaction
{
public:
    TRpcProxyTransaction(
        TRpcProxyConnectionPtr connection,
        NRpc::IChannelPtr channel,
        NTransactionClient::TTransactionId transactionId,
        NTransactionClient::TTimestamp startTimestamp,
        bool sticky);
    ~TRpcProxyTransaction();

    // Implementation of ITransaction

    virtual NApi::IConnectionPtr GetConnection() override
    {
        return Connection_;
    }

    virtual NApi::IClientPtr GetClient() const override
    {
        Y_UNIMPLEMENTED();
    }

    virtual NTransactionClient::ETransactionType GetType() const override
    {
        Y_UNIMPLEMENTED();
    }

    virtual const NTransactionClient::TTransactionId& GetId() const override;
    virtual NTransactionClient::TTimestamp GetStartTimestamp() const override;

    virtual NTransactionClient::EAtomicity GetAtomicity() const override
    {
        Y_UNIMPLEMENTED();
    }

    virtual NTransactionClient::EDurability GetDurability() const override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TDuration GetTimeout() const override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> Ping() override;
    virtual TFuture<NApi::TTransactionCommitResult> Commit(const NApi::TTransactionCommitOptions& options) override;
    virtual TFuture<void> Abort(const NApi::TTransactionAbortOptions& options) override;

    virtual void Detach() override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NApi::TTransactionFlushResult> Flush() override
    {
        Y_UNIMPLEMENTED();
    }

    virtual void SubscribeCommitted(const TClosure&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual void UnsubscribeCommitted(const TClosure&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual void SubscribeAborted(const TClosure&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual void UnsubscribeAborted(const TClosure&) override
    {
        Y_UNIMPLEMENTED();
    }


    virtual void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const NApi::TModifyRowsOptions& options) override;

    virtual void WriteRows(
        const NYPath::TYPath&,
        NTableClient::TNameTablePtr,
        TSharedRange<NTableClient::TVersionedRow>,
        const NApi::TModifyRowsOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual void DeleteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TKey> keys,
        const NApi::TModifyRowsOptions& options) override;

    virtual void ModifyRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NApi::TRowModification> modifications,
        const NApi::TModifyRowsOptions& options) override;

    // Implementation of IClientBase.

    virtual TFuture<NApi::ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const NApi::TTransactionStartOptions& options) override;

    virtual TFuture<NApi::IUnversionedRowsetPtr> LookupRows(
        const NYPath::TYPath& path, NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const NApi::TLookupRowsOptions& options) override;

    virtual TFuture<NApi::IVersionedRowsetPtr> VersionedLookupRows(
        const NYPath::TYPath& path, NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const NApi::TVersionedLookupRowsOptions& options) override;

    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        const TString& query,
        const NApi::TSelectRowsOptions& options) override;

    virtual TFuture<NTableClient::ISchemalessMultiChunkReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const NApi::TTableReaderOptions& options) override;

    virtual TFuture<NTableClient::ISchemalessWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const NApi::TTableWriterOptions& options) override;

    virtual TFuture<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const NApi::TGetNodeOptions& options) override;

    virtual TFuture<void> SetNode(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const NApi::TSetNodeOptions& options) override;

    virtual TFuture<void> RemoveNode(
        const NYPath::TYPath& path,
        const NApi::TRemoveNodeOptions& options) override;

    virtual TFuture<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const NApi::TListNodeOptions& options) override;

    virtual TFuture<NCypressClient::TNodeId> CreateNode(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const NApi::TCreateNodeOptions& options) override;

    virtual TFuture<NApi::TLockNodeResult> LockNode(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const NApi::TLockNodeOptions& options) override;

    virtual TFuture<NCypressClient::TNodeId> CopyNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const NApi::TCopyNodeOptions& options) override;

    virtual TFuture<NCypressClient::TNodeId> MoveNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const NApi::TMoveNodeOptions& options) override;

    virtual TFuture<NCypressClient::TNodeId> LinkNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const NApi::TLinkNodeOptions& options) override;

    virtual TFuture<void> ConcatenateNodes(
        const std::vector<NYPath::TYPath>& srcPaths,
        const NYPath::TYPath& dstPath,
        const NApi::TConcatenateNodesOptions& options) override;

    virtual TFuture<bool> NodeExists(
        const NYPath::TYPath& path,
        const NApi::TNodeExistsOptions& options) override;

    virtual TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const NApi::TCreateObjectOptions& options) override;

    virtual TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const NApi::TFileReaderOptions& options) override;

    virtual NApi::IFileWriterPtr CreateFileWriter(
        const NYPath::TYPath& path,
        const NApi::TFileWriterOptions& options) override;

    virtual NApi::IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const NApi::TJournalReaderOptions& options) override;

    virtual NApi::IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const NApi::TJournalWriterOptions& options) override;

private:
    const TRpcProxyConnectionPtr Connection_;
    const NRpc::IChannelPtr Channel_;

    const NTransactionClient::TTransactionId TransactionId_;
    const NTransactionClient::TTimestamp StartTimestamp_;
    const bool Sticky_;

    class TClient;
    const TIntrusivePtr<TClient> Client_;

    std::vector<TFuture<void>> AsyncRequests_;
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyTransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT

