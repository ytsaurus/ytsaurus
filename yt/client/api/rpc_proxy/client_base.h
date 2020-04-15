#pragma once

#include "helpers.h"
#include "connection_impl.h"
#include "api_service_proxy.h"

#include <yt/client/api/client.h>

#include <yt/client/driver/private.h>

#include <yt/core/rpc/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct TRpcProxyClientBufferTag
{ };

class TClientBase
    : public virtual NApi::IClientBase
{
protected:
    // Must return a bound RPC proxy connection for this interface.
    virtual TConnectionPtr GetRpcProxyConnection() = 0;
    // Must return a bound RPC proxy client for this interface.
    virtual TClientPtr GetRpcProxyClient() = 0;
    // Must return an RPC channel to use for API call.
    virtual NRpc::IChannelPtr GetChannel() = 0;
    // Must return an RPC channel to use for API calls within single transaction.
    virtual NRpc::IChannelPtr GetStickyChannel() = 0;

    virtual TApiServiceProxy CreateApiServiceProxy(
        NRpc::IChannelPtr channel = {});
    virtual void InitStreamingRequest(
        NRpc::TClientRequest& request);
    friend class TTransaction;

public:
    virtual NApi::IConnectionPtr GetConnection() override;

    // Transactions
    virtual TFuture<NApi::ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const NApi::TTransactionStartOptions& options) override;

    // Tables
    virtual TFuture<NApi::IUnversionedRowsetPtr> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const NApi::TLookupRowsOptions& options) override;

    virtual TFuture<NApi::IVersionedRowsetPtr> VersionedLookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const NApi::TVersionedLookupRowsOptions& options) override;

    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        const TString& query,
        const NApi::TSelectRowsOptions& options) override;

    virtual TFuture<NYson::TYsonString> ExplainQuery(
        const TString& query,
        const NApi::TExplainQueryOptions& options) override;

    // TODO(babenko): batch read and batch write

    // Cypress
    virtual TFuture<bool> NodeExists(
        const NYPath::TYPath& path,
        const NApi::TNodeExistsOptions& options) override;

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

    virtual TFuture<void> UnlockNode(
        const NYPath::TYPath& path,
        const NApi::TUnlockNodeOptions& options) override;

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
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const NApi::TConcatenateNodesOptions& options) override;

    virtual TFuture<void> ExternalizeNode(
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options = {}) override;

    virtual TFuture<void> InternalizeNode(
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options = {}) override;

    // Objects
    virtual TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const NApi::TCreateObjectOptions& options) override;

    //! NB: Readers and writers returned by methods below are NOT thread-safe.
    // Files
    virtual TFuture<NApi::IFileReaderPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const NApi::TFileReaderOptions& options) override;

    virtual NApi::IFileWriterPtr CreateFileWriter(
        const NYPath::TRichYPath& path,
        const NApi::TFileWriterOptions& options) override;

    // Journals
    virtual NApi::IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const NApi::TJournalReaderOptions& options) override;

    virtual NApi::IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const NApi::TJournalWriterOptions& options) override;

    // Tables
    virtual TFuture<NApi::ITableReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const NApi::TTableReaderOptions& options) override;

    virtual TFuture<NApi::ITableWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const NApi::TTableWriterOptions& options) override;
};

DEFINE_REFCOUNTED_TYPE(TClientBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
