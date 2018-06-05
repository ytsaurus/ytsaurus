#pragma once

#include "public.h"
#include "helpers.h"
#include "connection_impl.h"
#include "api_service_proxy.h"

#include <yt/ytlib/api/client.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NRpcProxy {

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

    // Objects
    virtual TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const NApi::TCreateObjectOptions& options) override;

    // Files
    virtual TFuture<NApi::IFileReaderPtr> CreateFileReader(
        const NYPath::TYPath&,
        const NApi::TFileReaderOptions&) override
    {
        ThrowUnimplemented("read_file");
    }

    virtual NApi::IFileWriterPtr CreateFileWriter(
        const NYPath::TYPath&,
        const NApi::TFileWriterOptions&) override
    {
        ThrowUnimplemented("write_file");
    }


    // Journals
    virtual NApi::IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath&,
        const NApi::TJournalReaderOptions&) override
    {
        ThrowUnimplemented("read_journal");
    }

    virtual NApi::IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath&,
        const NApi::TJournalWriterOptions&) override
    {
        ThrowUnimplemented("write_journal");
    }


    // Tables
    virtual TFuture<NTableClient::ISchemalessMultiChunkReaderPtr> CreateTableReader(
        const NYPath::TRichYPath&,
        const NApi::TTableReaderOptions&) override
    {
        ThrowUnimplemented("read_table");
    }

    virtual TFuture<NTableClient::ISchemalessWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath&,
        const NApi::TTableWriterOptions&) override
    {
        ThrowUnimplemented("write_table");
    }
};

DEFINE_REFCOUNTED_TYPE(TClientBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
