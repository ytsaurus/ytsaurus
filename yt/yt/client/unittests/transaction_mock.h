#pragma once

#include <yt/client/api/file_writer.h>
#include <yt/client/api/journal_reader.h>
#include <yt/client/api/journal_writer.h>
#include <yt/client/api/transaction.h>

#include <yt/core/test_framework/framework.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

class TMockTransaction
    : public ITransaction
{
public:
    // IClientBase
    IConnectionPtr Connection;

    virtual IConnectionPtr GetConnection() override
    {
        return Connection;
    }

    MOCK_METHOD2(StartTransaction, TFuture<ITransactionPtr>(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options));

    MOCK_METHOD4(LookupRows, TFuture<IUnversionedRowsetPtr>(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TLookupRowsOptions& options));

    MOCK_METHOD4(VersionedLookupRows, TFuture<IVersionedRowsetPtr>(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TVersionedLookupRowsOptions& options));

    MOCK_METHOD2(SelectRows, TFuture<TSelectRowsResult>(
        const TString& query,
        const TSelectRowsOptions& options));

    MOCK_METHOD2(ExplainQuery, TFuture<NYson::TYsonString>(
        const TString& query,
        const TExplainQueryOptions& options));

    MOCK_METHOD2(CreateTableReader, TFuture<ITableReaderPtr>(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options));

    MOCK_METHOD2(CreateTableWriter, TFuture<ITableWriterPtr>(
        const NYPath::TRichYPath& path,
        const TTableWriterOptions& options));

    MOCK_METHOD2(GetNode, TFuture<NYson::TYsonString>(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options));

    MOCK_METHOD3(SetNode, TFuture<void>(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options));

    MOCK_METHOD2(RemoveNode, TFuture<void>(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options));

    MOCK_METHOD2(ListNode, TFuture<NYson::TYsonString>(
        const NYPath::TYPath& path,
        const TListNodeOptions& options));

    MOCK_METHOD3(CreateNode, TFuture<NCypressClient::TNodeId>(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options));

    MOCK_METHOD3(LockNode, TFuture<TLockNodeResult>(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options));

    MOCK_METHOD2(UnlockNode, TFuture<void>(
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options));

    MOCK_METHOD3(CopyNode, TFuture<NCypressClient::TNodeId>(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options));

    MOCK_METHOD3(MoveNode, TFuture<NCypressClient::TNodeId>(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options));

    MOCK_METHOD3(LinkNode, TFuture<NCypressClient::TNodeId>(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options));

    MOCK_METHOD3(ConcatenateNodes, TFuture<void>(
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const TConcatenateNodesOptions& options));

    MOCK_METHOD3(ExternalizeNode, TFuture<void>(
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options));

    MOCK_METHOD2(InternalizeNode, TFuture<void>(
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options));

    MOCK_METHOD2(NodeExists, TFuture<bool>(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options));

    MOCK_METHOD2(CreateObject, TFuture<NObjectClient::TObjectId>(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options));

    MOCK_METHOD2(CreateFileReader, TFuture<IFileReaderPtr>(
        const NYPath::TYPath& path,
        const TFileReaderOptions& options));

    MOCK_METHOD2(CreateFileWriter, IFileWriterPtr(
        const NYPath::TRichYPath& path,
        const TFileWriterOptions& options));

    MOCK_METHOD2(CreateJournalReader, IJournalReaderPtr(
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options));

    MOCK_METHOD2(CreateJournalWriter, IJournalWriterPtr(
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options));

    // ITransaction
    IClientPtr Client;
    NTransactionClient::ETransactionType Type;
    NTransactionClient::TTransactionId Id;
    NTransactionClient::TTimestamp StartTimestamp;
    NTransactionClient::EAtomicity Atomicity;
    NTransactionClient::EDurability Durability;
    TDuration Timeout;

    virtual IClientPtr GetClient() const override
    {
        return Client;
    }
    virtual NTransactionClient::ETransactionType GetType() const override
    {
        return Type;
    }
    virtual NTransactionClient::TTransactionId GetId() const override
    {
        return Id;
    }
    virtual NTransactionClient::TTimestamp GetStartTimestamp() const override
    {
        return StartTimestamp;
    }
    virtual NTransactionClient::EAtomicity GetAtomicity() const override
    {
        return Atomicity;
    }
    virtual NTransactionClient::EDurability GetDurability() const override
    {
        return Durability;
    }
    virtual TDuration GetTimeout() const override
    {
        return Timeout;
    }

    MOCK_METHOD1(Ping, TFuture<void>(const NApi::TTransactionPingOptions& options));
    MOCK_METHOD1(Commit, TFuture<TTransactionCommitResult>(const TTransactionCommitOptions& options));
    MOCK_METHOD1(Abort, TFuture<void>(const TTransactionAbortOptions& options));
    MOCK_METHOD0(Detach, void());
    MOCK_METHOD0(Flush, TFuture<TTransactionFlushResult>());

    MOCK_METHOD1(RegisterAlienTransaction, void(const NApi::ITransactionPtr& transaction));

    MOCK_METHOD1(SubscribeCommitted, void(const TCallback<void()>& callback));
    MOCK_METHOD1(UnsubscribeCommitted, void(const TCallback<void()>& callback));
    MOCK_METHOD1(SubscribeAborted, void(const TCallback<void()>& callback));
    MOCK_METHOD1(UnsubscribeAborted, void(const TCallback<void()>& callback));

    MOCK_METHOD4(ModifyRows, void(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications,
        const TModifyRowsOptions& options));
};

DEFINE_REFCOUNTED_TYPE(TMockTransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
