#pragma once

#include "client_base.h"

#include <yt/ytlib/api/transaction.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionState,
    (Active)
    (Aborted)
    (Committing)
    (Committed)
    (Detached)
);

class TTransaction
    : public NApi::ITransaction
{
public:
    TTransaction(
        TConnectionPtr connection,
        TClientPtr client,
        NRpc::IChannelPtr channel,
        const NTransactionClient::TTransactionId& id,
        NTransactionClient::TTimestamp startTimestamp,
        NTransactionClient::ETransactionType type,
        NTransactionClient::EAtomicity atomicity,
        NTransactionClient::EDurability durability,
        TDuration timeout,
        TDuration pingPeriod,
        bool sticky);

    // ITransaction implementation
    virtual NApi::IConnectionPtr GetConnection() override;
    virtual NApi::IClientPtr GetClient() const override;

    virtual NTransactionClient::ETransactionType GetType() const override;
    virtual const NTransactionClient::TTransactionId& GetId() const override;
    virtual NTransactionClient::TTimestamp GetStartTimestamp() const override;
    virtual NTransactionClient::EAtomicity GetAtomicity() const override;
    virtual NTransactionClient::EDurability GetDurability() const override;
    virtual TDuration GetTimeout() const override;

    virtual TFuture<void> Ping() override;
    virtual TFuture<NApi::TTransactionCommitResult> Commit(const NApi::TTransactionCommitOptions& options) override;
    virtual TFuture<void> Abort(const NApi::TTransactionAbortOptions& options) override;
    virtual void Detach() override;
    virtual TFuture<NApi::TTransactionFlushResult> Flush() override;

    virtual void SubscribeCommitted(const TClosure&) override;
    virtual void UnsubscribeCommitted(const TClosure&) override;

    virtual void SubscribeAborted(const TClosure&) override;
    virtual void UnsubscribeAborted(const TClosure&) override;

    virtual void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const NApi::TModifyRowsOptions& options) override;

    virtual void WriteRows(
        const NYPath::TYPath&,
        NTableClient::TNameTablePtr,
        TSharedRange<NTableClient::TVersionedRow>,
        const NApi::TModifyRowsOptions&) override;

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

    // IClientBase implementation
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
    const TConnectionPtr Connection_;
    const TClientPtr Client_;
    const NRpc::IChannelPtr Channel_;
    const NTransactionClient::TTransactionId Id_;
    const NTransactionClient::TTimestamp StartTimestamp_;
    const NTransactionClient::ETransactionType Type_;
    const NTransactionClient::EAtomicity Atomicity_;
    const NTransactionClient::EDurability Durability_;
    const TDuration Timeout_;
    const TDuration PingPeriod_;
    const bool Sticky_;

    TSpinLock SpinLock_;
    TError Error_;
    ETransactionState State_ = ETransactionState::Active;

    std::vector<TFuture<void>> AsyncResults_;

    TSingleShotCallbackList<void()> Committed_;
    TSingleShotCallbackList<void()> Aborted_;

    ETransactionState GetState();

    TFuture<void> SendPing();
    void RunPeriodicPings();
    bool IsPingableState();

    void FireCommitted();
    void FireAborted();

    void SetCommitted(const NApi::TTransactionCommitResult& result);
    void SetAborted(const TError& error);
    void OnFailure(const TError& error);

    TFuture<void> SendAbort();

    void ValidateActive();
    void ValidateActive(TGuard<TSpinLock>&);

    template <class T>
    T PatchTransactionId(const T& options);
    NApi::TTransactionStartOptions PatchTransactionId(
        const NApi::TTransactionStartOptions& options);
    template <class T>
    T PatchTransactionTimestamp(const T& options);
};

DEFINE_REFCOUNTED_TYPE(TTransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT

#define TRANSACTION_IMPL_INL_H_
#include "transaction_impl-inl.h"
#undef TRANSACTION_IMPL_INL_H_

