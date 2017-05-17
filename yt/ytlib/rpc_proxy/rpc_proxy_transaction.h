#pragma once

#include "rpc_proxy_client_base.h"

#include <yt/ytlib/api/transaction.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyTransaction
    : public NApi::ITransaction
    , protected TRpcProxyClientBase
{
public:
    TRpcProxyTransaction(
        TRpcProxyConnectionPtr connection,
        NRpc::IChannelPtr channel,
        NTransactionClient::TTransactionId transactionId,
        NTransactionClient::TTimestamp startTimestamp,
        bool sticky);
    ~TRpcProxyTransaction();

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

    virtual void SubscribeCommitted(const TClosure& callback) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual void UnsubscribeCommitted(const TClosure& callback) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual void SubscribeAborted(const TClosure& callback) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual void UnsubscribeAborted(const TClosure& callback) override
    {
        Y_UNIMPLEMENTED();
    }


    virtual void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const NApi::TModifyRowsOptions& options) override;

    virtual void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TVersionedRow> rows,
        const NApi::TModifyRowsOptions& options) override
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

private:
    const TRpcProxyConnectionPtr Connection_;
    const NRpc::IChannelPtr Channel_;

    const NTransactionClient::TTransactionId TransactionId_;
    const NTransactionClient::TTimestamp StartTimestamp_;
    const bool Sticky_;

    std::vector<TFuture<void>> AsyncRequests_;

    virtual TRpcProxyConnectionPtr GetRpcProxyConnection() override;
    virtual NRpc::IChannelPtr GetChannel() override;
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyTransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT

