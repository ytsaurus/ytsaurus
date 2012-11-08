#pragma once

#include "private.h"
#include "public.h"
#include "driver.h"

#include <ytlib/misc/error.h>
#include <ytlib/misc/ref_counted.h>
#include <ytlib/ytree/public.h>
#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/chunk_client/public.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TRequestBase
    : public TYsonSerializable
{
    TRequestBase()
    {
        SetKeepOptions(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTransactedRequest
    : public TRequestBase
{
    NObjectClient::TTransactionId TransactionId;
    bool PingAncestorTransactions;

    TTransactedRequest()
    {
        Register("transaction_id", TransactionId)
            .Default(NObjectClient::NullTransactionId);
        Register("ping_ancestor_transactions", PingAncestorTransactions)
            .Default(false);
    }
};

typedef TIntrusivePtr<TTransactedRequest> TTransactedRequestPtr;

////////////////////////////////////////////////////////////////////////////////

struct ICommandContext
    : public TRefCounted
    // Please, note that there is no virtual inheritance.
{
    virtual TDriverConfigPtr GetConfig() = 0;
    virtual NRpc::IChannelPtr GetMasterChannel() = 0;
    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual NChunkClient::IBlockCachePtr GetBlockCache() = 0;
    virtual NTransactionClient::TTransactionManagerPtr GetTransactionManager() = 0;

    virtual const TDriverRequest* GetRequest() = 0;
    virtual TFuture<TDriverResponse> GetResponse() = 0;
    virtual TPromise<TDriverResponse> GetResponsePromise() = 0;

    virtual NYTree::TYsonProducer CreateInputProducer() = 0;
    virtual TAutoPtr<NYTree::IYsonConsumer> CreateOutputConsumer() = 0;
};

typedef TIntrusivePtr<ICommandContext> ICommandContextPtr;

////////////////////////////////////////////////////////////////////////////////

struct ICommand
    : public TRefCounted
{
    virtual void Execute() = 0;
};

typedef TIntrusivePtr<ICommand> ICommandPtr;

////////////////////////////////////////////////////////////////////////////////

class TUntypedCommandBase
    : public ICommand
{
protected:
    ICommandContextPtr Context;

    explicit TUntypedCommandBase(const ICommandContextPtr& host);

    void ReplyError(const TError& error);
    void ReplySuccess(const NYTree::TYsonString& yson);

};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
class TTypedCommandBase
    : public virtual TUntypedCommandBase
{
public:
    explicit TTypedCommandBase(const ICommandContextPtr& context)
        : TUntypedCommandBase(context)
    { }

    virtual void Execute()
    {
        try {
            Request = New<TRequest>();
            try {
                Request->Load(Context->GetRequest()->Arguments);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error parsing command arguments") << ex;
            }
            DoExecute();
        } catch (const std::exception& ex) {
            ReplyError(ex);
        }
    }

protected:
    TIntrusivePtr<TRequest> Request;

    virtual void DoExecute() = 0;

};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
class TTransactedCommandBase
    : public TTypedCommandBase<TRequest>
{
public:
    explicit TTransactedCommandBase(const ICommandContextPtr& context)
        : TTypedCommandBase<TRequest>(context)
        , TUntypedCommandBase(context)
    { }

protected:
    NTransactionClient::TTransactionId GetTransactionId(bool required)
    {
        auto transaction = GetTransaction(required);
        if (transaction) {
            return transaction->GetId();
        } else {
            return NTransactionClient::NullTransactionId;
        }
    }

    NTransactionClient::ITransactionPtr GetTransaction(bool required)
    {
        if (required && this->Request->TransactionId == NTransactionClient::NullTransactionId) {
            THROW_ERROR_EXCEPTION("Transaction is required");
        }
        auto transactionId = this->Request->TransactionId;
        if (transactionId == NTransactionClient::NullTransactionId) {
            return NULL;
        }
        bool pingAncestorTransactions = this->Request->PingAncestorTransactions;
        return this->Context->GetTransactionManager()->Attach(
            transactionId,
            false,
            pingAncestorTransactions);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

