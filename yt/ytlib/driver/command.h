#pragma once

#include "private.h"
#include "public.h"
#include "driver.h"

#include <ytlib/misc/error.h>
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
{
    virtual ~ICommandContext()
    { }

    virtual TDriverConfigPtr GetConfig() = 0;
    virtual NRpc::IChannelPtr GetMasterChannel() = 0;
    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual NChunkClient::IBlockCachePtr GetBlockCache() = 0;
    virtual NTransactionClient::TTransactionManagerPtr GetTransactionManager() = 0;

    virtual const TDriverRequest* GetRequest() = 0;
    virtual TDriverResponse* GetResponse() = 0;

    virtual NYTree::TYsonProducer CreateInputProducer() = 0;
    virtual TAutoPtr<NYTree::IYsonConsumer> CreateOutputConsumer() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ICommand
{
    virtual ~ICommand()
    { }

    virtual void Execute() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TUntypedCommandBase
    : public ICommand
{
protected:
    ICommandContext* Context;
    bool Replied;

    explicit TUntypedCommandBase(ICommandContext* host);

    void ReplyError(const TError& error);
    void ReplySuccess(const NYTree::TYsonString& yson);

};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
class TTypedCommandBase
    : public virtual TUntypedCommandBase
{
public:
    explicit TTypedCommandBase(ICommandContext* context)
        : TUntypedCommandBase(context)
    { }

    virtual void Execute()
    {
        try {
            Request = New<TRequest>();
            try {
                auto arguments = Context->GetRequest()->Arguments;
                Request->Load(arguments);
            } catch (const std::exception& ex) {
                ythrow yexception() << Sprintf("Error parsing command arguments\n%s", ex.what());
            }
            DoExecute();
        } catch (const std::exception& ex) {
            ReplyError(TError(ex.what()));
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
    explicit TTransactedCommandBase(ICommandContext* context)
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
            ythrow yexception() << "Transaction is required";
        }
        auto transactionId = this->Request->TransactionId;
        if (transactionId == NTransactionClient::NullTransactionId) {
            return NULL;
        }
        bool pingAncestorTransactions = this->Request->PingAncestorTransactions;
        return this->Context->GetTransactionManager()->Attach(transactionId, false, pingAncestorTransactions);
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

