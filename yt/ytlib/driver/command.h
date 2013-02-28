#pragma once

#include "private.h"
#include "public.h"
#include "driver.h"

#include <ytlib/misc/error.h>

#include <ytlib/ytree/public.h>
#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/yson/yson_consumer.h>
#include <ytlib/yson/yson_parser.h>
#include <ytlib/yson/yson_writer.h>

#include <ytlib/rpc/public.h>
#include <ytlib/rpc/channel.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/security_client/public.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/scheduler/scheduler_proxy.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TRequest
    : public TYsonSerializable
{
    TRequest()
    {
        SetKeepOptions(true);
    }
};

typedef TIntrusivePtr<TRequest> TRequestPtr;

////////////////////////////////////////////////////////////////////////////////

struct TTransactedRequest
    : public TRequest
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
    virtual TAutoPtr<NYson::IYsonConsumer> CreateOutputConsumer() = 0;
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

    THolder<NObjectClient::TObjectServiceProxy> ObjectProxy;
    THolder<NScheduler::TSchedulerServiceProxy> SchedulerProxy;

    bool Replied;

    explicit TUntypedCommandBase(ICommandContext* context);

    void Prepare();

    void ReplyError(const TError& error);
    void ReplySuccess(const NYTree::TYsonString& yson);

};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
class TTypedCommandBase
    : public TUntypedCommandBase
{
public:
    explicit TTypedCommandBase(ICommandContext* context)
        : TUntypedCommandBase(context)
    { }

    virtual void Execute()
    {
        try {
            ParseRequest();
            Prepare();
            DoExecute();
        } catch (const std::exception& ex) {
            ReplyError(ex);
        }
    }

protected:
    TIntrusivePtr<TRequest> Request;

    virtual void DoExecute() = 0;

private:
    void ParseRequest()
    {
        Request = New<TRequest>();
        try {
            auto arguments = Context->GetRequest()->Arguments;
            Request->Load(arguments);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing command arguments") <<
                ex;
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
class TTransactedCommandBase
    : public TTypedCommandBase<TRequest>
{
public:
    explicit TTransactedCommandBase(ICommandContext* context)
        : TTypedCommandBase<TRequest>(context)
    { }

protected:
    NTransactionClient::TTransactionId GetTransactionId(bool required)
    {
        auto transaction = GetTransaction(required);
        return transaction ? transaction->GetId() : NTransactionClient::NullTransactionId;
    }

    NTransactionClient::ITransactionPtr GetTransaction(bool required)
    {
        if (required && this->Request->TransactionId == NTransactionClient::NullTransactionId) {
            THROW_ERROR_EXCEPTION("Transaction is required");
        }

        auto transactionId = this->Request->TransactionId;
        if (transactionId == NTransactionClient::NullTransactionId) {
            return nullptr;
        }

        NTransactionClient::TTransactionAttachOptions options(transactionId);
        options.AutoAbort = false;
        options.Ping = true;
        options.PingAncestors = this->Request->PingAncestorTransactions;
        auto transactionManager = this->Context->GetTransactionManager();
        return transactionManager->Attach(options);
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

