#pragma once

#include "private.h"
#include "public.h"
#include "driver.h"

#include <ytlib/misc/error.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/ytree/public.h>
#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/chunk_client/public.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TRequestBase
    : public TConfigurable
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
    NObjectServer::TTransactionId TransactionId;

    TTransactedRequest()
    {
        Register("transaction_id", TransactionId)
            .Default(NObjectServer::NullTransactionId);
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
    virtual NTransactionClient::TTransactionManager::TPtr GetTransactionManager() = 0;

    virtual const TDriverRequest* GetRequest() = 0;
    virtual TDriverResponse* GetResponse() = 0;

    virtual NYTree::TYsonProducer CreateInputProducer() = 0;
    virtual TAutoPtr<NYTree::IYsonConsumer> CreateOutputConsumer() = 0;

    //virtual void ReplyError(const TError& error) = 0;
    //virtual void ReplySuccess() = 0;
    //virtual void ReplySuccess(const NYTree::TYson& yson) = 0;
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
    void ReplySuccess(const NYTree::TYson& yson);
    void ReplySuccess();

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
        YASSERT(Replied);
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
        if (required && this->Request->TransactionId == NTransactionClient::NullTransactionId) {
            ythrow yexception() << "Transaction is required";
        }
        return this->Request->TransactionId;
    }

    NTransactionClient::ITransaction::TPtr GetTransaction(bool required)
    {
        auto transactionId = GetTransactionId(required);
        if (transactionId == NTransactionClient::NullTransactionId) {
            return NULL;
        }
        return this->Context->GetTransactionManager()->Attach(transactionId);
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

