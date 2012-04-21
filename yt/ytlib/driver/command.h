#pragma once

#include "common.h"
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
    Stroka Do;

    TRequestBase()
    {
        SetKeepOptions(true);
        Register("do", Do);
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

struct ICommandHost
{
    virtual ~ICommandHost()
    { }

    virtual TDriver::TConfig::TPtr GetConfig() const = 0;
    virtual NRpc::IChannel::TPtr GetMasterChannel() const = 0;
    virtual NRpc::IChannel::TPtr GetSchedulerChannel() const = 0;

    virtual NYTree::TYsonProducer CreateInputProducer() = 0;
    virtual TAutoPtr<TInputStream> CreateInputStream() = 0;

    virtual TAutoPtr<NYTree::IYsonConsumer> CreateOutputConsumer() = 0;
    virtual TAutoPtr<TOutputStream> CreateOutputStream() = 0;

    virtual void ReplyError(const TError& error) = 0;
    virtual void ReplySuccess() = 0;
    virtual void ReplySuccess(const NYTree::TYson& yson) = 0;

    virtual NChunkClient::IBlockCachePtr GetBlockCache() = 0;
    virtual NTransactionClient::TTransactionManager::TPtr GetTransactionManager() = 0;

    virtual NObjectServer::TTransactionId GetTransactionId(TTransactedRequestPtr request, bool required = false) = 0;
    virtual NTransactionClient::ITransaction::TPtr GetTransaction(TTransactedRequestPtr request, bool required = false) = 0;

};

////////////////////////////////////////////////////////////////////////////////

struct ICommand
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<ICommand> TPtr;

    virtual void Execute(NYTree::INodePtr request) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TUntypedCommandBase
    : public ICommand
{
protected:
    ICommandHost* Host;

    explicit TUntypedCommandBase(ICommandHost* host);
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
class TTypedCommandBase
    : public virtual TUntypedCommandBase
{
public:
    explicit TTypedCommandBase(ICommandHost* host)
        : TUntypedCommandBase(host)
    { }

    virtual void Execute(NYTree::INodePtr request)
    {
        auto typedRequest = New<TRequest>();
        try {
            typedRequest->Load(~request);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing command request\n%s", ex.what());
        }
        DoExecute(typedRequest);
    }

protected:
    virtual void DoExecute(TIntrusivePtr<TRequest> request) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

