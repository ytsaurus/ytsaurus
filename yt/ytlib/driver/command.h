#pragma once

#include "common.h"

#include "../misc/error.h"
#include "../misc/configurable.h"
#include "../ytree/ytree.h"
#include "../ytree/yson_events.h"
#include "../ytree/yson_reader.h"
#include "../ytree/yson_writer.h"
#include "../ytree/fluent.h"
#include "../rpc/channel.h"
#include "../transaction_client/transaction.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct IDriverImpl
{
    ~IDriverImpl()
    { }

    virtual NRpc::IChannel::TPtr GetCellChannel() const = 0;
    virtual void ReplyError(const TError& error) = 0;
    virtual TAutoPtr<NYTree::IYsonConsumer> CreateSuccessConsumer() = 0;

    virtual NTransactionClient::TTransactionId GetTransactionId() = 0;
    virtual NTransactionClient::ITransaction::TPtr GetTransaction() = 0;
    virtual void SetTransaction(NTransactionClient::ITransaction* transaction) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestBase
    : TConfigurable
{
    Stroka Do;

    TRequestBase()
    {
        Register("do", Do);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct ICommand
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<ICommand> TPtr;

    virtual void Execute(NYTree::INode* request) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
class TCommandBase
    : public ICommand
{
public:
    virtual void Execute(NYTree::INode* request)
    {
        auto typedRequest = New<TRequest>();
        try {
            typedRequest->Load(request);
        }
        catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing request\n%s", ex.what());
        }
        DoExecute(~typedRequest);
    }

protected:
    IDriverImpl* DriverImpl;

    TCommandBase(IDriverImpl* driverImpl)
        : DriverImpl(driverImpl)
    { }

    void ReplyError(const TError& error)
    {
        DriverImpl->ReplyError(error);
    }

    void ReplySuccess(const NYTree::TYson& yson)
    {
        auto consumer = DriverImpl->CreateSuccessConsumer();
        NYTree::TYsonReader reader(~consumer);
        TStringInput input(yson);
        reader.Read(&input);
    }

    void ReplySuccess()
    {
        NYTree::BuildYsonFluently(~CreateSuccessConsumer())
            .Entity();
    }

    TAutoPtr<NYTree::IYsonConsumer> CreateSuccessConsumer()
    {
        return DriverImpl->CreateSuccessConsumer();
    }

    virtual void DoExecute(TRequest* request) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

