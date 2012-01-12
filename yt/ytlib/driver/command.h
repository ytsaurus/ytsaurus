#pragma once

#include "common.h"
#include "driver.h"

#include <ytlib/misc/error.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/ytree/yson_reader.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct IDriverImpl
{
    virtual ~IDriverImpl()
    { }

    virtual TDriver::TConfig* GetConfig() const = 0;
    virtual NRpc::IChannel* GetMasterChannel() const = 0;

    virtual NYTree::TYsonProducer::TPtr CreateInputProducer(const Stroka& spec = Stroka()) = 0;
    virtual TAutoPtr<TInputStream> CreateInputStream(const Stroka& spec = Stroka()) = 0;

    virtual TAutoPtr<NYTree::IYsonConsumer> CreateOutputConsumer(const Stroka& spec = Stroka()) = 0;
    virtual TAutoPtr<TOutputStream> CreateOutputStream(const Stroka& spec = Stroka()) = 0;

    virtual void ReplyError(const TError& error) = 0;
    virtual void ReplySuccess(const NYTree::TYson& yson, const Stroka& spec = Stroka()) = 0;

    virtual NChunkClient::IBlockCache* GetBlockCache() = 0;
    virtual NTransactionClient::TTransactionManager* GetTransactionManager() = 0;
    virtual NTransactionServer::TTransactionId GetCurrentTransactionId() = 0;
    virtual NTransactionClient::ITransaction* GetCurrentTransaction(bool required = false) = 0;
    virtual void SetCurrentTransaction(NTransactionClient::ITransaction* transaction) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestBase
    : public TConfigurable
{
    Stroka Do;

    TRequestBase()
    {
        Register("do", Do);
    }

    static IParamAction<const NYTree::INode::TPtr&>::TPtr StreamSpecIsValid;
};

Stroka ToStreamSpec(NYTree::INode::TPtr node);

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
            // TODO: fixme
            typedRequest->Validate();
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

    virtual void DoExecute(TRequest* request) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

