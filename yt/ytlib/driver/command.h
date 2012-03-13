#pragma once

#include "common.h"
#include "driver.h"

#include <ytlib/misc/error.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/ytree/public.h>
#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/ytree/yson_reader.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>

#include <tclap/CmdLine.h>
#include <ytlib/misc/tclap_helpers.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TRequestBase
    : public TConfigurable
{
    Stroka Do;

    TRequestBase()
    {
        Register("do", Do);
    }

    static IParamAction<const NYTree::INodePtr&>::TPtr StreamSpecIsValid;
};

Stroka ToStreamSpec(NYTree::INodePtr node);

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

////////////////////////////////////////////////////////////////////////////////

struct IDriverImpl
{
    virtual ~IDriverImpl()
    { }

    virtual TDriver::TConfig* GetConfig() const = 0;
    virtual NRpc::IChannel* GetMasterChannel() const = 0;

    virtual NYTree::TYsonProducer CreateInputProducer() = 0;
    virtual TAutoPtr<TInputStream> CreateInputStream() = 0;

    virtual TAutoPtr<NYTree::IYsonConsumer> CreateOutputConsumer() = 0;
    virtual TAutoPtr<TOutputStream> CreateOutputStream() = 0;

    virtual void ReplyError(const TError& error) = 0;
    virtual void ReplySuccess() = 0;
    virtual void ReplySuccess(const NYTree::TYson& yson) = 0;

    virtual NChunkClient::IBlockCache* GetBlockCache() = 0;
    virtual NTransactionClient::TTransactionManager* GetTransactionManager() = 0;
    virtual NObjectServer::TTransactionId GetCurrentTransactionId() = 0;

    virtual NObjectServer::TTransactionId GetTransactionId(TTransactedRequest* request) = 0;
    virtual NTransactionClient::ITransaction::TPtr GetTransaction(TTransactedRequest* request, bool required = false) = 0;

    virtual NTransactionClient::ITransaction* GetCurrentTransaction(bool required = false) = 0;
    virtual void SetCurrentTransaction(NTransactionClient::ITransaction* transaction) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ICommand
    : public virtual TRefCounted
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
        typedRequest->SetKeepOptions(true);
        try {
            typedRequest->Load(request);
        } catch (const std::exception& ex) {
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

struct INewCommand
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<INewCommand> TPtr;

    virtual void Execute(const yvector<Stroka>& args) = 0;
};


//TODO(panin): move to cpp
class TNewCommandBase
    : public INewCommand
{
public:
    TNewCommandBase(IDriverImpl* driverImpl)
        : DriverImpl(driverImpl)
    {
        Cmd.Reset(new TCLAP::CmdLine("Command line"));
        ConfigArg.Reset(new TCLAP::ValueArg<std::string>(
            "", "config", "configuration file", false, "", "file_name"));
        OptsArg.Reset(new TCLAP::MultiArg<std::string>(
            "", "opts", "other options", false, "options"));

        Cmd->add(~ConfigArg);
        Cmd->add(~OptsArg);
    }
protected:
    //useful typedefs
    typedef TCLAP::UnlabeledValueArg<Stroka> TFreeStringArg;

    THolder<TCLAP::CmdLine> Cmd;

    //TODO(panin): support config
    THolder<TCLAP::ValueArg<std::string> > ConfigArg;
    THolder<TCLAP::MultiArg<std::string> > OptsArg;

    IDriverImpl* DriverImpl;

    TAutoPtr<NYTree::IAttributeDictionary> GetOpts() const
    {
        auto options = NYTree::CreateEphemeralAttributes();
        FOREACH (auto opts, OptsArg->getValue()) {
            NYTree::TYson yson = Stroka("{") + Stroka(opts) + "}";
            options->MergeFrom(~NYTree::DeserializeFromYson(yson)->AsMap());
        }
        return options;
    }
    virtual void DoExecute(const yvector<Stroka>& args) = 0;

    void Execute(const yvector<Stroka>& args)
    {
        Parse(args);
        DoExecute(args);
    }

private:
    void Parse(const yvector<Stroka>& args)
    {
        std::vector<std::string> stringArgs;
        FOREACH (auto arg, args) {
            stringArgs.push_back(std::string(~arg));
        }
        Cmd->parse(stringArgs);
    }

};

class TTransactedCommand
    : public TNewCommandBase
{
public:
    TTransactedCommand(IDriverImpl* driverImpl)
        : TNewCommandBase(driverImpl)
    {
        TxArg.Reset(new TTxArg(
            "", "tx", "transaction id", false, NObjectServer::NullTransactionId, "guid"));
        Cmd->add(~TxArg);
    }
protected:
    typedef TCLAP::ValueArg<NObjectServer::TTransactionId> TTxArg;
    THolder<TTxArg> TxArg;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

