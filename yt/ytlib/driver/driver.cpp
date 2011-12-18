#include "stdafx.h"
#include "driver.h"
#include "command.h"
#include "cypress_commands.h"

#include "../ytree/fluent.h"
#include "../ytree/serialize.h"
#include "../ytree/forwarding_yson_events.h"
#include "../ytree/yson_reader.h"
#include "../ytree/ephemeral.h"
#include "../election/cell_channel.h"

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NRpc;
using namespace NElection;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

class TSuccessConsumer
    : public TForwardingYsonConsumer
{
public:
    TSuccessConsumer(IYsonConsumer* responseConsumer, bool box)
        : ResponseConsumer(responseConsumer)
        , Box(box)
    {
        WritePrologue();
        // NB: No need for TPtr here.
        ForwardNode(responseConsumer, ~FromMethod(&TSuccessConsumer::OnFinished, this));
    }

private:
    IYsonConsumer* ResponseConsumer;
    bool Box;

    void WritePrologue()
    {
        if (Box) {
            ResponseConsumer->OnBeginMap();
            ResponseConsumer->OnMapItem("success");
        }
    }

    void WriteEpilogue()
    {
        if (Box) {
            ResponseConsumer->OnEndMap();
        }
    }

    void OnFinished()
    {
        WriteEpilogue();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDriver::TImpl
    : private TNonCopyable
    , public IDriverImpl
{
public:
    TImpl(TConfig* config)
        : Config(config)
    {
        YASSERT(config != NULL);
        CellChannel = CreateCellChannel(~config->Masters);

        RegisterCommand("Get", ~New<TGetCommand>(this));
        RegisterCommand("Set", ~New<TSetCommand>(this));
    }

    TError Execute(
        const TYson& request,
        IYsonConsumer* responseConsumer)
    {
        ResponseConsumer = responseConsumer;
        Error = TError();
        try {
            GuardedExecute(request);
        } catch (const std::exception& ex) {
            ReplyError(TError(TError::Fail, ex.what()));
        }
        ResponseConsumer = NULL;
        return Error;
    }

    IChannel::TPtr GetCellChannel() const
    {
        return CellChannel;
    }

    virtual void ReplyError(const TError& error)
    {
        YASSERT(!error.IsOK());
        Error = error;
        BuildYsonFluently(ResponseConsumer)
            .BeginMap()
                .Item("error").BeginMap()
                    .DoIf(error.GetCode() != TError::Fail, [=] (TFluentMap fluent)
                        {
                            fluent.Item("code").Scalar(error.GetCode());
                        })
                    .Item("message").Scalar(error.GetMessage())
                .EndMap()
            .EndMap();
    }

    virtual TAutoPtr<NYTree::IYsonConsumer> CreateSuccessConsumer()
    {
        return new TSuccessConsumer(ResponseConsumer, Config->BoxSuccess);
    }

    virtual TTransactionId GetTransactionId()
    {
        return ~Transaction == NULL ? NullTransactionId : Transaction->GetId();
    }

    virtual ITransaction::TPtr GetTransaction()
    {
        return Transaction;
    }

    virtual void SetTransaction(ITransaction* transaction)
    {
        Transaction = transaction;
    }

private:
    TConfig::TPtr Config;
    IYsonConsumer* ResponseConsumer;
    TError Error;
    yhash_map<Stroka, ICommand::TPtr> Commands;
    IChannel::TPtr CellChannel;
    ITransaction::TPtr Transaction;

    void RegisterCommand(const Stroka& name, ICommand* command)
    {
        YVERIFY(Commands.insert(MakePair(name, command)).second);
    }

    void GuardedExecute(const TYson& requestYson)
    {
        INode::TPtr requestNode;
        TRequestBase requestBase;
        try {
            requestNode = DeserializeFromYson(requestYson);
            requestBase.Load(~requestNode);
        }
        catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing request\n%s", ex.what());
        }

        auto commandName = requestBase.Do;
        auto commandIt = Commands.find(commandName);
        if (commandIt == Commands.end()) {
            ythrow yexception() << Sprintf("Unknown command %s", ~commandName.Quote());
        }

        auto command = commandIt->second;
        try {
            command->Execute(~requestNode);
        }
        catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error executing request (Command: %s)\n%s",
                ~commandName,
                ex.what());
        }
    }
    
};

TDriver::TDriver(TConfig* config)
    : Impl(new TImpl(config))
{ }

TDriver::~TDriver()
{ }

TError TDriver::Execute(
    const TYson& request,
    IYsonConsumer* responseConsumer)
{
    return Impl->Execute(request, responseConsumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
