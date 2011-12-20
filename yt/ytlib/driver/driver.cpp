#include "stdafx.h"
#include "driver.h"
#include "command.h"
#include "cypress_commands.h"
#include "file_commands.h"

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

class TOutputStreamConsumer
    : public TForwardingYsonConsumer
{
public:
    TOutputStreamConsumer(TAutoPtr<TOutputStream> output, TYsonWriter::EFormat format)
        : Output(output)
        , BufferedOutput(~Output)
        , Writer(&BufferedOutput, format)
    {
        ForwardNode(&Writer, ~FromFunctor([=] ()
            {
                BufferedOutput.Write('\n');
            }));
    }

private:
    TAutoPtr<TOutputStream> Output;
    TBufferedOutput BufferedOutput;
    TYsonWriter Writer;

};

////////////////////////////////////////////////////////////////////////////////

class TOwningBufferedInput
    : public TInputStream
{
public:
    TOwningBufferedInput(TAutoPtr<TInputStream> slave)
        : Slave(slave)
        , Buffered(~Slave)
    { }

private:
    // NB: The order is important.
    TAutoPtr<TInputStream> Slave;
    TBufferedInput Buffered;

    virtual size_t DoRead(void* buf, size_t len)
    {
        return Buffered.Read(buf, len);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TOwningBufferedOutput
    : public TOutputStream
{
public:
    TOwningBufferedOutput(TAutoPtr<TOutputStream> slave)
        : Slave(slave)
        , Buffered(~Slave)
    { }

private:
    // NB: The order is important.
    TAutoPtr<TOutputStream> Slave;
    TBufferedOutput Buffered;

    virtual void DoWrite(const void* buf, size_t len)
    {
        Buffered.Write(buf, len);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDriver::TImpl
    : private TNonCopyable
    , public IDriverImpl
{
public:
    TImpl(
        TConfig* config,
        IDriverStreamProvider* streamProvider)
        : Config(config)
        , StreamProvider(streamProvider)
    {
        YASSERT(config != NULL);
        YASSERT(streamProvider != NULL);

        MasterChannel = CreateCellChannel(~config->Masters);

        RegisterCommand("Get", ~New<TGetCommand>(this));
        RegisterCommand("Set", ~New<TSetCommand>(this));

        RegisterCommand("ReadFile", ~New<TReadFileCommand>(this));
        RegisterCommand("WriteFile", ~New<TWriteFileCommand>(this));
    }

    TError Execute(const TYson& request)
    {
        Error = TError();
        try {
            GuardedExecute(request);
        } catch (const std::exception& ex) {
            ReplyError(TError(TError::Fail, ex.what()));
        }
        return Error;
    }

    IChannel::TPtr GetMasterChannel() const
    {
        return MasterChannel;
    }

    virtual void ReplyError(const TError& error)
    {
        YASSERT(!error.IsOK());
        Error = error;
        auto output = StreamProvider->CreateErrorStream();
        TYsonWriter writer(~output, Config->Format);
        BuildYsonFluently(&writer)
            .BeginMap()
                .Item("error").BeginMap()
                    .DoIf(error.GetCode() != TError::Fail, [=] (TFluentMap fluent)
                        {
                            fluent.Item("code").Scalar(error.GetCode());
                        })
                    .Item("message").Scalar(error.GetMessage())
                .EndMap()
            .EndMap();
        output->Write('\n');
    }

    virtual void ReplySuccess(const Stroka& spec, const TYson& yson)
    {
        auto consumer = CreateOutputConsumer(spec);
        TYsonReader reader(~consumer);
        TStringInput input(yson);
        reader.Read(&input);
    }

    virtual TYsonProducer::TPtr CreateInputProducer(const Stroka& spec)
    {
        auto stream = CreateInputStream(spec);
        return FromFunctor([=] (IYsonConsumer* consumer)
            {
                TYsonReader reader(consumer);
                reader.Read(~stream);
            });
    }

    virtual TAutoPtr<TInputStream> CreateInputStream(const Stroka& spec)
    {
        auto stream = StreamProvider->CreateInputStream(spec);
        return new TOwningBufferedInput(stream);
    }

    virtual TAutoPtr<IYsonConsumer> CreateOutputConsumer(const Stroka& spec)
    {
        auto stream = CreateOutputStream(spec);
        return new TOutputStreamConsumer(stream, Config->Format);
    }

    virtual TAutoPtr<TOutputStream> CreateOutputStream(const Stroka& spec)
    {
        auto stream = StreamProvider->CreateOutputStream(spec);
        return new TOwningBufferedOutput(stream);
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
    IDriverStreamProvider* StreamProvider;
    TError Error;
    yhash_map<Stroka, ICommand::TPtr> Commands;
    IChannel::TPtr MasterChannel;
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

TDriver::TDriver(
    TConfig* config,
    IDriverStreamProvider* streamProvider)
    : Impl(new TImpl(config, streamProvider))
{ }

TDriver::~TDriver()
{ }

TError TDriver::Execute(const TYson& request)
{
    return Impl->Execute(request);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
