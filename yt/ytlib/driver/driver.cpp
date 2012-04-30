#include "stdafx.h"
#include "driver.h"
#include "command.h"
#include "transaction_commands.h"
#include "cypress_commands.h"
#include "file_commands.h"
#include "table_commands.h"
#include "scheduler_commands.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/forwarding_yson_consumer.h>
#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/ephemeral.h>

#include <ytlib/election/leader_channel.h>

#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/scheduler/config.h>
#include <ytlib/scheduler/scheduler_channel.h>

#include <ytlib/job_proxy/config.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NRpc;
using namespace NElection;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

class TOutputStreamConsumer
    : public TForwardingYsonConsumer
{
public:
    TOutputStreamConsumer(TAutoPtr<TOutputStream> output, EYsonFormat format)
        : Output(output)
        , BufferedOutput(~Output)
        , Writer(&BufferedOutput, format)
    {
        ForwardNode(&Writer, BIND([=] () {
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
    , public ICommandHost
{
public:
    TImpl(
        TConfig::TPtr config,
        IDriverStreamProvider* streamProvider)
        : Config(config)
        , StreamProvider(streamProvider)
    {
        YASSERT(config);
        YASSERT(streamProvider);

        MasterChannel = CreateLeaderChannel(config->Masters);

        // TODO(babenko): for now we use the same timeout both for masters and scheduler
        SchedulerChannel = CreateSchedulerChannel(
            config->Masters->RpcTimeout,
            MasterChannel);

        BlockCache = CreateClientBlockCache(~config->BlockCache);

        TransactionManager = New<TTransactionManager>(
            config->TransactionManager,
            MasterChannel);

        RegisterCommand("start_tx", New<TStartTransactionCommand>(this));
        RegisterCommand("commit_tx", New<TCommitTransactionCommand>(this));
        RegisterCommand("abort_tx", New<TAbortTransactionCommand>(this));

        RegisterCommand("get", New<TGetCommand>(this));
        RegisterCommand("set", New<TSetCommand>(this));
        RegisterCommand("remove", New<TRemoveCommand>(this));
        RegisterCommand("list", New<TListCommand>(this));
        RegisterCommand("create", New<TCreateCommand>(this));
        RegisterCommand("lock", New<TLockCommand>(this));

        RegisterCommand("download", New<TDownloadCommand>(this));
        RegisterCommand("upload", New<TUploadCommand>(this));

        RegisterCommand("read", New<TReadCommand>(this));
        RegisterCommand("write", New<TWriteCommand>(this));

        RegisterCommand("map", New<TMapCommand>(this));
        RegisterCommand("merge", New<TMergeCommand>(this));
        RegisterCommand("erase", New<TEraseCommand>(this));
        RegisterCommand("abort_op", New<TAbortOperationCommand>(this));
    }

    TError Execute(INodePtr command)
    {
        Error = TError();
        try {
            DoExecute(command);
        } catch (const std::exception& ex) {
            ReplyError(TError(ex.what()));
        }
        return Error;
    }

    virtual TConfig::TPtr GetConfig() const
    {
        return ~Config;
    }

    IChannelPtr GetMasterChannel() const
    {
        return MasterChannel;
    }

    IChannelPtr GetSchedulerChannel() const
    {
        return SchedulerChannel;
    }

    virtual void ReplyError(const TError& error)
    {
        YASSERT(!error.IsOK());
        YASSERT(Error.IsOK());
        Error = error;
        auto output = StreamProvider->CreateErrorStream();
        TYsonWriter writer(~output, Config->OutputFormat);
        BuildYsonFluently(&writer)
            .BeginMap()
                .DoIf(error.GetCode() != TError::Fail, [=] (TFluentMap fluent) {
                    fluent.Item("code").Scalar(error.GetCode());
                })
                .Item("message").Scalar(error.GetMessage())
            .EndMap();
        output->Write('\n');
    }

    virtual void ReplySuccess(const TYson& yson)
    {
        auto consumer = CreateOutputConsumer();
        ParseYson(yson, ~consumer);
    }

    // Simplified version for unconditional success (yes, it's empty output).
    virtual void ReplySuccess()
    { }


    virtual TYsonProducer CreateInputProducer()
    {
        auto stream = CreateInputStream();
        return BIND([=] (IYsonConsumer* consumer) {
            ParseYson(~stream, consumer);
        });
    }

    virtual TAutoPtr<TInputStream> CreateInputStream()
    {
        auto stream = StreamProvider->CreateInputStream();
        return new TOwningBufferedInput(stream);
    }

    virtual TAutoPtr<IYsonConsumer> CreateOutputConsumer()
    {
        auto stream = CreateOutputStream();
        return new TOutputStreamConsumer(stream, Config->OutputFormat);
    }

    virtual TAutoPtr<TOutputStream> CreateOutputStream()
    {
        auto stream = StreamProvider->CreateOutputStream();
        return new TOwningBufferedOutput(stream);
    }

    virtual IBlockCache::TPtr GetBlockCache()
    {
        return BlockCache;
    }

    virtual TTransactionManager::TPtr GetTransactionManager()
    {
        return TransactionManager;
    }

    virtual TTransactionId GetTransactionId(TTransactedRequestPtr request, bool required)
    {
        if (required && request->TransactionId == NullTransactionId) {
            ythrow yexception() << "No transaction was set";
        }
        return request->TransactionId;
    }

    virtual ITransaction::TPtr GetTransaction(TTransactedRequestPtr request, bool required)
    {
        auto transactionId = GetTransactionId(request, required);
        if (transactionId == NullTransactionId) {
            return NULL;
        }
        return TransactionManager->Attach(transactionId);
    }

private:
    TConfig::TPtr Config;
    IDriverStreamProvider* StreamProvider;
    TError Error;
    yhash_map<Stroka, ICommand::TPtr> Commands;
    IChannelPtr MasterChannel;
    IChannelPtr SchedulerChannel;
    IBlockCache::TPtr BlockCache;
    TTransactionManager::TPtr TransactionManager;

    void RegisterCommand(const Stroka& name, ICommand::TPtr command)
    {
        YVERIFY(Commands.insert(MakePair(name, command)).second);
    }

    void DoExecute(INodePtr requestNode)
    {
        auto request = New<TRequestBase>();
        try {
            request->Load(~requestNode);
        }
        catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing command from node\n%s", ex.what());
        }

        auto commandName = request->Do;
        auto commandIt = Commands.find(commandName);
        if (commandIt == Commands.end()) {
            ythrow yexception() << Sprintf("Unknown command %s", ~commandName.Quote());
        }

        auto command = commandIt->second;
        command->Execute(~requestNode);
    }
    
};

////////////////////////////////////////////////////////////////////////////////

TDriver::TDriver(
    TConfig::TPtr config,
    IDriverStreamProvider* streamProvider)
    : Impl(new TImpl(config, streamProvider))
{ }

TDriver::~TDriver()
{ }

TError TDriver::Execute(INodePtr command)
{
    return Impl->Execute(command);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
