#include "stdafx.h"
#include "driver.h"
#include "config.h"
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
    TOutputStreamConsumer(TOutputStream* output, EYsonFormat format)
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

class TDriver
    : public IDriver
    , public ICommandHost
{
public:
    TDriver(
        TDriverConfigPtr config,
        IDriverHost* driverHost)
        : Config(config)
        , DriverHost(driverHost)
    {
        YASSERT(config);
        YASSERT(driverHost);

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

    virtual TDriverConfigPtr GetConfig() const
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
        auto output = DriverHost->GetErrorStream();
        TYsonWriter writer(output, Config->OutputFormat);
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
        auto stream = GetInputStream();
        return BIND([=] (IYsonConsumer* consumer) {
            ParseYson(stream, consumer);
        });
    }

    virtual TInputStream* GetInputStream()
    {
        return DriverHost->GetInputStream();
    }

    virtual TAutoPtr<IYsonConsumer> CreateOutputConsumer()
    {
        auto stream = GetOutputStream();
        return new TOutputStreamConsumer(stream, Config->OutputFormat);
    }

    virtual TOutputStream* GetOutputStream()
    {
        return DriverHost->GetOutputStream();
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
    TDriverConfigPtr Config;
    IDriverHost* DriverHost;
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

IDriverPtr CreateDriver(TDriverConfigPtr config, IDriverHost* driverHost)
{
    return New<TDriver>(config, driverHost);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
