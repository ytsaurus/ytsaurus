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
#include <ytlib/ytree/forwarding_yson_consumer.h>
#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/ephemeral.h>

#include <ytlib/rpc/scoped_channel.h>
#include <ytlib/rpc/retrying_channel.h>

#include <ytlib/meta_state/config.h>
#include <ytlib/meta_state/master_channel.h>

#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/scheduler/config.h>
#include <ytlib/scheduler/scheduler_channel.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NRpc;
using namespace NElection;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NScheduler;
using namespace NFormats;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

class TDriver
    : public IDriver
{
public:
    explicit TDriver(TDriverConfigPtr config)
        : Config(config)
    {
        YCHECK(config);

        //LeaderChannel = CreateRetryingChannel(
        //    Config->MasterRetries,
        //    CreateLeaderChannel(Config->Masters));
        LeaderChannel = CreateLeaderChannel(Config->Masters);

        //MasterChannel = CreateRetryingChannel(
        //    Config->MasterRetries,
        //    CreateMasterChannel(Config->Masters));
        MasterChannel = CreateMasterChannel(Config->Masters);

        // TODO(babenko): for now we use the same timeout both for masters and scheduler
        SchedulerChannel = CreateSchedulerChannel(
            Config->Masters->RpcTimeout,
            LeaderChannel);

        BlockCache = CreateClientBlockCache(Config->BlockCache);

        // Register all commands.
#define REGISTER(command, name, inDataType, outDataType, isVolatile, isHeavy) \
        RegisterCommand<command>(TCommandDescriptor(name, EDataType::inDataType, EDataType::outDataType, isVolatile, isHeavy));

        REGISTER(TStartTransactionCommand,  "start_tx",    Null,       Structured, true,  false);
        REGISTER(TRenewTransactionCommand,  "renew_tx",    Null,       Null,       true,  false);
        REGISTER(TCommitTransactionCommand, "commit_tx",   Null,       Null,       true,  false);
        REGISTER(TAbortTransactionCommand,  "abort_tx",    Null,       Null,       true,  false);

        REGISTER(TCreateCommand,            "create",      Null,       Structured, true,  false);
        REGISTER(TRemoveCommand,            "remove",      Null,       Null,       true,  false);
        REGISTER(TSetCommand,               "set",         Structured, Null,       true,  false);
        REGISTER(TGetCommand,               "get",         Null,       Structured, false, false);
        REGISTER(TListCommand,              "list",        Null,       Structured, false, false);
        REGISTER(TLockCommand,              "lock",        Null,       Null,       true,  false);
        REGISTER(TCopyCommand,              "copy",        Null,       Structured, true,  false);
        REGISTER(TMoveCommand,              "move",        Null,       Null,       true,  false);

        REGISTER(TUploadCommand,            "upload",      Binary,     Structured, true,  true );
        REGISTER(TDownloadCommand,          "download",    Null,       Binary,     false, true );

        REGISTER(TWriteCommand,             "write",       Tabular,    Null,       true,  true );
        REGISTER(TReadCommand,              "read",        Null,       Tabular,    false, true );

        REGISTER(TMergeCommand,             "merge",       Null,       Structured, true,  false);
        REGISTER(TEraseCommand,             "erase",       Null,       Structured, true,  false);
        REGISTER(TMapCommand,               "map",         Null,       Structured, true,  false);
        REGISTER(TSortCommand,              "sort",        Null,       Structured, true,  false);
        REGISTER(TReduceCommand,            "reduce",      Null,       Structured, true,  false);
        REGISTER(TMapReduceCommand,         "map_reduce",  Null,       Structured, true,  false);
        REGISTER(TAbortOperationCommand,    "abort_op",    Null,       Null,       true,  false);
#undef REGISTER
    }

    TDriverResponse Execute(const TDriverRequest& request) override
    {
        YCHECK(request.InputStream);
        YCHECK(request.OutputStream);

        auto it = Commands.find(request.CommandName);
        if (it == Commands.end()) {
            TDriverResponse response;
            response.Error = TError("Unknown command %s", ~request.CommandName.Quote());
            return response;
        }

        const auto& entry = it->second;

        TCommandContext context(
            this,
            entry.Descriptor,
            &request,
            LeaderChannel,
            !Config->ReadFromFollowers || entry.Descriptor.IsVolatile ? LeaderChannel : MasterChannel,
            SchedulerChannel);
        auto command = entry.Factory.Run(&context);
        command->Execute();

        return *context.GetResponse();
    }

    TNullable<TCommandDescriptor> FindCommandDescriptor(const Stroka& commandName) override
    {
        auto it = Commands.find(commandName);
        if (it == Commands.end()) {
            return Null;
        }
        return it->second.Descriptor;
    }

    std::vector<TCommandDescriptor> GetCommandDescriptors() override
    {
        std::vector<TCommandDescriptor> result;
        result.reserve(Commands.size());
        FOREACH (const auto& pair, Commands) {
            result.push_back(pair.second.Descriptor);
        }
        return result;
    }

    IChannelPtr GetMasterChannel() override
    {
        return LeaderChannel;
    }

    IChannelPtr GetSchedulerChannel() override
    {
        return SchedulerChannel;
    }

private:
    TDriverConfigPtr Config;

    IChannelPtr LeaderChannel;
    IChannelPtr MasterChannel;
    IChannelPtr SchedulerChannel;
    IBlockCachePtr BlockCache;

    typedef TCallback< TAutoPtr<ICommand>(ICommandContext*) > TCommandFactory;

    struct TCommandEntry
    {
        TCommandDescriptor Descriptor;
        TCommandFactory Factory;
    };

    yhash_map<Stroka, TCommandEntry> Commands;

    class TCommandContext
        : public ICommandContext
    {
    public:
        TCommandContext(
            TDriver* driver,
            const TCommandDescriptor& descriptor,
            const TDriverRequest* request,
            IChannelPtr leaderChannel,
            IChannelPtr masterChannel,
            IChannelPtr schedulerChannel)
            : Driver(driver)
            , Descriptor(descriptor)
            , Request(request)
            , MasterChannel(CreateScopedChannel(masterChannel))
            , SchedulerChannel(CreateScopedChannel(schedulerChannel))
            , TransactionManager(New<TTransactionManager>(
                Driver->Config->TransactionManager,
                leaderChannel))
        { }

        ~TCommandContext()
        {
            TError error("Command context terminated");
            MasterChannel->Terminate(error);
            SchedulerChannel->Terminate(error);
        }

        TDriverConfigPtr GetConfig() override
        {
            return Driver->Config;
        }

        IChannelPtr GetMasterChannel() override
        {
            return MasterChannel;
        }

        IChannelPtr GetSchedulerChannel() override
        {
            return SchedulerChannel;
        }

        IBlockCachePtr GetBlockCache() override
        {
            return Driver->BlockCache;
        }

        TTransactionManagerPtr GetTransactionManager() override
        {
            return TransactionManager;
        }

        const TDriverRequest* GetRequest() override
        {
            return Request;
        }

        TDriverResponse* GetResponse() override
        {
            return &Response;
        }

        TYsonProducer CreateInputProducer() override
        {
            return CreateProducerForFormat(
                Request->InputFormat,
                Descriptor.InputType,
                Request->InputStream);
        }

        TAutoPtr<IYsonConsumer> CreateOutputConsumer() override
        {
            return CreateConsumerForFormat(
                Request->OutputFormat,
                Descriptor.OutputType,
                Request->OutputStream);
        }

    private:
        TDriver* Driver;
        TCommandDescriptor Descriptor;
        const TDriverRequest* Request;

        IChannelPtr MasterChannel;
        IChannelPtr SchedulerChannel;
        TTransactionManagerPtr TransactionManager;

        TDriverResponse Response;

    };


    template <class TCommand>
    void RegisterCommand(const TCommandDescriptor& descriptor)
    {
        TCommandEntry entry;
        entry.Descriptor = descriptor;
        entry.Factory = BIND([] (ICommandContext* context) -> TAutoPtr<ICommand> {
            return new TCommand(context);
        });
        YCHECK(Commands.insert(MakePair(descriptor.CommandName, entry)).second);
    }
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateDriver(TDriverConfigPtr config)
{
    return New<TDriver>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
