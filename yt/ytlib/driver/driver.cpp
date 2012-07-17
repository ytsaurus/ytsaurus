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

#include <rpc/scoped_channel.h>

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
        YASSERT(config);

        MasterChannel = CreateLeaderChannel(config->Masters);

        // TODO(babenko): for now we use the same timeout both for masters and scheduler
        SchedulerChannel = CreateSchedulerChannel(
            config->Masters->RpcTimeout,
            MasterChannel);

        BlockCache = CreateClientBlockCache(config->BlockCache);

        // Register all commands.
#define REGISTER(command, name, inDataType, outDataType, isVolatile, isHeavy) \
        RegisterCommand<command>(TCommandDescriptor(name, EDataType::inDataType, EDataType::outDataType, isVolatile, isHeavy));

        REGISTER(TStartTransactionCommand,  "start_tx",  Null,       Structured, true,  false);
        REGISTER(TRenewTransactionCommand,  "renew_tx",  Null,       Null,       true,  false);
        REGISTER(TCommitTransactionCommand, "commit_tx", Null,       Null,       true,  false);
        REGISTER(TAbortTransactionCommand,  "abort_tx",  Null,       Null,       true,  false);

        REGISTER(TCreateCommand,            "create",    Null,       Structured, true,  false);
        REGISTER(TRemoveCommand,            "remove",    Null,       Null,       true,  false);
        REGISTER(TSetCommand,               "set",       Structured, Null,       true,  false);
        REGISTER(TGetCommand,               "get",       Null,       Structured, false, false);
        REGISTER(TListCommand,              "list",      Null,       Structured, false, false);
        REGISTER(TLockCommand,              "lock",      Null,       Null,       true,  false);
        REGISTER(TCopyCommand,              "copy",      Null,       Structured, true,  false);
        REGISTER(TMoveCommand,              "move",      Null,       Null,       true,  false);

        REGISTER(TUploadCommand,            "upload",    Binary,     Structured, true,  true );
        REGISTER(TDownloadCommand,          "download",  Null,       Binary,     false, true );

        REGISTER(TWriteCommand,             "write",     Tabular,    Null,       true,  true );
        REGISTER(TReadCommand,              "read",      Null,       Tabular,    false, true );

        REGISTER(TMergeCommand,             "merge",     Null,       Structured, true,  false);
        REGISTER(TEraseCommand,             "erase",     Null,       Structured, true,  false);
        REGISTER(TMapCommand,               "map",       Null,       Structured, true,  false);
        REGISTER(TSortCommand,              "sort",      Null,       Structured, true,  false);
        REGISTER(TReduceCommand,            "reduce",    Null,       Structured, true,  false);
        REGISTER(TAbortOperationCommand,    "abort_op",  Null,       Null,       true,  false);
#undef REGISTER
    }

    TDriverResponse Execute(const TDriverRequest& request) OVERRIDE
    {
        YASSERT(request.InputStream);
        YASSERT(request.OutputStream);

        auto it = Commands.find(request.CommandName);
        if (it == Commands.end()) {
            TDriverResponse response;
            response.Error = TError("Unknown command %s", ~request.CommandName.Quote());
            return response;
        }

        const auto& entry = it->second;

        TCommandContext context(this, entry.Descriptor, &request);
        auto command = entry.Factory.Run(&context);
        command->Execute();

        return *context.GetResponse();
    }

    TNullable<TCommandDescriptor> FindCommandDescriptor(const Stroka& commandName) OVERRIDE
    {
        auto it = Commands.find(commandName);
        if (it == Commands.end()) {
            return Null;
        }
        return it->second.Descriptor;
    }

    std::vector<TCommandDescriptor> GetCommandDescriptors() OVERRIDE
    {
        std::vector<TCommandDescriptor> result;
        result.reserve(Commands.size());
        FOREACH (const auto& pair, Commands) {
            result.push_back(pair.second.Descriptor);
        }
        return result;
    }

    IChannelPtr GetMasterChannel() OVERRIDE
    {
        return MasterChannel;
    }

    IChannelPtr GetSchedulerChannel() OVERRIDE
    {
        return SchedulerChannel;
    }

private:
    TDriverConfigPtr Config;

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
            const TDriverRequest* request)
            : Driver(driver)
            , Descriptor(descriptor)
            , Request(request)
            , MasterChannel(CreateScopedChannel(Driver->GetMasterChannel()))
            , SchedulerChannel(CreateScopedChannel(Driver->GetSchedulerChannel()))
            , TransactionManager(New<TTransactionManager>(
                Driver->Config->TransactionManager,
                MasterChannel))
        { }

        ~TCommandContext()
        {
            MasterChannel->Terminate();
            SchedulerChannel->Terminate();
        }

        TDriverConfigPtr GetConfig() OVERRIDE
        {
            return Driver->Config;
        }

        IChannelPtr GetMasterChannel() OVERRIDE
        {
            return MasterChannel;
        }

        IChannelPtr GetSchedulerChannel() OVERRIDE
        {
            return SchedulerChannel;
        }

        IBlockCachePtr GetBlockCache() OVERRIDE
        {
            return Driver->BlockCache;
        }

        TTransactionManagerPtr GetTransactionManager() OVERRIDE
        {
            return TransactionManager;
        }

        const TDriverRequest* GetRequest() OVERRIDE
        {
            return Request;
        }

        TDriverResponse* GetResponse() OVERRIDE
        {
            return &Response;
        }

        TYsonProducer CreateInputProducer() OVERRIDE
        {
            return CreateProducerForFormat(
                Request->InputFormat,
                Descriptor.InputType,
                Request->InputStream);
        }

        TAutoPtr<IYsonConsumer> CreateOutputConsumer() OVERRIDE
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
