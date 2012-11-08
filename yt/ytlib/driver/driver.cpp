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
#include <ytlib/ytree/ephemeral_node_factory.h>

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

        LeaderChannel = CreateLeaderChannel(Config->Masters);
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
        REGISTER(TExistsCommand,            "exists",      Null,       Structured, false, false);

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

    virtual TFuture<TDriverResponse> Execute(const TDriverRequest& request) override
    {
        YCHECK(request.InputStream);
        YCHECK(request.OutputStream);

        auto it = Commands.find(request.CommandName);

        if (it == Commands.end()) {
            return MakeFuture(TDriverResponse({
                TError("Unknown command: %s", ~request.CommandName)
            }));
        }

        const auto& entry = it->second;

        try {
            auto context = New<TCommandContext>(
                *this,
                entry.Descriptor,
                request,
                LeaderChannel,
                !Config->ReadFromFollowers || entry.Descriptor.IsVolatile ? LeaderChannel : MasterChannel,
                SchedulerChannel);

            entry.Factory.Run(context)->Execute();
            return context->GetResponse();
        } catch (const std::exception& ex) {
            return MakeFuture(TDriverResponse({
                TError("Uncaught exception") << ex
            }));
        }
    }

    virtual TNullable<TCommandDescriptor> FindCommandDescriptor(const Stroka& commandName) override
    {
        auto it = Commands.find(commandName);
        if (it == Commands.end()) {
            return Null;
        }
        return it->second.Descriptor;
    }

    virtual std::vector<TCommandDescriptor> GetCommandDescriptors() override
    {
        std::vector<TCommandDescriptor> result;
        result.reserve(Commands.size());
        FOREACH (const auto& pair, Commands) {
            result.push_back(pair.second.Descriptor);
        }
        return result;
    }

    virtual IChannelPtr GetMasterChannel() override
    {
        return LeaderChannel;
    }

    virtual IChannelPtr GetSchedulerChannel() override
    {
        return SchedulerChannel;
    }

private:
    TDriverConfigPtr Config;

    IChannelPtr LeaderChannel;
    IChannelPtr MasterChannel;
    IChannelPtr SchedulerChannel;
    IBlockCachePtr BlockCache;

    typedef TCallback< ICommandPtr(const ICommandContextPtr&) > TCommandFactory;

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
            const TDriver& driver,
            const TCommandDescriptor& descriptor,
            const TDriverRequest& request,
            IChannelPtr leaderChannel,
            IChannelPtr masterChannel,
            IChannelPtr schedulerChannel)
            : Descriptor(descriptor)
            , Request(request)
            , DriverConfig(driver.Config)
            , MasterChannel(CreateScopedChannel(MoveRV(masterChannel)))
            , SchedulerChannel(CreateScopedChannel(MoveRV(schedulerChannel)))
            , BlockCache(driver.BlockCache)
            , TransactionManager(New<TTransactionManager>(
                driver.Config->TransactionManager,
                MoveRV(leaderChannel)))
            , ResponsePromise(NewPromise<TDriverResponse>())
        { }

        ~TCommandContext()
        {
            TError error("Command context terminated");
            MasterChannel->Terminate(error);
            SchedulerChannel->Terminate(error);
        }

        virtual TDriverConfigPtr GetConfig() override
        {
            return DriverConfig;
        }

        virtual IChannelPtr GetMasterChannel() override
        {
            return MasterChannel;
        }

        virtual IChannelPtr GetSchedulerChannel() override
        {
            return SchedulerChannel;
        }

        virtual IBlockCachePtr GetBlockCache() override
        {
            return BlockCache;
        }

        virtual TTransactionManagerPtr GetTransactionManager() override
        {
            return TransactionManager;
        }

        virtual const TDriverRequest* GetRequest() override
        {
            return &Request;
        }

        virtual TFuture<TDriverResponse> GetResponse() override
        {
            return ResponsePromise;
        }

        virtual TPromise<TDriverResponse> GetResponsePromise() override
        {
            return ResponsePromise;
        }

        virtual TYsonProducer CreateInputProducer() override
        {
            return CreateProducerForFormat(
                Request.InputFormat,
                Descriptor.InputType,
                Request.InputStream);
        }

        virtual TAutoPtr<IYsonConsumer> CreateOutputConsumer() override
        {
            return CreateConsumerForFormat(
                Request.OutputFormat,
                Descriptor.OutputType,
                Request.OutputStream);
        }

    private:
        const TCommandDescriptor Descriptor;
        const TDriverRequest Request;

        TDriverConfigPtr DriverConfig;
        IChannelPtr MasterChannel;
        IChannelPtr SchedulerChannel;
        IBlockCachePtr BlockCache;
        TTransactionManagerPtr TransactionManager;

        TPromise<TDriverResponse> ResponsePromise;
    };

    template <class TCommand>
    void RegisterCommand(const TCommandDescriptor& descriptor)
    {
        TCommandEntry entry;
        entry.Descriptor = descriptor;
        entry.Factory = BIND([] (const ICommandContextPtr& context) -> ICommandPtr {
            return New<TCommand>(context);
        });
        YCHECK(Commands.insert(MakePair(descriptor.CommandName, entry)).second);
    }
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateDriver(TDriverConfigPtr config)
{
    return New<TDriver>(MoveRV(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
