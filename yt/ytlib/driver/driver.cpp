#include "stdafx.h"
#include "dispatcher.h"
#include "driver.h"
#include "config.h"
#include "command.h"
#include "transaction_commands.h"
#include "cypress_commands.h"
#include "etc_commands.h"
#include "file_commands.h"
#include "table_commands.h"
#include "scheduler_commands.h"

#include <core/actions/invoker_util.h>

#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/fiber.h>

#include <core/ytree/fluent.h>
#include <core/ytree/forwarding_yson_consumer.h>
#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/null_yson_consumer.h>

#include <core/yson/parser.h>

#include <core/rpc/scoped_channel.h>
#include <core/rpc/retrying_channel.h>
#include <core/rpc/helpers.h>

#include <ytlib/meta_state/config.h>
#include <ytlib/meta_state/master_channel.h>

#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/scheduler/config.h>
#include <ytlib/scheduler/scheduler_channel.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NElection;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NScheduler;
using namespace NFormats;
using namespace NSecurityClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

TDriverRequest::TDriverRequest()
    : InputStream(nullptr)
    , OutputStream(nullptr)
    , ResponseParametersConsumer(GetNullYsonConsumer())
{ }

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor IDriver::GetCommandDescriptor(const Stroka& commandName)
{
    auto descriptor = FindCommandDescriptor(commandName);
    YCHECK(descriptor);
    return descriptor.Get();
}

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

        SchedulerChannel = CreateSchedulerChannel(Config->Scheduler, LeaderChannel);

        BlockCache = CreateClientBlockCache(Config->BlockCache);

        TDispatcher::Get()->Configure(Config);

        // Register all commands.
#define REGISTER(command, name, inDataType, outDataType, isVolatile, isHeavy) \
        RegisterCommand<command>(TCommandDescriptor(name, EDataType::inDataType, EDataType::outDataType, isVolatile, isHeavy));

        REGISTER(TStartTransactionCommand,  "start_tx",          Null,       Structured, true,  false);
        REGISTER(TPingTransactionCommand,   "ping_tx",           Null,       Null,       true,  false);
        REGISTER(TCommitTransactionCommand, "commit_tx",         Null,       Null,       true,  false);
        REGISTER(TAbortTransactionCommand,  "abort_tx",          Null,       Null,       true,  false);

        REGISTER(TCreateCommand,            "create",            Null,       Structured, true,  false);
        REGISTER(TRemoveCommand,            "remove",            Null,       Null,       true,  false);
        REGISTER(TSetCommand,               "set",               Structured, Null,       true,  false);
        REGISTER(TGetCommand,               "get",               Null,       Structured, false, false);
        REGISTER(TListCommand,              "list",              Null,       Structured, false, false);
        REGISTER(TLockCommand,              "lock",              Null,       Structured, true,  false);
        REGISTER(TCopyCommand,              "copy",              Null,       Structured, true,  false);
        REGISTER(TMoveCommand,              "move",              Null,       Null,       true,  false);
        REGISTER(TLinkCommand,              "link",              Null,       Structured, true,  false);
        REGISTER(TExistsCommand,            "exists",            Null,       Structured, false, false);

        REGISTER(TUploadCommand,            "upload",            Binary,     Null,       true,  true );
        REGISTER(TDownloadCommand,          "download",          Null,       Binary,     false, true );

        REGISTER(TWriteCommand,             "write",             Tabular,    Null,       true,  true );
        REGISTER(TReadCommand,              "read",              Null,       Tabular,    false, true );

        REGISTER(TMergeCommand,             "merge",             Null,       Structured, true,  false);
        REGISTER(TEraseCommand,             "erase",             Null,       Structured, true,  false);
        REGISTER(TMapCommand,               "map",               Null,       Structured, true,  false);
        REGISTER(TSortCommand,              "sort",              Null,       Structured, true,  false);
        REGISTER(TReduceCommand,            "reduce",            Null,       Structured, true,  false);
        REGISTER(TMapReduceCommand,         "map_reduce",        Null,       Structured, true,  false);
        REGISTER(TAbortOperationCommand,    "abort_op",          Null,       Null,       true,  false);
        REGISTER(TSuspendOperationCommand,  "suspend_op",        Null,       Null,       true,  false);
        REGISTER(TResumeOperationCommand,   "resume_op",         Null,       Null,       true,  false);

        REGISTER(TParseYPathCommand,        "parse_ypath",       Null,       Structured, false, false);

        REGISTER(TAddMemberCommand,         "add_member",        Null,       Null,       true,  false);
        REGISTER(TRemoveMemberCommand,      "remove_member",     Null,       Null,       true,  false);
        REGISTER(TCheckPersmissionCommand,  "check_permission",  Null,       Structured, false, false);
#undef REGISTER
    }

    virtual TFuture<TDriverResponse> Execute(const TDriverRequest& request) override
    {
        TDriverResponse response;

        auto it = Commands.find(request.CommandName);
        if (it == Commands.end()) {
            return MakePromise(TDriverResponse(TError("Unknown command: %s", ~request.CommandName)));
        }

        LOG_INFO("Command started (Command: %s, User: %s)",
            ~request.CommandName,
            ~ToString(request.AuthenticatedUser));

        const auto& entry = it->second;

        YCHECK(entry.Descriptor.InputType == EDataType::Null || request.InputStream);
        YCHECK(entry.Descriptor.OutputType == EDataType::Null || request.OutputStream);

        auto masterChannel = LeaderChannel;
        if (request.AuthenticatedUser) {
            masterChannel = CreateAuthenticatedChannel(
                masterChannel,
                request.AuthenticatedUser.Get());
        }
        masterChannel = CreateScopedChannel(masterChannel);

        auto schedulerChannel = SchedulerChannel;
        if (request.AuthenticatedUser) {
            schedulerChannel = CreateAuthenticatedChannel(
                schedulerChannel,
                request.AuthenticatedUser.Get());
        }
        schedulerChannel = CreateScopedChannel(schedulerChannel);

        auto transactionManager = New<TTransactionManager>(
            Config->TransactionManager,
            masterChannel);

        // TODO(babenko): ReadFromFollowers is switched off
        auto context = New<TCommandContext>(
            this,
            entry.Descriptor,
            request,
            masterChannel,
            schedulerChannel,
            transactionManager);

        auto command = entry.Factory.Run();

        auto invoker = entry.Descriptor.IsHeavy
            ? TDispatcher::Get()->GetHeavyInvoker()
            : TDispatcher::Get()->GetLightInvoker();

        return BIND([=] () -> TDriverResponse {
            command->Execute(context);

            const auto& request = context->Request();
            const auto& response = context->Response();

            if (response.Error.IsOK()) {
                LOG_INFO("Command completed (Command: %s)", ~request.CommandName);
            } else {
                LOG_INFO(response.Error, "Command failed (Command: %s)", ~request.CommandName);
            }

            transactionManager->AsyncAbortAll();

            WaitFor(context->TerminateChannels());

            return response;
        }).AsyncVia(invoker).Run();
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
private:
    class TCommandContext;
    typedef TIntrusivePtr<TCommandContext> TCommandContextPtr;

    typedef TCallback< ICommandPtr() > TCommandFactory;

    TDriverConfigPtr Config;

    IChannelPtr LeaderChannel;
    IChannelPtr MasterChannel;
    IChannelPtr SchedulerChannel;
    IBlockCachePtr BlockCache;

    struct TCommandEntry
    {
        TCommandDescriptor Descriptor;
        TCommandFactory Factory;
    };

    yhash_map<Stroka, TCommandEntry> Commands;

    template <class TCommand>
    void RegisterCommand(const TCommandDescriptor& descriptor)
    {
        TCommandEntry entry;
        entry.Descriptor = descriptor;
        entry.Factory = BIND([] () -> ICommandPtr {
            return New<TCommand>();
        });
        YCHECK(Commands.insert(std::make_pair(descriptor.CommandName, entry)).second);
    }

    class TCommandContext
        : public ICommandContext
    {
    public:
        TCommandContext(
            TDriver* driver,
            const TCommandDescriptor& descriptor,
            const TDriverRequest request,
            IChannelPtr masterChannel,
            IChannelPtr schedulerChannel,
            TTransactionManagerPtr transactionManager)
            : Driver(driver)
            , Descriptor(descriptor)
            , Request_(request)
            , MasterChannel(std::move(masterChannel))
            , SchedulerChannel(std::move(schedulerChannel))
            , TransactionManager(std::move(transactionManager))
            , SyncInputStream(CreateSyncInputStream(request.InputStream))
            , SyncOutputStream(CreateSyncOutputStream(request.OutputStream))
        { }

        TFuture<void> TerminateChannels()
        {
            LOG_DEBUG("Terminating channels");

            TError error("Command context terminated");
            auto awaiter = New<TParallelAwaiter>(GetSyncInvoker());
            awaiter->Await(MasterChannel->Terminate(error));
            awaiter->Await(SchedulerChannel->Terminate(error));
            return awaiter->Complete();
        }

        virtual TDriverConfigPtr GetConfig() override
        {
            return Driver->Config;
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
            return Driver->BlockCache;
        }

        virtual TTransactionManagerPtr GetTransactionManager() override
        {
            return TransactionManager;
        }

        virtual const TDriverRequest& Request() const override
        {
            return Request_;
        }

        virtual const TDriverResponse& Response() const
        {
            return Response_;
        }

        virtual TDriverResponse& Response()
        {
            return Response_;
        }

        virtual TYsonProducer CreateInputProducer() override
        {
            return CreateProducerForFormat(
                GetInputFormat(),
                Descriptor.InputType,
                ~SyncInputStream);
        }

        virtual std::unique_ptr<IYsonConsumer> CreateOutputConsumer() override
        {
            return CreateConsumerForFormat(
                GetOutputFormat(),
                Descriptor.OutputType,
                ~SyncOutputStream);
        }

        virtual const TFormat& GetInputFormat() override
        {
            if (!InputFormat) {
                InputFormat = ConvertTo<TFormat>(Request_.Arguments->GetChild("input_format"));
            }
            return *InputFormat;
        }

        virtual const TFormat& GetOutputFormat() override
        {
            if (!OutputFormat) {
                OutputFormat = ConvertTo<TFormat>(Request_.Arguments->GetChild("output_format"));
            }
            return *OutputFormat;
        }

    private:
        TDriver* Driver;
        TCommandDescriptor Descriptor;

        const TDriverRequest Request_;
        TDriverResponse Response_;

        IChannelPtr MasterChannel;
        IChannelPtr SchedulerChannel;
        TTransactionManagerPtr TransactionManager;

        TNullable<TFormat> InputFormat;
        TNullable<TFormat> OutputFormat;

        std::unique_ptr<TInputStream> SyncInputStream;
        std::unique_ptr<TOutputStream> SyncOutputStream;

    };
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateDriver(TDriverConfigPtr config)
{
    return New<TDriver>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
