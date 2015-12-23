#include "driver.h"
#include "command.h"
#include "config.h"
#include "cypress_commands.h"
#include "dispatcher.h"
#include "etc_commands.h"
#include "file_commands.h"
#include "journal_commands.h"
#include "scheduler_commands.h"
#include "table_commands.h"
#include "transaction_commands.h"

#include <yt/ytlib/api/connection.h>

#include <yt/ytlib/chunk_client/block_cache.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/tablet_client/table_mount_cache.h>

#include <yt/ytlib/transaction_client/timestamp_provider.h>

#include <yt/core/concurrency/parallel_awaiter.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/common.h>

#include <yt/core/rpc/scoped_channel.h>

#include <yt/core/yson/parser.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/forwarding_yson_consumer.h>
#include <yt/core/ytree/null_yson_consumer.h>

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
using namespace NHydra;
using namespace NHive;
using namespace NTabletClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

TDriverRequest::TDriverRequest()
    : ResponseParametersConsumer(GetNullYsonConsumer())
{ }

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor IDriver::GetCommandDescriptor(const Stroka& commandName)
{
    auto descriptor = FindCommandDescriptor(commandName);
    YCHECK(descriptor);
    return descriptor.Get();
}

////////////////////////////////////////////////////////////////////////////////

class TDriver;
typedef TIntrusivePtr<TDriver> TDriverPtr;

class TDriver
    : public IDriver
{
public:
    explicit TDriver(TDriverConfigPtr config)
        : Config(config)
    {
        YCHECK(Config);

        Connection_ = CreateConnection(Config);

        // Register all commands.
#define REGISTER(command, name, inDataType, outDataType, isVolatile, isHeavy) \
        RegisterCommand<command>( \
            TCommandDescriptor(name, EDataType::inDataType, EDataType::outDataType, isVolatile, isHeavy));

        REGISTER(TStartTransactionCommand,     "start_tx",                Null,       Structured, true,  false);
        REGISTER(TPingTransactionCommand,      "ping_tx",                 Null,       Null,       true,  false);
        REGISTER(TCommitTransactionCommand,    "commit_tx",               Null,       Null,       true,  false);
        REGISTER(TAbortTransactionCommand,     "abort_tx",                Null,       Null,       true,  false);

        REGISTER(TCreateCommand,               "create",                  Null,       Structured, true,  false);
        REGISTER(TRemoveCommand,               "remove",                  Null,       Null,       true,  false);
        REGISTER(TSetCommand,                  "set",                     Structured, Null,       true,  false);
        REGISTER(TGetCommand,                  "get",                     Null,       Structured, false, false);
        REGISTER(TListCommand,                 "list",                    Null,       Structured, false, false);
        REGISTER(TLockCommand,                 "lock",                    Null,       Structured, true,  false);
        REGISTER(TCopyCommand,                 "copy",                    Null,       Structured, true,  false);
        REGISTER(TMoveCommand,                 "move",                    Null,       Structured, true,  false);
        REGISTER(TLinkCommand,                 "link",                    Null,       Structured, true,  false);
        REGISTER(TExistsCommand,               "exists",                  Null,       Structured, false, false);
        REGISTER(TConcatenateCommand,          "concatenate",             Null,       Structured, true,  false);

        REGISTER(TWriteFileCommand,            "write_file",              Binary,     Null,       true,  true );
        REGISTER(TReadFileCommand,             "read_file",               Null,       Binary,     false, true );

        REGISTER(TWriteTableCommand,           "write_table",             Tabular,    Null,       true,  true );
        REGISTER(TReadTableCommand,            "read_table",              Null,       Tabular,    false, true );
        REGISTER(TInsertRowsCommand,           "insert_rows",             Tabular,    Null,       true,  true );
        REGISTER(TDeleteRowsCommand,           "delete_rows",             Tabular,    Null,       true,  true);
        REGISTER(TSelectRowsCommand,           "select_rows",             Null,       Tabular,    false, true );
        REGISTER(TLookupRowsCommand,           "lookup_rows",             Tabular,    Tabular,    false, true );

        REGISTER(TMountTableCommand,           "mount_table",             Null,       Null,       true,  false);
        REGISTER(TUnmountTableCommand,         "unmount_table",           Null,       Null,       true,  false);
        REGISTER(TRemountTableCommand,         "remount_table",           Null,       Null,       true,  false);
        REGISTER(TReshardTableCommand,         "reshard_table",           Null,       Null,       true,  false);

        REGISTER(TMergeCommand,                "merge",                   Null,       Structured, true,  false);
        REGISTER(TEraseCommand,                "erase",                   Null,       Structured, true,  false);
        REGISTER(TMapCommand,                  "map",                     Null,       Structured, true,  false);
        REGISTER(TSortCommand,                 "sort",                    Null,       Structured, true,  false);
        REGISTER(TReduceCommand,               "reduce",                  Null,       Structured, true,  false);
        REGISTER(TJoinReduceCommand,           "join_reduce",             Null,       Structured, true,  false);
        REGISTER(TMapReduceCommand,            "map_reduce",              Null,       Structured, true,  false);
        REGISTER(TRemoteCopyCommand,           "remote_copy",             Null,       Structured, true,  false);
        REGISTER(TAbortOperationCommand,       "abort_op",                Null,       Null,       true,  false);
        REGISTER(TSuspendOperationCommand,     "suspend_op",              Null,       Null,       true,  false);
        REGISTER(TResumeOperationCommand,      "resume_op",               Null,       Null,       true,  false);

        REGISTER(TParseYPathCommand,           "parse_ypath",             Null,       Structured, false, false);

        REGISTER(TAddMemberCommand,            "add_member",              Null,       Null,       true,  false);
        REGISTER(TRemoveMemberCommand,         "remove_member",           Null,       Null,       true,  false);
        REGISTER(TCheckPermissionCommand,      "check_permission",        Null,       Structured, false, false);

        REGISTER(TWriteJournalCommand,         "write_journal",           Tabular,    Null,       true,  true );
        REGISTER(TReadJournalCommand,          "read_journal",            Null,       Tabular,    false, true );

        REGISTER(TDumpJobContextCommand,       "dump_job_context",        Null,       Null,       true,  false);
        REGISTER(TStraceJobCommand,            "strace_job",              Null,       Structured, false, false);
        REGISTER(TSignalJobCommand,            "signal_job",              Null,       Null,       false, false);
        REGISTER(TAbandonJobCommand,           "abandon_job",             Null,       Null,       false, false);

        REGISTER(TGetVersionCommand,           "get_version",             Null,       Structured, false, false);

#undef REGISTER
    }

    virtual TFuture<void> Execute(const TDriverRequest& request) override
    {
        auto it = Commands.find(request.CommandName);
        if (it == Commands.end()) {
            return MakeFuture(TError(
                "Unknown command %Qv",
                request.CommandName));
        }

        LOG_INFO("Command initialized (RequestId: %" PRIx64 ", Command: %v, User: %v)",
            request.Id,
            request.CommandName,
            request.AuthenticatedUser);

        const auto& entry = it->second;

        YCHECK(entry.Descriptor.InputType == EDataType::Null || request.InputStream);
        YCHECK(entry.Descriptor.OutputType == EDataType::Null || request.OutputStream);

        // TODO(babenko): ReadFromFollowers is switched off
        auto context = New<TCommandContext>(
            this,
            entry.Descriptor,
            request);

        auto invoker = entry.Descriptor.IsHeavy
            ? TDispatcher::Get()->GetHeavyInvoker()
            : TDispatcher::Get()->GetLightInvoker();

        return BIND(&TDriver::DoExecute, entry.Execute, context)
            .AsyncVia(invoker)
            .Run();
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
        for (const auto& pair : Commands) {
            result.push_back(pair.second.Descriptor);
        }
        return result;
    }

    virtual IConnectionPtr GetConnection() override
    {
        return Connection_;
    }

private:
    class TCommandContext;
    typedef TIntrusivePtr<TCommandContext> TCommandContextPtr;
    typedef TCallback<void(ICommandContextPtr)> TExecuteCallback;

    TDriverConfigPtr Config;

    IConnectionPtr Connection_;

    struct TCommandEntry
    {
        TCommandDescriptor Descriptor;
        TExecuteCallback Execute;
    };

    yhash_map<Stroka, TCommandEntry> Commands;

    void RegisterCommand(TExecuteCallback executeCallback, const TCommandDescriptor& descriptor)
    {
        TCommandEntry entry;
        entry.Descriptor = descriptor;
        entry.Execute = std::move(executeCallback);
        YCHECK(Commands.insert(std::make_pair(descriptor.CommandName, entry)).second);
    }

    template <class TCommand>
    void RegisterCommand(const TCommandDescriptor& descriptor)
    {
        TCommandEntry entry;
        entry.Descriptor = descriptor;
        entry.Execute = BIND([] (ICommandContextPtr context) {
            TCommand command;
            auto parameters = context->Request().Parameters;
            Deserialize(command, parameters);
            command.Execute(context);
        });
        YCHECK(Commands.insert(std::make_pair(descriptor.CommandName, entry)).second);
    }

    static void DoExecute(TExecuteCallback executeCallback, TCommandContextPtr context)
    {
        const auto& request = context->Request();

        TError result;
        TRACE_CHILD("Driver", request.CommandName) {
            LOG_INFO("Command started (RequestId: %" PRIx64 ", Command: %v, User: %v)",
                request.Id,
                request.CommandName,
                request.AuthenticatedUser);

            try {
                executeCallback.Run(context);
                result = TError();
            } catch (const std::exception& ex) {
                result = TError(ex);
            }
        }

        if (result.IsOK()) {
            LOG_INFO("Command completed (RequestId: %" PRIx64 ", Command: %v, User: %v)",
                request.Id,
                request.CommandName,
                request.AuthenticatedUser);
        } else {
            LOG_INFO(result, "Command failed (RequestId: %" PRIx64 ", Command: %v, User: %v)",
                request.Id,
                request.CommandName,
                request.AuthenticatedUser);
        }

        WaitFor(context->Terminate());

        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

    class TCommandContext
        : public ICommandContext
    {
    public:
        TCommandContext(
            TDriverPtr driver,
            const TCommandDescriptor& descriptor,
            const TDriverRequest& request)
            : Driver_(driver)
            , Descriptor_(descriptor)
            , Request_(request)
        {
            TClientOptions options;
            options.User = Request_.AuthenticatedUser;
            Client_ = Driver_->Connection_->CreateClient(options);
        }

        TFuture<void> Terminate()
        {
            LOG_DEBUG("Terminating client");
            return Client_->Terminate();
        }

        virtual TDriverConfigPtr GetConfig() override
        {
            return Driver_->Config;
        }

        virtual IClientPtr GetClient() override
        {
            return Client_;
        }

        virtual const TDriverRequest& Request() const override
        {
            return Request_;
        }

        virtual const TFormat& GetInputFormat() override
        {
            if (!InputFormat_) {
                InputFormat_ = ConvertTo<TFormat>(Request_.Parameters->GetChild("input_format"));
            }
            return *InputFormat_;
        }

        virtual const TFormat& GetOutputFormat() override
        {
            if (!OutputFormat_) {
                OutputFormat_ = ConvertTo<TFormat>(Request_.Parameters->GetChild("output_format"));
            }
            return *OutputFormat_;
        }

        virtual NYTree::TYsonString ConsumeInputValue() override
        {
            YCHECK(Request_.InputStream);
            auto syncInputStream = CreateSyncAdapter(Request_.InputStream);

            auto producer = CreateProducerForFormat(
                GetInputFormat(),
                Descriptor_.InputType,
                syncInputStream.get());

            return ConvertToYsonString(producer);
        }

        virtual void ProduceOutputValue(const NYTree::TYsonString& yson) override
        {
            YCHECK(Request_.OutputStream);
            auto syncOutputStream = CreateSyncAdapter(Request_.OutputStream);

            TBufferedOutput bufferedOutputStream(syncOutputStream.get());

            auto consumer = CreateConsumerForFormat(
                GetOutputFormat(),
                Descriptor_.OutputType,
                &bufferedOutputStream);
            Consume(yson, consumer.get());
        }

    private:
        const TDriverPtr Driver_;
        const TCommandDescriptor Descriptor_;
        const TDriverRequest Request_;

        TNullable<TFormat> InputFormat_;
        TNullable<TFormat> OutputFormat_;

        IClientPtr Client_;

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

