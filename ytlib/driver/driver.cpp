#include "driver.h"
#include "driver.h"
#include "command.h"
#include "config.h"
#include "cypress_commands.h"
#include "etc_commands.h"
#include "file_commands.h"
#include "journal_commands.h"
#include "scheduler_commands.h"
#include "table_commands.h"
#include "transaction_commands.h"

#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/core/yson/null_consumer.h>

#include <yt/core/misc/sync_cache.h>

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
using namespace NHiveClient;
using namespace NTabletClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

TDriverRequest::TDriverRequest()
    : ResponseParametersConsumer(GetNullYsonConsumer())
{ }

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor IDriver::GetCommandDescriptor(const Stroka& commandName) const
{
    auto descriptor = FindCommandDescriptor(commandName);
    YCHECK(descriptor);
    return *descriptor;
}

TCommandDescriptor IDriver::GetCommandDescriptorOrThrow(const Stroka& commandName) const
{
    auto descriptor = FindCommandDescriptor(commandName);
    if (!descriptor) {
        THROW_ERROR_EXCEPTION("Unknown command %Qv", commandName);
    }
    return *descriptor;
}

////////////////////////////////////////////////////////////////////////////////

class TCachedClient
    : public TSyncCacheValueBase<Stroka, TCachedClient>
{
public:
    TCachedClient(
        const Stroka& user,
        INativeClientPtr client)
        : TSyncCacheValueBase(user)
        , Client_(std::move(client))
    { }

    INativeClientPtr GetClient()
    {
        return Client_;
    }

private:
    const INativeClientPtr Client_;
};

class TDriver;
typedef TIntrusivePtr<TDriver> TDriverPtr;

class TDriver
    : public IDriver
    , public TSyncSlruCacheBase<Stroka, TCachedClient>
{
public:
    explicit TDriver(TDriverConfigPtr config)
        : TSyncSlruCacheBase(config->ClientCache)
        , Config(config)
    {
        YCHECK(Config);

        Connection_ = CreateNativeConnection(Config);

        // Register all commands.
#define REGISTER(command, name, inDataType, outDataType, isVolatile, isHeavy) \
        RegisterCommand<command>( \
            TCommandDescriptor{name, EDataType::inDataType, EDataType::outDataType, isVolatile, isHeavy});

        REGISTER(TStartTransactionCommand,     "start_tx",                Null,       Structured, true,  false);
        REGISTER(TPingTransactionCommand,      "ping_tx",                 Null,       Null,       true,  false);
        REGISTER(TCommitTransactionCommand,    "commit_tx",               Null,       Null,       true,  false);
        REGISTER(TAbortTransactionCommand,     "abort_tx",                Null,       Null,       true,  false);
        REGISTER(TGenerateTimestampCommand,    "generate_timestamp",      Null,       Structured, false, false);

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
        REGISTER(TConcatenateCommand,          "concatenate",             Null,       Null,       true,  false);

        REGISTER(TWriteFileCommand,            "write_file",              Binary,     Null,       true,  true );
        REGISTER(TReadFileCommand,             "read_file",               Null,       Binary,     false, true );

        REGISTER(TWriteTableCommand,           "write_table",             Tabular,    Null,       true,  true );
        REGISTER(TReadTableCommand,            "read_table",              Null,       Tabular,    false, true );
        REGISTER(TReadBlobTableCommand,        "read_blob_table",         Null,       Binary,     false, true );

        REGISTER(TInsertRowsCommand,           "insert_rows",             Tabular,    Null,       true,  true );
        REGISTER(TDeleteRowsCommand,           "delete_rows",             Tabular,    Null,       true,  true);
        REGISTER(TTrimRowsCommand,             "trim_rows",               Null,       Null,       true,  true);
        REGISTER(TSelectRowsCommand,           "select_rows",             Null,       Tabular,    false, true );
        REGISTER(TLookupRowsCommand,           "lookup_rows",             Tabular,    Tabular,    false, true );

        REGISTER(TEnableTableReplicaCommand,   "enable_table_replica",    Null,       Null,       true,  false);
        REGISTER(TDisableTableReplicaCommand,  "disable_table_replica",   Null,       Null,       true,  false);
        REGISTER(TAlterTableReplicaCommand,    "alter_table_replica",     Null,       Null,       true,  false);

        REGISTER(TMountTableCommand,           "mount_table",             Null,       Null,       true,  false);
        REGISTER(TUnmountTableCommand,         "unmount_table",           Null,       Null,       true,  false);
        REGISTER(TRemountTableCommand,         "remount_table",           Null,       Null,       true,  false);
        REGISTER(TFreezeTableCommand,          "freeze_table",            Null,       Null,       true,  false);
        REGISTER(TUnfreezeTableCommand,        "unfreeze_table",          Null,       Null,       true,  false);
        REGISTER(TReshardTableCommand,         "reshard_table",           Null,       Null,       true,  false);
        REGISTER(TAlterTableCommand,           "alter_table",             Null,       Null,       true,  false);

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
        REGISTER(TCompleteOperationCommand,    "complete_op",             Null,       Null,       true,  false);

        REGISTER(TParseYPathCommand,           "parse_ypath",             Null,       Structured, false, false);

        REGISTER(TAddMemberCommand,            "add_member",              Null,       Null,       true,  false);
        REGISTER(TRemoveMemberCommand,         "remove_member",           Null,       Null,       true,  false);
        REGISTER(TCheckPermissionCommand,      "check_permission",        Null,       Structured, false, false);

        REGISTER(TWriteJournalCommand,         "write_journal",           Tabular,    Null,       true,  true );
        REGISTER(TReadJournalCommand,          "read_journal",            Null,       Tabular,    false, true );

        REGISTER(TDumpJobContextCommand,       "dump_job_context",        Null,       Null,       true,  false);
        REGISTER(TGetJobInputCommand,          "get_job_input",           Null,       Binary,     false, true);
        REGISTER(TGetJobStderrCommand,         "get_job_stderr",          Null,       Binary,     false, true);
        REGISTER(TListJobsCommand,             "list_jobs",               Null,       Tabular,    false, true);
        REGISTER(TStraceJobCommand,            "strace_job",              Null,       Structured, false, false);
        REGISTER(TSignalJobCommand,            "signal_job",              Null,       Null,       false, false);
        REGISTER(TAbandonJobCommand,           "abandon_job",             Null,       Null,       false, false);
        REGISTER(TPollJobShellCommand,         "poll_job_shell",          Null,       Structured, true,  false);
        REGISTER(TAbortJobCommand,             "abort_job",               Null,       Null,       false, false);

        REGISTER(TGetVersionCommand,           "get_version",             Null,       Structured, false, false);

        REGISTER(TExecuteBatchCommand,         "execute_batch",           Null,       Structured, true,  false);
#undef REGISTER
    }

    virtual TFuture<void> Execute(const TDriverRequest& request) override
    {
        auto it = CommandNameToEntry_.find(request.CommandName);
        if (it == CommandNameToEntry_.end()) {
            return MakeFuture(TError(
                "Unknown command %Qv",
                request.CommandName));
        }

        const auto& entry = it->second;

        YCHECK(entry.Descriptor.InputType == EDataType::Null || request.InputStream);
        YCHECK(entry.Descriptor.OutputType == EDataType::Null || request.OutputStream);

        const auto& user = request.AuthenticatedUser;

        auto cachedClient = Find(user);
        if (!cachedClient) {
            TClientOptions options;
            options.User = user;
            cachedClient = New<TCachedClient>(user, Connection_->CreateNativeClient(options));

            TryInsert(cachedClient, &cachedClient);
        }

        auto context = New<TCommandContext>(
            this,
            entry.Descriptor,
            request,
            cachedClient->GetClient());

        auto invoker = entry.Descriptor.Heavy
            ? Connection_->GetHeavyInvoker()
            : Connection_->GetLightInvoker();

        return BIND(&TDriver::DoExecute, entry.Execute, context)
            .AsyncVia(invoker)
            .Run();
    }

    virtual TNullable<TCommandDescriptor> FindCommandDescriptor(const Stroka& commandName) const override
    {
        auto it = CommandNameToEntry_.find(commandName);
        return it == CommandNameToEntry_.end() ? Null : MakeNullable(it->second.Descriptor);
    }

    virtual const std::vector<TCommandDescriptor> GetCommandDescriptors() const override
    {
        std::vector<TCommandDescriptor> result;
        result.reserve(CommandNameToEntry_.size());
        for (const auto& pair : CommandNameToEntry_) {
            result.push_back(pair.second.Descriptor);
        }
        return result;
    }

    virtual IConnectionPtr GetConnection() override
    {
        return Connection_;
    }

    virtual void Terminate() override
    {
        // TODO(ignat): find and eliminate reference loop.
        // Reset of the connection should be sufficient to release this connection.
        // But there is some reference loop and it does not work.

        // Release the connection with entire thread pools.
        if (Connection_) {
            Connection_->Terminate();
            Connection_.Reset();
        }
    }

private:
    class TCommandContext;
    typedef TIntrusivePtr<TCommandContext> TCommandContextPtr;
    typedef TCallback<void(ICommandContextPtr)> TExecuteCallback;

    const TDriverConfigPtr Config;

    INativeConnectionPtr Connection_;

    struct TCommandEntry
    {
        TCommandDescriptor Descriptor;
        TExecuteCallback Execute;
    };

    yhash<Stroka, TCommandEntry> CommandNameToEntry_;


    template <class TCommand>
    void RegisterCommand(const TCommandDescriptor& descriptor)
    {
        TCommandEntry entry;
        entry.Descriptor = descriptor;
        entry.Execute = BIND([] (ICommandContextPtr context) {
            TCommand command;
            command.Execute(context);
        });
        YCHECK(CommandNameToEntry_.insert(std::make_pair(descriptor.CommandName, entry)).second);
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

        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

    class TCommandContext
        : public ICommandContext
    {
    public:
        TCommandContext(
            TDriverPtr driver,
            const TCommandDescriptor& descriptor,
            const TDriverRequest& request,
            INativeClientPtr client)
            : Driver_(driver)
            , Descriptor_(descriptor)
            , Request_(request)
            , Client_(std::move(client))
        { }

        virtual TDriverConfigPtr GetConfig() override
        {
            return Driver_->Config;
        }

        virtual INativeClientPtr GetClient() override
        {
            return Client_;
        }

        virtual IDriverPtr GetDriver() override
        {
            return Driver_;
        }

        virtual const TDriverRequest& Request() override
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

        virtual TYsonString ConsumeInputValue() override
        {
            YCHECK(Request_.InputStream);
            auto syncInputStream = CreateSyncAdapter(Request_.InputStream);

            auto producer = CreateProducerForFormat(
                GetInputFormat(),
                Descriptor_.InputType,
                syncInputStream.get());

            return ConvertToYsonString(producer);
        }

        virtual void ProduceOutputValue(const TYsonString& yson) override
        {
            YCHECK(Request_.OutputStream);
            auto syncOutputStream = CreateSyncAdapter(Request_.OutputStream);

            TBufferedOutput bufferedOutputStream(syncOutputStream.get());

            auto consumer = CreateConsumerForFormat(
                GetOutputFormat(),
                Descriptor_.OutputType,
                &bufferedOutputStream);

            Serialize(yson, consumer.get());

            consumer->Flush();
        }

    private:
        const TDriverPtr Driver_;
        const TCommandDescriptor Descriptor_;

        const TDriverRequest Request_;

        TNullable<TFormat> InputFormat_;
        TNullable<TFormat> OutputFormat_;

        INativeClientPtr Client_;

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

