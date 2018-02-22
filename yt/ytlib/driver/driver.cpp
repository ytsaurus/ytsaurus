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
#include <yt/ytlib/api/connection.h>

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

TDriverRequest::TDriverRequest(THolderPtr holder)
    : ResponseParametersConsumer(GetNullYsonConsumer())
    , Holder_(std::move(holder))
{ }

void TDriverRequest::Reset()
{
    Holder_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor IDriver::GetCommandDescriptor(const TString& commandName) const
{
    auto descriptor = FindCommandDescriptor(commandName);
    YCHECK(descriptor);
    return *descriptor;
}

TCommandDescriptor IDriver::GetCommandDescriptorOrThrow(const TString& commandName) const
{
    auto descriptor = FindCommandDescriptor(commandName);
    if (!descriptor) {
        THROW_ERROR_EXCEPTION("Unknown command %Qv", commandName);
    }
    return *descriptor;
}

////////////////////////////////////////////////////////////////////////////////

class TCachedClient
    : public TSyncCacheValueBase<TString, TCachedClient>
{
public:
    TCachedClient(
        const TString& user,
        IClientPtr client)
        : TSyncCacheValueBase(user)
        , Client_(std::move(client))
    { }

    const IClientPtr& GetClient()
    {
        return Client_;
    }

private:
    const IClientPtr Client_;
};

class TDriver;
typedef TIntrusivePtr<TDriver> TDriverPtr;

class TDriver
    : public IDriver
    , public TSyncSlruCacheBase<TString, TCachedClient>
{
public:
    TDriver(TDriverConfigPtr config, IConnectionPtr connection)
        : TSyncSlruCacheBase(config->ClientCache)
        , Config_(std::move(config))
        , Connection_(std::move(connection))
    {
        YCHECK(Config_);
        YCHECK(Connection_);

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

        REGISTER(TGetFileFromCacheCommand,     "get_file_from_cache",     Null,       Structured, false, false );
        REGISTER(TPutFileToCacheCommand,       "put_file_to_cache",       Null,       Structured, true,  false);

        REGISTER(TWriteTableCommand,           "write_table",             Tabular,    Null,       true,  true );
        REGISTER(TReadTableCommand,            "read_table",              Null,       Tabular,    false, true );
        REGISTER(TReadBlobTableCommand,        "read_blob_table",         Null,       Binary,     false, true );
        REGISTER(TLocateSkynetShareCommand,    "locate_skynet_share",     Null,       Structured, false, true );

        REGISTER(TInsertRowsCommand,           "insert_rows",             Tabular,    Null,       true,  true );
        REGISTER(TDeleteRowsCommand,           "delete_rows",             Tabular,    Null,       true,  true);
        REGISTER(TTrimRowsCommand,             "trim_rows",               Null,       Null,       true,  true);
        REGISTER(TSelectRowsCommand,           "select_rows",             Null,       Tabular,    false, true );
        REGISTER(TLookupRowsCommand,           "lookup_rows",             Tabular,    Tabular,    false, true );

        REGISTER(TEnableTableReplicaCommand,   "enable_table_replica",    Null,       Null,       true,  false);
        REGISTER(TDisableTableReplicaCommand,  "disable_table_replica",   Null,       Null,       true,  false);
        REGISTER(TAlterTableReplicaCommand,    "alter_table_replica",     Null,       Null,       true,  false);
        REGISTER(TGetInSyncReplicasCommand,    "get_in_sync_replicas",    Tabular,    Structured, false, true );

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
        REGISTER(TStartOperationCommand,       "start_op",                Null,       Structured, true,  false);
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
        REGISTER(TListOperationsCommand,       "list_operations",         Null,       Structured, false, false);
        REGISTER(TListJobsCommand,             "list_jobs",               Null,       Structured, false, false);
        REGISTER(TGetJobCommand,               "get_job",                 Null,       Structured, false, false);
        REGISTER(TStraceJobCommand,            "strace_job",              Null,       Structured, false, false);
        REGISTER(TSignalJobCommand,            "signal_job",              Null,       Null,       false, false);
        REGISTER(TAbandonJobCommand,           "abandon_job",             Null,       Null,       false, false);
        REGISTER(TPollJobShellCommand,         "poll_job_shell",          Null,       Structured, true,  false);
        REGISTER(TAbortJobCommand,             "abort_job",               Null,       Null,       false, false);
        REGISTER(TGetOperationCommand,         "get_operation",           Null,       Structured, false, false);

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
            cachedClient = New<TCachedClient>(user, Connection_->CreateClient(options));

            TryInsert(cachedClient, &cachedClient);
        }

        auto context = New<TCommandContext>(
            this,
            cachedClient->GetClient(),
            Config_,
            entry.Descriptor,
            request);

        return BIND(&TDriver::DoExecute, entry.Execute, context)
            .AsyncVia(Connection_->GetInvoker())
            .Run();
    }

    virtual TNullable<TCommandDescriptor> FindCommandDescriptor(const TString& commandName) const override
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

    const TDriverConfigPtr Config_;

    IConnectionPtr Connection_;

    struct TCommandEntry
    {
        TCommandDescriptor Descriptor;
        TExecuteCallback Execute;
    };

    yhash<TString, TCommandEntry> CommandNameToEntry_;


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

        context->MutableRequest().Reset();

        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

    class TCommandContext
        : public ICommandContext
    {
    public:
        TCommandContext(
            IDriverPtr driver,
            IClientPtr client,
            TDriverConfigPtr config,
            const TCommandDescriptor& descriptor,
            const TDriverRequest& request)
            : Driver_(std::move(driver))
            , Client_(std::move(client))
            , Config_(std::move(config))
            , Descriptor_(descriptor)
            , Request_(request)
        { }

        virtual const TDriverConfigPtr& GetConfig() override
        {
            return Config_;
        }

        virtual const IClientPtr& GetClient() override
        {
            return Client_;
        }

        virtual const IDriverPtr& GetDriver() override
        {
            return Driver_;
        }

        virtual const TDriverRequest& Request() override
        {
            return Request_;
        }

        virtual TDriverRequest& MutableRequest() override
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
            auto syncOutputStream = CreateBufferedSyncAdapter(Request_.OutputStream);

            auto consumer = CreateConsumerForFormat(
                GetOutputFormat(),
                Descriptor_.OutputType,
                syncOutputStream.get());

            Serialize(yson, consumer.get());

            consumer->Flush();
        }

    private:
        const IDriverPtr Driver_;
        const IClientPtr Client_;
        const TDriverConfigPtr Config_;
        const TCommandDescriptor Descriptor_;
        TDriverRequest Request_;

        TNullable<TFormat> InputFormat_;
        TNullable<TFormat> OutputFormat_;

    };
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateDriver(INodePtr configNode)
{
    auto config = ConvertTo<TDriverConfigPtr>(configNode);
    auto connection = CreateConnection(configNode);
    return New<TDriver>(std::move(config), std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

