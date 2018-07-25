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

#include <yt/ytlib/api/connection.h>

#include <yt/client/api/transaction.h>
#include <yt/client/api/connection.h>

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
#define REGISTER(command, name, inDataType, outDataType, isVolatile, isHeavy, version) \
            if (version == Config_->ApiVersion) { \
                RegisterCommand<command>( \
                    TCommandDescriptor{name, EDataType::inDataType, EDataType::outDataType, isVolatile, isHeavy}); \
            }
#define REGISTER_ALL(command, name, inDataType, outDataType, isVolatile, isHeavy) \
            RegisterCommand<command>( \
                TCommandDescriptor{name, EDataType::inDataType, EDataType::outDataType, isVolatile, isHeavy}); \

        REGISTER    (TStartTransactionCommand,            "start_tx",                      Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TPingTransactionCommand,             "ping_tx",                       Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TCommitTransactionCommand,           "commit_tx",                     Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TAbortTransactionCommand,            "abort_tx",                      Null,       Null,       true,  false, ApiVersion3);

        REGISTER    (TStartTransactionCommand,            "start_transaction",             Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TPingTransactionCommand,             "ping_transaction",              Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TCommitTransactionCommand,           "commit_transaction",            Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TAbortTransactionCommand,            "abort_transaction",             Null,       Structured, true,  false, ApiVersion4);

        REGISTER_ALL(TGenerateTimestampCommand,           "generate_timestamp",            Null,       Structured, false, false);

        REGISTER_ALL(TCreateCommand,                      "create",                        Null,       Structured, true,  false);
        REGISTER_ALL(TGetCommand,                         "get",                           Null,       Structured, false, false);
        REGISTER_ALL(TListCommand,                        "list",                          Null,       Structured, false, false);
        REGISTER_ALL(TLockCommand,                        "lock",                          Null,       Structured, true,  false);
        REGISTER_ALL(TCopyCommand,                        "copy",                          Null,       Structured, true,  false);
        REGISTER_ALL(TMoveCommand,                        "move",                          Null,       Structured, true,  false);
        REGISTER_ALL(TLinkCommand,                        "link",                          Null,       Structured, true,  false);
        REGISTER_ALL(TExistsCommand,                      "exists",                        Null,       Structured, false, false);

        REGISTER    (TConcatenateCommand,                 "concatenate",                   Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TRemoveCommand,                      "remove",                        Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TSetCommand,                         "set",                           Structured, Null,       true,  false, ApiVersion3);

        REGISTER    (TConcatenateCommand,                 "concatenate",                   Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TRemoveCommand,                      "remove",                        Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TSetCommand,                         "set",                           Structured, Structured, true,  false, ApiVersion4);

        REGISTER    (TWriteFileCommand,                   "write_file",                    Binary,     Null,       true,  true,  ApiVersion3);
        REGISTER    (TWriteFileCommand,                   "write_file",                    Binary,     Structured, true,  true,  ApiVersion4);
        REGISTER_ALL(TReadFileCommand,                    "read_file",                     Null,       Binary,     false, true );

        REGISTER_ALL(TGetFileFromCacheCommand,            "get_file_from_cache",           Null,       Structured, false, false);
        REGISTER_ALL(TPutFileToCacheCommand,              "put_file_to_cache",             Null,       Structured, true,  false);

        REGISTER    (TWriteTableCommand,                  "write_table",                   Tabular,    Null,       true,  true , ApiVersion3);
        REGISTER    (TWriteTableCommand,                  "write_table",                   Tabular,    Structured, true,  true , ApiVersion4);
        REGISTER_ALL(TGetTableColumnarStatisticsCommand,  "get_table_columnar_statistics", Null,       Structured, false, false);
        REGISTER_ALL(TReadTableCommand,                   "read_table",                    Null,       Tabular,    false, true );
        REGISTER_ALL(TReadBlobTableCommand,               "read_blob_table",               Null,       Binary,     false, true );
        REGISTER_ALL(TLocateSkynetShareCommand,           "locate_skynet_share",           Null,       Structured, false, true );

        REGISTER    (TInsertRowsCommand,                  "insert_rows",                   Tabular,    Null,       true,  true , ApiVersion3);
        REGISTER    (TDeleteRowsCommand,                  "delete_rows",                   Tabular,    Null,       true,  true , ApiVersion3);
        REGISTER    (TTrimRowsCommand,                    "trim_rows",                     Null,       Null,       true,  true , ApiVersion3);

        REGISTER    (TInsertRowsCommand,                  "insert_rows",                   Tabular,    Structured, true,  true , ApiVersion4);
        REGISTER    (TDeleteRowsCommand,                  "delete_rows",                   Tabular,    Structured, true,  true , ApiVersion4);
        REGISTER    (TTrimRowsCommand,                    "trim_rows",                     Null,       Structured, true,  true , ApiVersion4);

        REGISTER_ALL(TSelectRowsCommand,                  "select_rows",                   Null,       Tabular,    false, true );
        REGISTER_ALL(TLookupRowsCommand,                  "lookup_rows",                   Tabular,    Tabular,    false, true );

        REGISTER    (TEnableTableReplicaCommand,          "enable_table_replica",          Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TDisableTableReplicaCommand,         "disable_table_replica",         Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TAlterTableReplicaCommand,           "alter_table_replica",           Null,       Null,       true,  false, ApiVersion3);

        REGISTER    (TEnableTableReplicaCommand,          "enable_table_replica",          Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TDisableTableReplicaCommand,         "disable_table_replica",         Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TAlterTableReplicaCommand,           "alter_table_replica",           Null,       Structured, true,  false, ApiVersion4);

        REGISTER_ALL(TGetInSyncReplicasCommand,           "get_in_sync_replicas",          Tabular,    Structured, false, true );

        REGISTER    (TMountTableCommand,                  "mount_table",                   Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TUnmountTableCommand,                "unmount_table",                 Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TRemountTableCommand,                "remount_table",                 Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TFreezeTableCommand,                 "freeze_table",                  Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TUnfreezeTableCommand,               "unfreeze_table",                Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TReshardTableCommand,                "reshard_table",                 Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TAlterTableCommand,                  "alter_table",                   Null,       Null,       true,  false, ApiVersion3);

        REGISTER    (TMountTableCommand,                  "mount_table",                   Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TUnmountTableCommand,                "unmount_table",                 Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TRemountTableCommand,                "remount_table",                 Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TFreezeTableCommand,                 "freeze_table",                  Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TUnfreezeTableCommand,               "unfreeze_table",                Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TReshardTableCommand,                "reshard_table",                 Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TAlterTableCommand,                  "alter_table",                   Null,       Structured, true,  false, ApiVersion4);

        REGISTER    (TMergeCommand,                       "merge",                         Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TEraseCommand,                       "erase",                         Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TMapCommand,                         "map",                           Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TSortCommand,                        "sort",                          Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TReduceCommand,                      "reduce",                        Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TJoinReduceCommand,                  "join_reduce",                   Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TMapReduceCommand,                   "map_reduce",                    Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TRemoteCopyCommand,                  "remote_copy",                   Null,       Structured, true,  false, ApiVersion3);

        REGISTER    (TStartOperationCommand,              "start_op",                      Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TAbortOperationCommand,              "abort_op",                      Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TSuspendOperationCommand,            "suspend_op",                    Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TResumeOperationCommand,             "resume_op",                     Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TCompleteOperationCommand,           "complete_op",                   Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TUpdateOperationParametersCommand,   "update_op_parameters",          Null,       Null,       true,  false, ApiVersion3);

        REGISTER    (TStartOperationCommand,              "start_operation",               Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TAbortOperationCommand,              "abort_operation",               Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TSuspendOperationCommand,            "suspend_operation",             Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TResumeOperationCommand,             "resume_operation",              Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TCompleteOperationCommand,           "complete_operation",            Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TUpdateOperationParametersCommand,   "update_operation_parameters",   Null,   Structured, true,  false, ApiVersion4);

        REGISTER_ALL(TParseYPathCommand,                  "parse_ypath",                   Null,       Structured, false, false);

        REGISTER    (TAddMemberCommand,                   "add_member",                    Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TRemoveMemberCommand,                "remove_member",                 Null,       Null,       true,  false, ApiVersion3);

        REGISTER    (TAddMemberCommand,                   "add_member",                    Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TRemoveMemberCommand,                "remove_member",                 Null,       Structured, true,  false, ApiVersion4);

        REGISTER_ALL(TCheckPermissionCommand,             "check_permission",              Null,       Structured, false, false);
        REGISTER_ALL(TCheckPermissionByAclCommand,        "check_permission_by_acl",       Null,       Structured, false, false);

        REGISTER    (TWriteJournalCommand,                "write_journal",                 Tabular,    Null,       true,  true , ApiVersion3);
        REGISTER    (TWriteJournalCommand,                "write_journal",                 Tabular,    Structured, true,  true , ApiVersion4);
        REGISTER_ALL(TReadJournalCommand,                 "read_journal",                  Null,       Tabular,    false, true );

        REGISTER_ALL(TGetJobInputCommand,                 "get_job_input",                 Null,       Binary,     false, true );
        REGISTER_ALL(TGetJobStderrCommand,                "get_job_stderr",                Null,       Binary,     false, true );
        REGISTER_ALL(TGetJobFailContextCommand,           "get_job_fail_context",          Null,       Binary,     false, true );
        REGISTER_ALL(TListOperationsCommand,              "list_operations",               Null,       Structured, false, false);
        REGISTER_ALL(TListJobsCommand,                    "list_jobs",                     Null,       Structured, false, false);
        REGISTER_ALL(TGetJobCommand,                      "get_job",                       Null,       Structured, false, false);
        REGISTER_ALL(TStraceJobCommand,                   "strace_job",                    Null,       Structured, false, false);
        REGISTER_ALL(TPollJobShellCommand,                "poll_job_shell",                Null,       Structured, true,  false);
        REGISTER_ALL(TGetOperationCommand,                "get_operation",                 Null,       Structured, false, false);

        REGISTER    (TDumpJobContextCommand,              "dump_job_context",              Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TSignalJobCommand,                   "signal_job",                    Null,       Null,       false, false, ApiVersion3);
        REGISTER    (TAbandonJobCommand,                  "abandon_job",                   Null,       Null,       false, false, ApiVersion3);
        REGISTER    (TAbortJobCommand,                    "abort_job",                     Null,       Null,       false, false, ApiVersion3);

        REGISTER    (TDumpJobContextCommand,              "dump_job_context",              Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TSignalJobCommand,                   "signal_job",                    Null,       Structured, false, false, ApiVersion4);
        REGISTER    (TAbandonJobCommand,                  "abandon_job",                   Null,       Structured, false, false, ApiVersion4);
        REGISTER    (TAbortJobCommand,                    "abort_job",                     Null,       Structured, false, false, ApiVersion4);

        REGISTER_ALL(TGetVersionCommand,                  "get_version",                   Null,       Structured, false, false);

        REGISTER_ALL(TExecuteBatchCommand,                "execute_batch",                 Null,       Structured, true,  false);

        REGISTER    (TDiscoverProxiesCommand,             "discover_proxies",              Null,       Structured, false, false, ApiVersion4);

#undef REGISTER
#undef REGISTER_ALL
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
            if (request.UserToken) {
                options.Token = request.UserToken;
            }
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

    virtual void ClearMetadataCaches() override
    {
        Clear();
        Connection_->ClearMetadataCaches();
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

        ClearMetadataCaches();

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

    THashMap<TString, TCommandEntry> CommandNameToEntry_;


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
            syncOutputStream->Flush();
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

