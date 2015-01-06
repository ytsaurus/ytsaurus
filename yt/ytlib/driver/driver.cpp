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
#include "journal_commands.h"

#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/scheduler.h>

#include <core/ytree/forwarding_yson_consumer.h>
#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/null_yson_consumer.h>

#include <core/yson/parser.h>

#include <core/rpc/scoped_channel.h>

#include <ytlib/transaction_client/timestamp_provider.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/chunk_client/block_cache.h>

#include <ytlib/tablet_client/table_mount_cache.h>

#include <ytlib/api/connection.h>

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
        REGISTER(TMoveCommand,              "move",              Null,       Structured, true,  false);
        REGISTER(TLinkCommand,              "link",              Null,       Structured, true,  false);
        REGISTER(TExistsCommand,            "exists",            Null,       Structured, false, false);

        REGISTER(TWriteFileCommand,         "write_file",        Binary,     Null,       true,  true );
        REGISTER(TReadFileCommand,          "read_file",         Null,       Binary,     false, true );
        // COMPAT(babenko)
        REGISTER(TWriteFileCommand,         "upload",            Binary,     Null,       true,  true );
        REGISTER(TReadFileCommand,          "download",          Null,       Binary,     false, true );

        REGISTER(TWriteTableCommand,        "write_table",       Tabular,    Null,       true,  true );
        REGISTER(TReadTableCommand,         "read_table",        Null,       Tabular,    false, true );
        // TODO(babenko): proper naming
        REGISTER(TInsertCommand,            "insert",            Tabular,    Null,       true,  true );
        REGISTER(TSelectCommand,            "select",            Null,       Tabular,    false, true );
        REGISTER(TLookupCommand,            "lookup",            Null,       Tabular,    false, true );
        REGISTER(TDeleteCommand,            "delete",            Null,       Null,       true,  true);
        // COMPAT(babenko)
        REGISTER(TWriteTableCommand,        "write",             Tabular,    Null,       true,  true );
        REGISTER(TReadTableCommand,         "read",              Null,       Tabular,    false, true );

        REGISTER(TMountTableCommand,        "mount_table",       Null,       Null,       true,  false);
        REGISTER(TUnmountTableCommand,      "unmount_table",     Null,       Null,       true,  false);
        REGISTER(TRemountTableCommand,      "remount_table",     Null,       Null,       true,  false);
        REGISTER(TReshardTableCommand,      "reshard_table",     Null,       Null,       true,  false);

        REGISTER(TMergeCommand,             "merge",             Null,       Structured, true,  false);
        REGISTER(TEraseCommand,             "erase",             Null,       Structured, true,  false);
        REGISTER(TMapCommand,               "map",               Null,       Structured, true,  false);
        REGISTER(TSortCommand,              "sort",              Null,       Structured, true,  false);
        REGISTER(TReduceCommand,            "reduce",            Null,       Structured, true,  false);
        REGISTER(TMapReduceCommand,         "map_reduce",        Null,       Structured, true,  false);
        REGISTER(TRemoteCopyCommand,        "remote_copy",       Null,       Structured, true,  false);
        REGISTER(TAbortOperationCommand,    "abort_op",          Null,       Null,       true,  false);
        REGISTER(TSuspendOperationCommand,  "suspend_op",        Null,       Null,       true,  false);
        REGISTER(TResumeOperationCommand,   "resume_op",         Null,       Null,       true,  false);

        REGISTER(TParseYPathCommand,        "parse_ypath",       Null,       Structured, false, false);

        REGISTER(TAddMemberCommand,         "add_member",        Null,       Null,       true,  false);
        REGISTER(TRemoveMemberCommand,      "remove_member",     Null,       Null,       true,  false);
        REGISTER(TCheckPermissionCommand,   "check_permission",  Null,       Structured, false, false);

        REGISTER(TWriteJournalCommand,      "write_journal",       Tabular,    Null,       true,  true );
        REGISTER(TReadJournalCommand,       "read_journal",        Null,       Tabular,    false, true );
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

        LOG_INFO("Command started (Command: %v, User: %v)",
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

        auto command = entry.Factory.Run();

        auto invoker = entry.Descriptor.IsHeavy
            ? TDispatcher::Get()->GetHeavyInvoker()
            : TDispatcher::Get()->GetLightInvoker();

        return BIND(&TDriver::DoExecute, command, context)
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

    typedef TCallback< ICommandPtr() > TCommandFactory;

    TDriverConfigPtr Config;

    IConnectionPtr Connection_;

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

    static void DoExecute(ICommandPtr command, TCommandContextPtr context)
    {
        const auto& request = context->Request();

        TRACE_CHILD("Driver", request.CommandName) {
            command->Execute(context);
        }

        const auto& error = context->GetError();
        if (error.IsOK()) {
            LOG_INFO("Command completed (Command: %v)", request.CommandName);
        } else {
            LOG_INFO(error, "Command failed (Command: %v)", request.CommandName);
        }

        WaitFor(context->Terminate());

        THROW_ERROR_EXCEPTION_IF_FAILED(error);
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
            , SyncInputStream_(request.InputStream ? CreateSyncAdapter(request.InputStream) : nullptr)
            , SyncOutputStream_(request.OutputStream ? CreateSyncAdapter(request.OutputStream) : nullptr)
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

        virtual TYsonProducer CreateInputProducer() override
        {
            return CreateProducerForFormat(
                GetInputFormat(),
                Descriptor_.InputType,
                SyncInputStream_.get());
        }

        virtual std::unique_ptr<IYsonConsumer> CreateOutputConsumer() override
        {
            return CreateConsumerForFormat(
                GetOutputFormat(),
                Descriptor_.OutputType,
                SyncOutputStream_.get());
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

        virtual void Reply(const TError& error) override
        {
            YCHECK(!Replied_);
            Error_ = error;
            Replied_ = true;
        }

        const TError& GetError() const
        {
            return Error_;
        }

    private:
        const TDriverPtr Driver_;
        const TCommandDescriptor Descriptor_;
        const TDriverRequest Request_;

        bool Replied_ = false;
        TError Error_;

        TNullable<TFormat> InputFormat_;
        TNullable<TFormat> OutputFormat_;

        std::unique_ptr<TInputStream> SyncInputStream_;
        std::unique_ptr<TOutputStream> SyncOutputStream_;

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

