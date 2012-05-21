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

#include <ytlib/chunk_client/block_cache.h>

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

/*
class TOutputStreamConsumer
    : public TForwardingYsonConsumer
{
public:
    TOutputStreamConsumer(TOutputStream* output, EYsonFormat format)
        : Writer(output, format)
    {
        ForwardNode(&Writer, BIND([=] () {
            output->Write('\n');
        }));
    }

private:
    TYsonWriter Writer;
};
*/
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

        BlockCache = CreateClientBlockCache(~config->BlockCache);

        TransactionManager = New<TTransactionManager>(
            config->TransactionManager,
            MasterChannel);

        // Register all commands.
        RegisterCommand<TStartTransactionCommand>(TCommandDescriptor("start_tx", EDataType::Null, EDataType::Structured));
        RegisterCommand<TRenewTransactionCommand>(TCommandDescriptor("renew_tx", EDataType::Null, EDataType::Null));
        RegisterCommand<TCommitTransactionCommand>(TCommandDescriptor("commit_tx", EDataType::Null, EDataType::Null));
        RegisterCommand<TAbortTransactionCommand>(TCommandDescriptor("abort_tx", EDataType::Null, EDataType::Null));

        RegisterCommand<TGetCommand>(TCommandDescriptor("get", EDataType::Null, EDataType::Structured));
        RegisterCommand<TSetCommand>(TCommandDescriptor("set", EDataType::Structured, EDataType::Null));
        RegisterCommand<TRemoveCommand>(TCommandDescriptor("remove", EDataType::Null, EDataType::Null));
        RegisterCommand<TListCommand>(TCommandDescriptor("list", EDataType::Null, EDataType::Structured));
        RegisterCommand<TCreateCommand>(TCommandDescriptor("create", EDataType::Null, EDataType::Structured));
        RegisterCommand<TLockCommand>(TCommandDescriptor("lock", EDataType::Null, EDataType::Structured));

        RegisterCommand<TDownloadCommand>(TCommandDescriptor("download", EDataType::Null, EDataType::Binary));
        RegisterCommand<TUploadCommand>(TCommandDescriptor("upload", EDataType::Binary, EDataType::Null));

        RegisterCommand<TWriteCommand>(TCommandDescriptor("write", EDataType::Tabular, EDataType::Null));
        RegisterCommand<TReadCommand>(TCommandDescriptor("read", EDataType::Null, EDataType::Tabular));

        RegisterCommand<TMapCommand>(TCommandDescriptor("map", EDataType::Null, EDataType::Structured));
        RegisterCommand<TMergeCommand>(TCommandDescriptor("merge", EDataType::Null, EDataType::Structured));
        RegisterCommand<TSortCommand>(TCommandDescriptor("sort", EDataType::Null, EDataType::Structured));
        RegisterCommand<TEraseCommand>(TCommandDescriptor("erase", EDataType::Null, EDataType::Structured));
        RegisterCommand<TAbortOperationCommand>(TCommandDescriptor("abort_op", EDataType::Null, EDataType::Null));
    }

    virtual TDriverResponse Execute(const TDriverRequest& request)
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

    virtual TNullable<TCommandDescriptor> FindCommandDescriptor(const Stroka& commandName)
    {
        auto it = Commands.find(commandName);
        if (it == Commands.end()) {
            return Null;
        }
        return it->second.Descriptor;
    }

    virtual std::vector<TCommandDescriptor> GetCommandDescriptors()
    {
        std::vector<TCommandDescriptor> result;
        result.reserve(Commands.size());
        FOREACH (const auto& pair, Commands) {
            result.push_back(pair.second.Descriptor);
        }
        return result;
    }

private:
    TDriverConfigPtr Config;

    IChannelPtr MasterChannel;
    IChannelPtr SchedulerChannel;
    IBlockCachePtr BlockCache;
    TTransactionManager::TPtr TransactionManager;

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
        TCommandContext(TDriver* driver, const TCommandDescriptor& descriptor, const TDriverRequest* request)
            : Driver(driver)
            , Descriptor(descriptor)
            , Request(request)
        { }

        virtual TDriverConfigPtr GetConfig()
        {
            return Driver->Config;
        }

        virtual IChannelPtr GetMasterChannel()
        {
            return Driver->MasterChannel;
        }

        virtual IChannelPtr GetSchedulerChannel()
        {
            return Driver->SchedulerChannel;
        }

        virtual IBlockCachePtr GetBlockCache()
        {
            return Driver->BlockCache;
        }

        virtual TTransactionManager::TPtr GetTransactionManager()
        {
            return Driver->TransactionManager;
        }

        virtual const TDriverRequest* GetRequest()
        {
            return Request;
        }

        virtual TDriverResponse* GetResponse()
        {
            return &Response;
        }

        virtual TYsonProducer CreateInputProducer()
        {
            return CreateProducerForFormat(
                Request->InputFormat,
                Descriptor.InputType,
                Request->InputStream);
        }

        virtual TAutoPtr<IYsonConsumer> CreateOutputConsumer()
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
        YVERIFY(Commands.insert(MakePair(descriptor.CommandName, entry)).second);
    }

    //void DoExecute(const Stroka& commandName, INodePtr requestNode)
    //{
    //    auto request = New<TRequestBase>();
    //    try {
    //        request->Load(~requestNode);
    //    }
    //    catch (const std::exception& ex) {
    //        ythrow yexception() << Sprintf("Error parsing command from node\n%s", ex.what());
    //    }

    //    auto commandIt = Commands.find(commandName);
    //    if (commandIt == Commands.end()) {
    //        ythrow yexception() << Sprintf("Unknown command %s", ~commandName.Quote());
    //    }

    //    auto command = commandIt->second;
    //    command->Execute(~requestNode);
    //}
    
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateDriver(TDriverConfigPtr config)
{
    return New<TDriver>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
