#include "stdafx.h"
#include "driver.h"
#include "command.h"
#include "transaction_commands.h"
#include "cypress_commands.h"
#include "file_commands.h"
#include "table_commands.h"
#include "scheduler_commands.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/forwarding_yson_consumer.h>
#include <ytlib/ytree/yson_reader.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/chunk_client/client_block_cache.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NRpc;
using namespace NElection;
using namespace NTransactionClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

class TOutputStreamConsumer
    : public TForwardingYsonConsumer
{
public:
    TOutputStreamConsumer(TAutoPtr<TOutputStream> output, EYsonFormat format)
        : Output(output)
        , BufferedOutput(~Output)
        , Writer(&BufferedOutput, format)
    {
        ForwardNode(&Writer, ~FromFunctor([=] ()
            {
                BufferedOutput.Write('\n');
            }));
    }

private:
    TAutoPtr<TOutputStream> Output;
    TBufferedOutput BufferedOutput;
    TYsonWriter Writer;

};

////////////////////////////////////////////////////////////////////////////////

class TOwningBufferedInput
    : public TInputStream
{
public:
    TOwningBufferedInput(TAutoPtr<TInputStream> slave)
        : Slave(slave)
        , Buffered(~Slave)
    { }

private:
    // NB: The order is important.
    TAutoPtr<TInputStream> Slave;
    TBufferedInput Buffered;

    virtual size_t DoRead(void* buf, size_t len)
    {
        return Buffered.Read(buf, len);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TOwningBufferedOutput
    : public TOutputStream
{
public:
    TOwningBufferedOutput(TAutoPtr<TOutputStream> slave)
        : Slave(slave)
        , Buffered(~Slave)
    { }

private:
    // NB: The order is important.
    TAutoPtr<TOutputStream> Slave;
    TBufferedOutput Buffered;

    virtual void DoWrite(const void* buf, size_t len)
    {
        Buffered.Write(buf, len);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDriver::TImpl
    : private TNonCopyable
    , public IDriverImpl
{
public:
    TImpl(
        TConfig* config,
        IDriverStreamProvider* streamProvider)
        : Config(config)
        , StreamProvider(streamProvider)
    {
        YASSERT(config);
        YASSERT(streamProvider);

        MasterChannel = CreateLeaderChannel(~config->Masters);

        BlockCache = CreateClientBlockCache(~config->BlockCache);

        TransactionManager = New<TTransactionManager>(
            ~config->TransactionManager,
            ~MasterChannel);

        RegisterCommand("get", ~New<TNewGetCommand>(this));

//        RegisterCommand("start_transaction", ~New<TStartTransactionCommand>(this));
//        RegisterCommand("commit_transaction", ~New<TCommitTransactionCommand>(this));
//        RegisterCommand("abort_transaction", ~New<TAbortTransactionCommand>(this));

//        RegisterCommand("get", ~New<TGetCommand>(this));
//        RegisterCommand("set", ~New<TSetCommand>(this));
//        RegisterCommand("remove", ~New<TRemoveCommand>(this));
//        RegisterCommand("list", ~New<TListCommand>(this));
//        RegisterCommand("create", ~New<TCreateCommand>(this));
//        RegisterCommand("lock", ~New<TLockCommand>(this));

//        RegisterCommand("download", ~New<TDownloadCommand>(this));
//        RegisterCommand("upload", ~New<TUploadCommand>(this));

//        RegisterCommand("read", ~New<TReadCommand>(this));
//        RegisterCommand("write", ~New<TWriteCommand>(this));

//        RegisterCommand("map", ~New<TMapCommand>(this));
    }

    TError Execute(yvector<Stroka> args)
    {
        Error = TError();
        try {
            DoExecute(args);
        } catch (const std::exception& ex) {
            ReplyError(TError(ex.what()));
        }
        return Error;
    }

    virtual TConfig* GetConfig() const
    {
        return ~Config;
    }

    IChannel* GetMasterChannel() const
    {
        return ~MasterChannel;
    }


    virtual void ReplyError(const TError& error)
    {
        YASSERT(!error.IsOK());
        YASSERT(Error.IsOK());
        Error = error;
        auto output = StreamProvider->CreateErrorStream();
        TYsonWriter writer(~output, Config->OutputFormat);
        BuildYsonFluently(&writer)
            .BeginMap()
                .DoIf(error.GetCode() != TError::Fail, [=] (TFluentMap fluent)
                    {
                        fluent.Item("code").Scalar(error.GetCode());
                    })
                .Item("message").Scalar(error.GetMessage())
            .EndMap();
        output->Write('\n');
    }

    virtual void ReplySuccess(const TYson& yson, const Stroka& spec = "")
    {
        auto consumer = CreateOutputConsumer(spec);
        TStringInput input(yson);
        TYsonReader reader(~consumer, &input);
        reader.Read();
    }

    // Simplified version for unconditional success (yes, its empty output).
    virtual void ReplySuccess()
    { }


    virtual TYsonProducer CreateInputProducer(const Stroka& spec)
    {
        auto stream = CreateInputStream(spec);
        return FromFunctor([=] (IYsonConsumer* consumer)
            {
                TYsonReader reader(consumer, ~stream);
                reader.Read();
            });
    }

    virtual TAutoPtr<TInputStream> CreateInputStream(const Stroka& spec)
    {
        auto stream = StreamProvider->CreateInputStream(spec);
        return new TOwningBufferedInput(stream);
    }

    virtual TAutoPtr<IYsonConsumer> CreateOutputConsumer(const Stroka& spec)
    {
        auto stream = CreateOutputStream(spec);
        return new TOutputStreamConsumer(stream, Config->OutputFormat);
    }

    virtual TAutoPtr<TOutputStream> CreateOutputStream(const Stroka& spec)
    {
        auto stream = StreamProvider->CreateOutputStream(spec);
        return new TOwningBufferedOutput(stream);
    }

    virtual IBlockCache* GetBlockCache()
    {
        return ~BlockCache;
    }

    virtual TTransactionManager* GetTransactionManager()
    {
        return ~TransactionManager;
    }

    virtual TTransactionId GetCurrentTransactionId()
    {
        return !Transaction ? NullTransactionId : Transaction->GetId();
    }

    virtual TTransactionId GetTransactionId(TTransactedRequest* request)
    {
        return request->TransactionId != NullTransactionId ? request->TransactionId : GetCurrentTransactionId();
    }

    virtual ITransaction::TPtr GetTransaction(TTransactedRequest* request, bool required)
    {
        if (request->TransactionId == NullTransactionId) {
            return GetCurrentTransaction(required);
        } else {
            return TransactionManager->Attach(request->TransactionId);
        }
    }

    virtual ITransaction* GetCurrentTransaction(bool required)
    {
        if (!Transaction && required) {
            ythrow yexception() << "No current transaction";
        }
        return ~Transaction;
    }

    virtual void SetCurrentTransaction(ITransaction* transaction)
    {
        Transaction = transaction;
    }

private:
    TConfig::TPtr Config;
    IDriverStreamProvider* StreamProvider;
    TError Error;
    yhash_map<Stroka, INewCommand::TPtr> Commands;
    IChannel::TPtr MasterChannel;
    IBlockCache::TPtr BlockCache;
    TTransactionManager::TPtr TransactionManager;
    ITransaction::TPtr Transaction;

    void RegisterCommand(const Stroka& name, INewCommand* command)
    {
        YVERIFY(Commands.insert(MakePair(name, command)).second);
    }

    void DoExecute(const yvector<Stroka>& args)
    {
        if (args.size() < 2) {
            ythrow yexception() << Sprintf("Command name is not set");
        }

        auto commandName = args[1];
        auto commandIt = Commands.find(commandName);
        if (commandIt == Commands.end()) {
            ythrow yexception() << Sprintf("Unknown command %s", ~commandName.Quote());
        }

        auto command = commandIt->second;
        yvector<Stroka> remainingArgs(args.begin() + 1, args.end());
        command->Execute(remainingArgs);
    }
    
};

////////////////////////////////////////////////////////////////////////////////

TDriver::TDriver(
    TConfig* config,
    IDriverStreamProvider* streamProvider)
    : Impl(new TImpl(config, streamProvider))
{ }

TDriver::~TDriver()
{ }

TError TDriver::Execute(const yvector<Stroka>& args)
{
    return Impl->Execute(args);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
