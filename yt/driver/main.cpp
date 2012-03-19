#include <ytlib/logging/log_manager.h>

#include <ytlib/profiling/profiling_manager.h>

#include <ytlib/misc/delayed_invoker.h>

#include <ytlib/driver/driver.h>

#include <ytlib/rpc/rpc_manager.h>

#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/yson_reader.h>
#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/yson_parser.h>

#include <ytlib/misc/home.h>
#include <ytlib/misc/fs.h>
#include <ytlib/misc/errortrace.h>

#include <util/config/last_getopt.h>
#include <util/stream/pipe.h>

#ifdef _win_
#include <io.h>
#else
#include <unistd.h>
#include <errno.h>
#endif

#include <tclap/CmdLine.h>
#include <ytlib/misc/tclap_helpers.h>

namespace NYT {

using namespace NDriver;
using namespace NYTree;

static NLog::TLogger& Logger = DriverLogger;
static const char* DefaultConfigFileName = ".ytdriver.config.yson";

////////////////////////////////////////////////////////////////////////////////

//TODO(panin): move to proper place
class TArgsBase
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TArgsBase> TPtr;

    TArgsBase()
    {
        Cmd.Reset(new TCLAP::CmdLine("Command line"));
        ConfigArg.Reset(new TCLAP::ValueArg<std::string>(
            "", "config", "configuration file", false, "", "file_name"));
        OptsArg.Reset(new TCLAP::MultiArg<std::string>(
            "", "opts", "other options", false, "options"));

        Cmd->add(~ConfigArg);
        Cmd->add(~OptsArg);
    }

    void Parse(const std::vector<Stroka>& args)
    {
        std::vector<std::string> stringArgs;
        FOREACH (auto arg, args) {
            stringArgs.push_back(std::string(~arg));
        }
        Cmd->parse(stringArgs);
    }

    INodePtr GetCommand()
    {
        auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        builder->BeginTree();
        builder->OnBeginMap();
        AddItems(~builder);
        builder->OnEndMap();
        return builder->EndTree();
    }

    Stroka GetConfigName()
    {
        return Stroka(ConfigArg->getValue());
    }

protected:
    //useful typedefs
    typedef TCLAP::UnlabeledValueArg<Stroka> TFreeStringArg;

    THolder<TCLAP::CmdLine> Cmd;

    THolder<TCLAP::ValueArg<std::string> > ConfigArg;
    THolder<TCLAP::MultiArg<std::string> > OptsArg;

    void AddOpts(IYsonConsumer* consumer)
    {
        FOREACH (auto opts, OptsArg->getValue()) {
            NYTree::TYson yson = Stroka("{") + Stroka(opts) + "}";
            auto items = NYTree::DeserializeFromYson(yson)->AsMap();
            FOREACH (auto child, items->GetChildren()) {
                consumer->OnMapItem(child.first);
                consumer->OnStringScalar(child.second->AsString()->GetValue());
            }
        }
    }

    // TODO(panin): maybe rewrite using fluent
    virtual void AddItems(IYsonConsumer* consumer) {
        AddOpts(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTransactedArgs
    : public TArgsBase
{
public:
    TTransactedArgs()
    {
        TxArg.Reset(new TTxArg(
            "", "tx", "transaction id", false, NObjectServer::NullTransactionId, "guid"));
        Cmd->add(~TxArg);
    }
protected:
    typedef TCLAP::ValueArg<NObjectServer::TTransactionId> TTxArg;
    THolder<TTxArg> TxArg;

    virtual void AddItems(IYsonConsumer* consumer)
    {
        consumer->OnMapItem("transaction_id");
        consumer->OnStringScalar(TxArg->getValue().ToString());
        TArgsBase::AddItems(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGetArgs
    : public TTransactedArgs
{
public:
    TGetArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        Cmd->add(~PathArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    virtual void AddItems(IYsonConsumer* consumer)
    {
        consumer->OnMapItem("do");
        consumer->OnStringScalar("get");

        consumer->OnMapItem("path");
        consumer->OnStringScalar(PathArg->getValue());

        TTransactedArgs::AddItems(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSetArgs
    : public TTransactedArgs
{
public:
    TSetArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        ValueArg.Reset(new TFreeStringArg("value", "value to set", true, "", "yson"));

        Cmd->add(~PathArg);
        Cmd->add(~ValueArg);
    }

    virtual void AddItems(IYsonConsumer* consumer)
    {
        consumer->OnMapItem("do");
        consumer->OnStringScalar("set");

        consumer->OnMapItem("path");
        consumer->OnStringScalar(PathArg->getValue());

        consumer->OnMapItem("value");
        TStringInput input(ValueArg->getValue());
        ParseYson(&input, consumer);

        TTransactedArgs::AddItems(consumer);
    }

private:
    THolder<TFreeStringArg> PathArg;
    THolder<TFreeStringArg> ValueArg;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveArgs
    : public TTransactedArgs
{
public:
    TRemoveArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        Cmd->add(~PathArg);
    }

    virtual void AddItems(IYsonConsumer* consumer)
    {
        consumer->OnMapItem("do");
        consumer->OnStringScalar("remove");

        consumer->OnMapItem("path");
        consumer->OnStringScalar(PathArg->getValue());

        TTransactedArgs::AddItems(consumer);
    }


private:
    THolder<TFreeStringArg> PathArg;
};

////////////////////////////////////////////////////////////////////////////////

class TListArgs
    : public TTransactedArgs
{
public:
    TListArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        Cmd->add(~PathArg);
    }

private:
    THolder<TFreeStringArg> PathArg;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateArgs
    : public TTransactedArgs
{
public:
    TCreateArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        TypeArg.Reset(new TTypeArg(
            "type", "type of node", true, NObjectServer::EObjectType::Undefined, "object type"));

        Cmd->add(~PathArg);
        Cmd->add(~TypeArg);

        ManifestArg.Reset(new TManifestArg("", "manifest", "manifest", false, "", "yson"));
        Cmd->add(~ManifestArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    typedef TCLAP::UnlabeledValueArg<NObjectServer::EObjectType> TTypeArg;
    THolder<TTypeArg> TypeArg;

    typedef TCLAP::ValueArg<NYTree::TYson> TManifestArg;
    THolder<TManifestArg> ManifestArg;
};

////////////////////////////////////////////////////////////////////////////////

class TLockArgs
    : public TTransactedArgs
{
public:
    TLockArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        //TODO(panin): check given value
        ModeArg.Reset(new TModeArg(
            "", "mode", "lock mode", false, NCypress::ELockMode::Exclusive, "snapshot, shared, exclusive"));
        Cmd->add(~PathArg);
        Cmd->add(~ModeArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    typedef TCLAP::ValueArg<NCypress::ELockMode> TModeArg;
    THolder<TModeArg> ModeArg;
};

////////////////////////////////////////////////////////////////////////////////

class TSystemInput
    : public TInputStream
{
public:
    TSystemInput(int handle)
        : Handle(handle)
    { }

private:
    int Handle;

    virtual size_t DoRead(void* buf, size_t len)
    {
        int result;
        do {
            result = read(Handle, buf, len);
        } while (result < 0 && errno == EINTR);
        

        if (result < 0) {
            ythrow yexception() << Sprintf("Error reading from stream (Handle: %d, Error: %d)",
                Handle,
                errno);
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSystemOutput
    : public TOutputStream
{
public:
    TSystemOutput(int handle)
        : Handle(handle)
    { }

private:
    int Handle;

    virtual void DoWrite(const void* buf, size_t len)
    {
        size_t totalWritten = 0;
        while (totalWritten < len) {
            int result;
            do {
                result = write(Handle, static_cast<const char*>(buf) + totalWritten, len - totalWritten);
            } while (result < 0 && errno == EINTR);

            if (result == 0) {
                ythrow yexception() << Sprintf("Error writing to stream (Handle: %d, Error: nothing written)",
                    Handle);
            }
            if (result < 0 ) {
                ythrow yexception() << Sprintf("Error writing to stream (Handle: %d, Error: %d)",
                    Handle,
                    errno);
            }
            
            totalWritten += result;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStreamProvider
    : public IDriverStreamProvider
{
public:
    virtual TAutoPtr<TInputStream> CreateInputStream()
    {
        return new TSystemInput(0);
    }

    virtual TAutoPtr<TOutputStream> CreateOutputStream()
    {
        return new TSystemOutput(1);
    }

    virtual TAutoPtr<TOutputStream> CreateErrorStream()
    {
        return new TSystemOutput(2);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDriverProgram
{
public:
    struct TConfig
        : public TDriver::TConfig
    {
        INodePtr Logging;

        TConfig()
        {
            Register("logging", Logging);
        }
    };

    TDriverProgram()
        : ExitCode(0)
    {
        RegisterParser("get", ~New<TGetArgs>());
        RegisterParser("set", ~New<TSetArgs>());
        RegisterParser("remove", ~New<TRemoveArgs>());
    }

    int Main(int argc, const char* argv[])
    {
        NYT::SetupErrorHandler();

        try {
            if (argc < 2) {
                ythrow yexception() << "Not enough arguments";
            }
            auto argsParser = GetArgsParser(Stroka(argv[1]));

            std::vector<Stroka> remainingArgs;
            for (int i = 1; i < argc; ++i) {
                remainingArgs.push_back(Stroka(argv[i]));
            }

            argsParser->Parse(remainingArgs);

            Stroka configFileName = argsParser->GetConfigName();
            if (configFileName.empty()) {
                auto configFromEnv = getenv("YT_CONFIG");
                if (configFromEnv) {
                    configFileName = Stroka(configFromEnv);
                } else {
                    configFileName = NFS::CombinePaths(GetHomePath(), DefaultConfigFileName);
                }
            }

            auto config = New<TConfig>();
            INodePtr configNode;
            try {
                TIFStream configStream(configFileName);
                configNode = DeserializeFromYson(&configStream);
            } catch (const std::exception& ex) {
                ythrow yexception() << Sprintf("Error reading configuration\n%s", ex.what());
            }

            try {
                config->Load(~configNode);
            } catch (const std::exception& ex) {
                ythrow yexception() << Sprintf("Error parsing configuration\n%s", ex.what());
            }

            NLog::TLogManager::Get()->Configure(~config->Logging);

            Driver = new TDriver(~config, &StreamProvider);

            auto command = argsParser->GetCommand();
            RunCommand(command);

        } catch (const std::exception& ex) {
            LOG_ERROR("%s", ex.what());
            ExitCode = 1;
        }

        // TODO: refactor system shutdown
        NLog::TLogManager::Get()->Shutdown();
        NRpc::TRpcManager::Get()->Shutdown();
        NProfiling::TProfilingManager::Get()->Shutdown();
        TDelayedInvoker::Shutdown();

        return ExitCode;
    }

private:
    int ExitCode;

    TStreamProvider StreamProvider;
    TAutoPtr<TDriver> Driver;

    yhash_map<Stroka, TArgsBase::TPtr> ArgsParsers;

    void RegisterParser(const Stroka& name, TArgsBase* command)
    {
        YVERIFY(ArgsParsers.insert(MakePair(name, command)).second);
    }

    TArgsBase::TPtr GetArgsParser(Stroka command) {
        auto parserIt = ArgsParsers.find(command);
        if (parserIt == ArgsParsers.end()) {
            ythrow yexception() << Sprintf("Unknown command %s", ~command.Quote());
        }
        return parserIt->second;
    }


    void RunCommand(INodePtr command)
    {
        auto error = Driver->Execute(command);
        if (!error.IsOK()) {
            ExitCode = 1;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT

int main(int argc, const char* argv[])
{
    NYT::TDriverProgram program;
    return program.Main(argc, argv);
}

