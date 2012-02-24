#include <ytlib/logging/log_manager.h>

#include <ytlib/misc/delayed_invoker.h>

#include <ytlib/driver/driver.h>

#include <ytlib/rpc/rpc_manager.h>

#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/yson_reader.h>
#include <ytlib/ytree/tree_builder.h>

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

namespace NYT {

using namespace NDriver;
using namespace NYTree;

static NLog::TLogger& Logger = DriverLogger;
static const char* DefaultConfigFileName = ".ytdriver.config.yson";

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
    virtual TAutoPtr<TInputStream> CreateInputStream(const Stroka& spec)
    {
        if (spec.empty()) {
            return new TSystemInput(0);
        }

        //XXX(ijon): do we really need syntactic sugar like this? I guess not
        if (spec[0] == '&') {
            int handle = ParseHandle(spec);
            return new TSystemInput(handle);
        }

        //XXX(ijon): do we really need syntactic sugar like this? I guess not
        if (spec[0] == '<') {
            auto fileName = TrimLeadingWhitespace(spec.substr(1));
            return new TFileInput(fileName);
        }

        return new TPipeInput(~spec);
    }

    virtual TAutoPtr<TOutputStream> CreateOutputStream(const Stroka& spec)
    {
        if (spec.empty()) {
            return new TSystemOutput(1);
        }

        //XXX(ijon): do we really need syntactic sugar like this? I guess not
        if (spec[0] == '&') {
            int handle = ParseHandle(spec);
            return new TSystemOutput(handle);
        }

        //XXX(ijon): do we really need syntactic sugar like this? I guess not
        if (spec[0] == '>') {
            if (spec.length() >= 2 && spec[1] == '>') {
                auto fileName = TrimLeadingWhitespace(spec.substr(2));
                TFile file(fileName, OpenAlways | ForAppend | WrOnly | Seq);
                return new TFileOutput(file);
            } else {
                auto fileName = TrimLeadingWhitespace(spec.substr(1));
                return new TFileOutput(fileName);
            }
        }

        return new TPipeOutput(~spec);
    }

    virtual TAutoPtr<TOutputStream> CreateErrorStream()
    {
        return new TSystemOutput(2);
    }

private:
    int ParseHandle(const Stroka& spec)
    {
        try {
            return FromString(TStringBuf(spec.begin() + 1, spec.length() - 1));
        } catch (const TFromStringException&) {
            ythrow yexception() << Sprintf("Invalid handle in stream specification %s", ~spec);
        }
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
        , HaltOnError(false)
    { }

    int Main(int argc, const char* argv[])
    {
		NYT::SetupErrorHandler();

        try {
            using namespace NLastGetopt;

            Stroka configFileName;;
            TOpts opts;

            opts.AddHelpOption();

            const auto& configOpt = opts.AddLongOption("config", "configuration file")
                .Optional()
                .RequiredArgument("FILENAME")
                .StoreResult(&configFileName);

            Stroka formatStr;
            const auto& formatOpt = opts.AddLongOption("format", "output format: Text, Pretty or Binary (default is Text)")
                .Optional()
                .RequiredArgument("FORMAT")
                .StoreResult(&formatStr);

            const auto& haltOnErrorOpt = opts.AddLongOption("halt-on-error", "halt batch execution upon receiving an error")
                .Optional()
                .NoArgument();

            // accept yson text as single free command line argument
            opts.SetFreeArgsMin(0);
            opts.SetFreeArgsMax(1);
            opts.SetFreeArgTitle(0, "CMD");

            TOptsParseResult results(&opts, argc, argv);
            if (!results.Has(&configOpt)) {
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
                config->LoadAndValidate(~configNode);
            } catch (const std::exception& ex) {
                ythrow yexception() << Sprintf("Error parsing configuration\n%s", ex.what());
            }

            NLog::TLogManager::Get()->Configure(~config->Logging);

            if (results.Has(&formatOpt)) {
                config->OutputFormat = EYsonFormat::FromString(formatStr);
            }

            HaltOnError = results.Has(&haltOnErrorOpt);

            Driver = new TDriver(~config, &StreamProvider);

            yvector<Stroka> freeArgs(results.GetFreeArgs());
            if (freeArgs.empty()) {
                RunBatch(Cin);
            } else {
                // opts was configured to accept no more then one free arg
                TStringInput input(freeArgs[0]);
                RunBatch(input);
            }
        } catch (const std::exception& ex) {
            LOG_ERROR("%s", ex.what());
            ExitCode = 1;
        }

        // TODO: refactor system shutdown
        NLog::TLogManager::Get()->Shutdown();
        NRpc::TRpcManager::Get()->Shutdown();
        TDelayedInvoker::Shutdown();

        return ExitCode;
    }

private:
    int ExitCode;
    bool HaltOnError;
    TStreamProvider StreamProvider;
    TAutoPtr<TDriver> Driver;

    void RunBatch(TInputStream& input)
    {
        auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        TYsonFragmentReader parser(~builder, &input);
        while(parser.HasNext()) {
            builder->BeginTree();
            parser.ReadNext();
            auto commandNode = builder->EndTree();

            auto error = Driver->Execute(commandNode);
            if (!error.IsOK() && HaltOnError) {
                ExitCode = 1;
                break;
            }
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

