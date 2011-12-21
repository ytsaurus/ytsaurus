#include <yt/ytlib/logging/log_manager.h>

#include <yt/ytlib/misc/delayed_invoker.h>

#include <yt/ytlib/driver/driver.h>

#include <yt/ytlib/ytree/serialize.h>
#include <yt/ytlib/ytree/yson_writer.h>

#include <util/config/last_getopt.h>
#include <util/stream/pipe.h>

#ifdef _win_
#include <io.h>
#else
#include <unistd.h>
#endif

namespace NYT {

using namespace NDriver;
using namespace NYTree;

static NLog::TLogger& Logger = DriverLogger;

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

        if (spec[0] == '&') {
            int handle = ParseHandle(spec);
            return new TSystemInput(handle);
        }

        if (spec[0] == '<') {
            return new TFileInput(spec.substr(1));
        }

        return new TPipeInput(~spec);
    }

    virtual TAutoPtr<TOutputStream> CreateOutputStream(const Stroka& spec)
    {
        if (spec.empty()) {
            return new TSystemOutput(1);
        }

        if (spec[0] == '&') {
            int handle = ParseHandle(spec);
            return new TSystemOutput(handle);
        }

        if (spec[0] == '>') {
            if (spec.length() >= 2 && spec[1] == '>') {
                TFile file(spec.substr(2), OpenAlways | ForAppend | WrOnly | Seq);
                return new TFileOutput(file);
            } else {
                return new TFileOutput(spec.substr(1));
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
        : TDriver::TConfig
    {
        INode::TPtr Logging;

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
        try {
            using namespace NLastGetopt;

            Stroka configFileName;;
            TOpts opts;

            opts.AddHelpOption();

            const auto& configOpt = opts.AddLongOption("config", "configuration file")
                .Optional()
                .RequiredArgument("FILENAME")
                .StoreResult(&configFileName);

            const auto& shellOpt = opts.AddLongOption("shell", "run in shell mode")
                .Optional()
                .NoArgument();

            Stroka formatStr;
            const auto& formatOpt = opts.AddLongOption("format", "output format: Text, Pretty or Binary (default is Text)")
                .Optional()
                .RequiredArgument("FORMAT")
                .StoreResult(&formatStr);

            const auto& haltOnErrorOpt = opts.AddLongOption("halt-on-error", "halt batch execution upon receiving an error")
                .Optional()
                .NoArgument();

            TOptsParseResult results(&opts, argc, argv);

            auto config = New<TConfig>();
            if (results.Has(&configOpt)) {
                INode::TPtr configNode;
                try {
                    TIFStream configStream(configFileName);
                    TYson configYson = configStream.ReadAll();
                    configNode = DeserializeFromYson(configYson);
                } catch (const std::exception& ex) {
                    ythrow yexception() << Sprintf("Error reading configuration\n%s", ex.what());
                }

                try {
                    config->Load(~configNode);
                } catch (const std::exception& ex) {
                    ythrow yexception() << Sprintf("Error parsing configuration\n%s", ex.what());
                }

                NLog::TLogManager::Get()->Configure(~config->Logging);
            } else {
                config->Validate();
            }

            if (results.Has(&formatOpt)) {
                config->OutputFormat = TYsonWriter::EFormat::FromString(formatStr);
            }

            HaltOnError = results.Has(&haltOnErrorOpt);

            Driver = new TDriver(~config, &StreamProvider);

            if (results.Has(&shellOpt)) {
                RunShell();
            } else {
                RunBatch();
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

    void RunBatch()
    {
        while (true) {
            Stroka request;
            if (!Cin.ReadLine(request))
                break;

            if (request.empty())
                continue;

            auto error = Driver->Execute(request);
            if (!error.IsOK() && HaltOnError) {
                ExitCode = 1;
                break;
            }
        }
    }

    void RunShell()
    {
        YUNIMPLEMENTED();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char* argv[])
{
    NYT::TDriverProgram program;
    return program.Main(argc, argv);
}

