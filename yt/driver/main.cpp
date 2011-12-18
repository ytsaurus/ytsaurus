#include <yt/ytlib/logging/log_manager.h>

#include <yt/ytlib/misc/delayed_invoker.h>

#include <yt/ytlib/driver/driver.h>

#include <yt/ytlib/ytree/serialize.h>
#include <yt/ytlib/ytree/yson_writer.h>

#include <util/config/last_getopt.h>

namespace NYT {

using namespace NDriver;
using namespace NYTree;

static NLog::TLogger& Logger = DriverLogger;

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
        : Format(TYsonWriter::EFormat::Text)
        , ExitCode(0)
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

            Stroka boxSuccessStr;
            const auto& boxSuccessOpt = opts.AddLongOption("box-success", "override success boxing")
                .Optional()
                .RequiredArgument("FLAG")
                .StoreResult(&boxSuccessStr);

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
                Format = TYsonWriter::EFormat::FromString(formatStr);
            }

            if (results.Has(&boxSuccessOpt)) {
                config->BoxSuccess = FromString<bool>(boxSuccessStr);
            }

            HaltOnError = results.Has(&haltOnErrorOpt);

            Driver = new TDriver(~config);

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
    TYsonWriter::EFormat Format;
    int ExitCode;
    bool HaltOnError;
    TAutoPtr<TDriver> Driver;

    void RunBatch()
    {
        while (true) {
            Stroka request;
            if (!Cin.ReadLine(request))
                break;

            if (request.empty())
                continue;

            TYsonWriter writer(&StdOutStream(), Format);
            auto error = Driver->Execute(request, &writer);
            Cout << Endl;

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

