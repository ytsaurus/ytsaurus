#include "executor.h"
#include "preprocess.h"

#include <build.h>

#include <ytlib/misc/home.h>
#include <ytlib/misc/fs.h>
#include <ytlib/misc/assert.h>

#include <ytlib/ytree/tokenizer.h>
#include <ytlib/ytree/yson_format.h>
#include <ytlib/ytree/fluent.h>

#include <ytlib/job_proxy/config.h>

#include <ytlib/driver/driver.h>
#include <ytlib/driver/command.h>

#include <ytlib/logging/log_manager.h>

#include <util/folder/dirut.h>

namespace NYT {

using namespace NYTree;
using namespace NDriver;
using namespace NScheduler;
using namespace NRpc;
using namespace NFormats;

////////////////////////////////////////////////////////////////////////////////

static const char* UserConfigFileName = ".ytdriver.conf";
static const char* SystemConfigFileName = "ytdriver.conf";
static const char* SystemConfigPath = "/etc/";
static const char* ConfigEnvVar = "YT_CONFIG";

////////////////////////////////////////////////////////////////////////////////

TExecutor::TExecutor()
    : CmdLine("Command line", ' ', YT_VERSION)
    , ConfigArg("", "config", "configuration file", false, "", "STRING")
    , FormatArg("", "format", "format (both input and output)", false, "", "YSON")
    , InputFormatArg("", "in_format", "input format", false, "", "YSON")
    , OutputFormatArg("", "out_format", "output format", false, "", "YSON")
    , ConfigOptArg("", "config_opt", "override configuration option", false, "YPATH=YSON")
    , OptArg("", "opt", "override command option", false, "YPATH=YSON")
{
    CmdLine.add(ConfigArg);
    CmdLine.add(FormatArg);
    CmdLine.add(InputFormatArg);
    CmdLine.add(OutputFormatArg);
    CmdLine.add(ConfigOptArg);
    CmdLine.add(OptArg);
}

IMapNodePtr TExecutor::GetArgs()
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    builder->OnBeginMap();
    BuildArgs(~builder);
    builder->OnEndMap();
    auto args = builder->EndTree()->AsMap();
    FOREACH (const auto& opt, OptArg.getValue()) {
        ApplyYPathOverride(args, opt);
    }
    return args;
}

Stroka TExecutor::GetConfigFileName()
{
    Stroka fromCommandLine = ConfigArg.getValue();;
    Stroka fromEnv = Stroka(getenv(ConfigEnvVar));
    Stroka user = NFS::CombinePaths(GetHomePath(), UserConfigFileName);
    Stroka system = NFS::CombinePaths(SystemConfigPath, SystemConfigFileName);

    if (!fromCommandLine.empty()) {
        return fromCommandLine;
    }

    if (!fromEnv.empty()) {
        return fromEnv;
    }

    if (isexist(~user)) {
        return user;
    }

    if (isexist(~system)) {
        return system;
    }

    ythrow yexception() <<
        Sprintf("Configuration file cannot be found. Please specify it using one of the following methods:\n"
        "1) --config command-line option\n"
        "2) %s environment variable\n"
        "3) per-user file %s\n"
        "4) system-wide file %s",
        ConfigEnvVar,
        ~user.Quote(),
        ~system.Quote());
}

void TExecutor::InitConfig()
{
    // Choose config file name.
    auto fileName = GetConfigFileName();

    // Load config into YSON tree.
    INodePtr configNode;
    try {
        TIFStream configStream(fileName);
        configNode = DeserializeFromYson(&configStream);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error reading configuration\n%s", ex.what());
    }

    // Parse config.
    Config = New<TExecutorConfig>();
    try {
        Config->Load(configNode);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing configuration\n%s", ex.what());
    }

    // Now convert back YSON tree to populate the defaults.
    configNode = DeserializeFromYson(BIND(&TConfigurable::Save, Config));

    // Patch config from command line.
    FOREACH (const auto& opt, ConfigOptArg.getValue()) {
        ApplyYPathOverride(configNode, opt);
    }

    // And finally parse it again.
    try {
        Config->Load(configNode);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing configuration\n%s", ex.what());
    }
}

void TExecutor::Execute(const std::vector<std::string>& args)
{
    auto argsCopy = args;
    CmdLine.parse(argsCopy);

    InitConfig();

    NLog::TLogManager::Get()->Configure(~Config->Logging);
   
    Driver = CreateDriver(Config);

    auto commandName = GetCommandName();
    
    auto descriptor = Driver->FindCommandDescriptor(commandName);
    YASSERT(descriptor);

    auto inputFormat = FormatArg.getValue();
    auto outputFormat = FormatArg.getValue();
    if (!InputFormatArg.getValue().empty()) {
        inputFormat = InputFormatArg.getValue();
    }
    if (!OutputFormatArg.getValue().empty()) {
        outputFormat = OutputFormatArg.getValue();
    }

    // Set stream buffers.
    TBufferedOutput outputStream(&StdOutStream(), 1 << 16);

    TDriverRequest request;
    // GetArgs() must be called before GetInputStream()
    request.Arguments = GetArgs();
    request.CommandName = GetCommandName();
    request.InputStream = GetInputStream();
    request.InputFormat = GetFormat(descriptor->InputType, inputFormat);
    request.OutputStream = &outputStream;
    request.OutputFormat = GetFormat(descriptor->OutputType, outputFormat);;

    DoExecute(request);
}

TFormat TExecutor::GetFormat(EDataType dataType, const Stroka& custom)
{
    if (!custom.empty()) {
        INodePtr customNode;
        try {
            customNode = DeserializeFromYson(custom);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing format description\n%s", ex.what());
        }
        return TFormat::FromYson(customNode);
    }

    switch (dataType) {
        case EDataType::Null:
        case EDataType::Binary:
            return TFormat(EFormatType::Null);

        case EDataType::Structured:
            return TFormat::FromYson(Config->FormatDefaults->Structured);

        case EDataType::Tabular:
            return TFormat::FromYson(Config->FormatDefaults->Tabular);

        default:
            YUNREACHABLE();
    }
}

void TExecutor::BuildArgs(IYsonConsumer* consumer)
{
    UNUSED(consumer);
}

void TExecutor::DoExecute(const TDriverRequest& request)
{
    auto response = Driver->Execute(request);

    if (!response.Error.IsOK()) {
        ythrow yexception() << response.Error.ToString();
    }
}

TInputStream* TExecutor::GetInputStream()
{
    return &StdInStream();
}

////////////////////////////////////////////////////////////////////////////////

TTransactedExecutor::TTransactedExecutor(bool required)
    : TxArg("", "tx", "set transaction id", required, "", "GUID")
{
    CmdLine.add(TxArg);
}

void TTransactedExecutor::BuildArgs(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .DoIf(TxArg.isSet(), [=] (TFluentMap fluent) {
            fluent.Item("transaction_id").Scalar(TxArg.getValue());
        });

    TExecutor::BuildArgs(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
