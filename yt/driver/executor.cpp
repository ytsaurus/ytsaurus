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

TExecutorBase::TExecutorBase()
    : CmdLine("Command line", ' ', YT_VERSION)
    , ConfigArg("", "config", "configuration file", false, "", "file_name")
    , FormatArg("", "format", "format (both input and output)", false, "", "yson")
    , InputFormatArg("", "in_format", "input format", false, "", "yson")
    , OutputFormatArg("", "out_format", "output format", false, "", "yson")
    , ConfigSetArg("", "config_set", "set configuration value", false, "ypath=yson")
    , OptsArg("", "opts", "other options", false, "key=yson")
{
    CmdLine.add(ConfigArg);
    CmdLine.add(FormatArg);
    CmdLine.add(InputFormatArg);
    CmdLine.add(OutputFormatArg);
    CmdLine.add(ConfigSetArg);
    CmdLine.add(OptsArg);
}

IMapNodePtr TExecutorBase::GetArgs()
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    builder->OnBeginMap();
    BuildArgs(~builder);
    builder->OnEndMap();
    return builder->EndTree()->AsMap();
}

Stroka TExecutorBase::GetConfigFileName()
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
        Sprintf("Unable to find configuration file. Please specify it using one of the following methods:\n"
        "1) --config option\n"
        "2) YT_CONFIG environment variable\n"
        "3) per-user file %s\n"
        "4) system-wide file %s",
        ~user.Quote(),
        ~system.Quote());
}

void TExecutorBase::InitConfig()
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
        Config->Load(~configNode);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing configuration\n%s", ex.what());
    }

    // Now convert back YSON tree to populate defaults.
    configNode = DeserializeFromYson(BIND(&TConfigurable::Save, Config));

    // Patch config from command line.
    ApplyConfigUpdates(configNode);

    // And finally parse it again.
    try {
        Config->Load(~configNode);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing configuration\n%s", ex.what());
    }
}

void TExecutorBase::Execute(const std::vector<std::string>& args)
{
    auto argsCopy = args;
    CmdLine.parse(argsCopy);

    InitConfig();

    NLog::TLogManager::Get()->Configure(~Config->Logging);
   
    Driver = CreateDriver(Config);

    auto commandName = GetDriverCommandName();
    
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

    // Set bufferization of streams
    TAutoPtr<TInputStream> inputStream = new TBufferedInput(&StdInStream(), 1 << 16);
    TAutoPtr<TOutputStream> outputStream = new TBufferedOutput(&StdOutStream(), 1 << 16);

    TDriverRequest request;
    request.CommandName = GetDriverCommandName();
    request.InputStream = ~inputStream;
    request.InputFormat = GetFormat(descriptor->InputType, inputFormat);
    request.OutputStream = ~outputStream;
    request.OutputFormat = GetFormat(descriptor->OutputType, outputFormat);;
    request.Arguments = GetArgs();

    DoExecute(request);
}

void TExecutorBase::ApplyConfigUpdates(IYPathServicePtr service)
{
    FOREACH (auto updateString, ConfigSetArg.getValue()) {
        TTokenizer tokenizer(updateString);
        tokenizer.ParseNext();
        while (tokenizer.GetCurrentType() != KeyValueSeparatorToken) {
            if (!tokenizer.ParseNext()) {
                ythrow yexception() << "Incorrect option";
            }
        }
        TStringBuf ypath = TStringBuf(updateString).Chop(tokenizer.CurrentInput().length());
        SyncYPathSet(service, TYPath(ypath), TYson(tokenizer.GetCurrentSuffix()));
    }
}

TFormat TExecutorBase::GetFormat(EDataType dataType, const Stroka& custom)
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

void TExecutorBase::BuildOptions(IYsonConsumer* consumer)
{
    FOREACH (const auto& opts, OptsArg.getValue()) {
        // TODO(babenko): think about a better way of doing this
        auto items = DeserializeFromYson("{" + opts + "}")->AsMap();
        FOREACH (const auto& pair, items->GetChildren()) {
            consumer->OnKeyedItem(pair.first);
            VisitTree(pair.second, consumer, true);
        }
    }
}

void TExecutorBase::BuildArgs(IYsonConsumer* consumer)
{
    UNUSED(consumer);
}

void TExecutorBase::DoExecute(const TDriverRequest& request)
{
    auto response = Driver->Execute(request);

    if (!response.Error.IsOK()) {
        ythrow yexception() << response.Error.ToString();
    }
}

////////////////////////////////////////////////////////////////////////////////

TTransactedExecutor::TTransactedExecutor(bool required)
    : TxArg("", "tx", "set transaction id", required, "", "transaction_id")
{
    CmdLine.add(TxArg);
}

void TTransactedExecutor::BuildArgs(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .DoIf(TxArg.isSet(), [=] (TFluentMap fluent) {
            Stroka txId = TxArg.getValue();
            fluent.Item("transaction_id").Scalar(txId);
        });

    TExecutorBase::BuildArgs(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
