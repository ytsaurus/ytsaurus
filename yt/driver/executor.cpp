#include "executor.h"
#include "preprocess.h"

#include <yt/build.h>

#include <ytlib/misc/home.h>
#include <ytlib/misc/fs.h>
#include <ytlib/misc/assert.h>

#include <ytlib/ytree/tokenizer.h>
#include <ytlib/ytree/yson_format.h>
#include <ytlib/ytree/fluent.h>

#include <server/job_proxy/config.h>

#include <ytlib/driver/driver.h>
#include <ytlib/driver/command.h>

#include <ytlib/logging/log_manager.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NScheduler;
using namespace NRpc;
using namespace NFormats;
using namespace NTransactionClient;

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
        configNode = ConvertToNode(&configStream);
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

    // Now convert back YSON tree to populate defaults.
    configNode = ConvertToNode(BIND(&TYsonSerializable::Save, Config));

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

EExitCode TExecutor::Execute(const std::vector<std::string>& args)
{
    auto argsCopy = args;
    CmdLine.parse(argsCopy);

    InitConfig();

    NLog::TLogManager::Get()->Configure(Config->Logging);
   
    Driver = CreateDriver(Config);

    auto commandName = GetCommandName();
    
    auto descriptor = Driver->FindCommandDescriptor(commandName);
    YASSERT(descriptor);

    Stroka inputFormatString = FormatArg.getValue();
    Stroka outputFormatString = FormatArg.getValue();
    if (!InputFormatArg.getValue().empty()) {
        inputFormatString = InputFormatArg.getValue();
    }
    if (!OutputFormatArg.getValue().empty()) {
        outputFormatString = OutputFormatArg.getValue();
    }
    
    TNullable<TYsonString> inputFormat, outputFormat;
    if (!inputFormatString.empty()) {
        inputFormat = TYsonString(inputFormatString);
    }
    if (!outputFormatString.empty()) {
        outputFormat = TYsonString(outputFormatString);
    }

    // Set stream buffers.
    TBufferedOutput outputStream(&StdOutStream(), 1 << 16);

    TDriverRequest request;
    // GetArgs() must be called before GetInputStream()
    request.Arguments = GetArgs();
    request.CommandName = GetCommandName();

    request.InputStream = GetInputStream();
    try {
        request.InputFormat = GetFormat(descriptor->InputType, inputFormat);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing input format\n%s", ex.what());
    }

    request.OutputStream = &outputStream;
    try {
        request.OutputFormat = GetFormat(descriptor->OutputType, outputFormat);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing output format\n%s", ex.what());
    }

    return DoExecute(request);
}

TFormat TExecutor::GetFormat(EDataType dataType, const TNullable<TYsonString>& yson)
{
    if (yson) {
        INodePtr node;
        try {
            node = ConvertToNode(*yson);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing format description\n%s", ex.what());
        }
        return TFormat::FromYson(node);
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

EExitCode TExecutor::DoExecute(const TDriverRequest& request)
{
    auto response = Driver->Execute(request);

    if (!response.Error.IsOK()) {
        ythrow yexception() << response.Error.ToString();
    }

    return EExitCode::OK;
}

TInputStream* TExecutor::GetInputStream()
{
    return &StdInStream();
}

////////////////////////////////////////////////////////////////////////////////

TTransactedExecutor::TTransactedExecutor(
    bool txRequired,
    bool txLabeled)
    : LabeledTxArg("", "tx", "set transaction id", txRequired, TTransactionId(), "TX_ID")
    , UnlabeledTxArg("tx", "transaction id", txRequired, TTransactionId(), "TX_ID")
    , PingAncestorTxsArg("", "ping_ancestor_txs", "ping ancestor transactions", false)
{
    CmdLine.add(txLabeled ? LabeledTxArg : UnlabeledTxArg);
    CmdLine.add(PingAncestorTxsArg);
}

void TTransactedExecutor::BuildArgs(IYsonConsumer* consumer)
{
    TNullable<TTransactionId> txId;
    if (LabeledTxArg.isSet()) {
        txId = LabeledTxArg.getValue();
    }
    if (UnlabeledTxArg.isSet()) {
        txId = UnlabeledTxArg.getValue();
    }

    if (PingAncestorTxsArg.getValue() && !txId) {
        ythrow yexception() << "ping_ancestor_txs is set but no tx_id is given";
    }

    BuildYsonMapFluently(consumer)
        .DoIf(txId, [=] (TFluentMap fluent) {
            fluent.Item("transaction_id").Scalar(txId.Get());
        })
        .Item("ping_ancestor_transactions").Scalar(PingAncestorTxsArg.getValue());

    TExecutor::BuildArgs(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
