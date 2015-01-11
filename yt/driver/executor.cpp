#include "executor.h"
#include "preprocess.h"

#include <core/build.h>

#include <core/misc/fs.h>
#include <core/misc/assert.h>

#include <core/yson/tokenizer.h>
#include <core/yson/format.h>
#include <core/ytree/fluent.h>

#include <server/job_proxy/config.h>

#include <ytlib/driver/driver.h>
#include <ytlib/driver/dispatcher.h>
#include <ytlib/driver/command.h>

#include <core/logging/log_manager.h>

#include <core/tracing/trace_context.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NYson;
using namespace NScheduler;
using namespace NRpc;
using namespace NFormats;
using namespace NTransactionClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const char* UserConfigFileName = ".ytdriver.conf";
static const char* SystemConfigFileName = "ytdriver.conf";
static const char* SystemConfigPath = "/etc/";
static const char* ConfigEnvVar = "YT_CONFIG";
static const i64 OutputBufferSize = (1 << 16);

////////////////////////////////////////////////////////////////////////////////

TExecutor::TExecutor()
    : CmdLine("Command line", ' ', GetVersion())
    , ConfigArg("", "config", "configuration file", false, "", "STRING")
    , ConfigOptArg("", "config_opt", "override configuration option", false, "YPATH=YSON")
{
    CmdLine.add(ConfigArg);
    CmdLine.add(ConfigOptArg);
}

Stroka TExecutor::GetConfigFileName()
{
    Stroka fromCommandLine = ConfigArg.getValue();;
    Stroka fromEnv = Stroka(getenv(ConfigEnvVar));
    Stroka user = NFS::CombinePaths(NFS::GetHomePath(), UserConfigFileName);
    Stroka system = NFS::CombinePaths(SystemConfigPath, SystemConfigFileName);

    if (!fromCommandLine.empty()) {
        return fromCommandLine;
    }

    if (!fromEnv.empty()) {
        return fromEnv;
    }

    if (NFS::Exists(user)) {
        return user;
    }

    if (NFS::Exists(system)) {
        return system;
    }

    throw std::runtime_error(Format(
        "Configuration file cannot be found. Please specify it using one of the following methods:\n"
        "1) --config command-line option\n"
        "2) %v environment variable\n"
        "3) per-user file %Qv\n"
        "4) system-wide file %Qv",
        ConfigEnvVar,
        user,
        system));
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
        THROW_ERROR_EXCEPTION("Error reading configuration")
            << ex;
    }

    // Parse config.
    Config = New<TExecutorConfig>();
    try {
        Config->Load(configNode);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing configuration")
            << ex;
    }

    // Now convert back YSON tree to populate defaults.
    configNode = ConvertToNode(Config);

    // Patch config from command line.
    for (const auto& opt : ConfigOptArg.getValue()) {
        ApplyYPathOverride(configNode, opt);
    }

    // And finally parse it again.
    try {
        Config->Load(configNode);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing configuration")
            << ex;
    }
}

void TExecutor::Execute(const std::vector<std::string>& args)
{
    auto argsCopy = args;
    CmdLine.parse(argsCopy);

    InitConfig();

    NTracing::TTraceContextGuard guard(Config->Trace
        ? NTracing::CreateRootTraceContext()
        : NTracing::NullTraceContext);

    NLog::TLogManager::Get()->Configure(Config->Logging);
    TAddressResolver::Get()->Configure(Config->AddressResolver);

    TDispatcher::Get()->Configure(Config->Driver->HeavyPoolSize);
    Driver = CreateDriver(Config->Driver);

    DoExecute();
}

////////////////////////////////////////////////////////////////////////////////

TRequestExecutor::TRequestExecutor()
    : AuthenticatedUserArg("", "user", "user to impersonate", false, "", "STRING")
    , FormatArg("", "format", "format (both input and output)", false, "", "YSON")
    , InputFormatArg("", "in_format", "input format", false, "", "YSON")
    , OutputFormatArg("", "out_format", "output format", false, "", "YSON")
    , OptArg("", "opt", "override command option", false, "YPATH=YSON")
    , ResponseParametersArg("", "response_parameters", "print response parameters", false)
{
    CmdLine.add(AuthenticatedUserArg);
    CmdLine.add(FormatArg);
    CmdLine.add(InputFormatArg);
    CmdLine.add(OutputFormatArg);
    CmdLine.add(OptArg);
    CmdLine.add(ResponseParametersArg);
}

void TRequestExecutor::DoExecute()
{
    auto commandName = GetCommandName();

    auto descriptor = Driver->GetCommandDescriptor(commandName);

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
    OutputStream_ = std::unique_ptr<TOutputStream>(new TBufferedOutput(&StdOutStream(), OutputBufferSize));

    TDriverRequest request;
    // GetParameters() must be called before GetInputStream()
    request.Parameters = GetParameters();
    request.CommandName = GetCommandName();

    if (AuthenticatedUserArg.isSet()) {
        request.AuthenticatedUser = AuthenticatedUserArg.getValue();
    }

    request.InputStream = CreateAsyncAdapter(GetInputStream());
    try {
        request.Parameters->AddChild(
            ConvertToNode(GetFormat(descriptor.InputType, inputFormat)),
            "input_format");
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing input format") << ex;
    }

    request.OutputStream = CreateAsyncAdapter(OutputStream_.get());
    try {
        request.Parameters->AddChild(
            ConvertToNode(GetFormat(descriptor.OutputType, outputFormat)),
            "output_format");
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing output format") << ex;
    }

    TYsonWriter ysonWriter(&StdErrStream(), NYson::EYsonFormat::Pretty);
    if (ResponseParametersArg.getValue()) {
        request.ResponseParametersConsumer = &ysonWriter;
    }

    DoExecute(request);
}

void TRequestExecutor::DoExecute(const TDriverRequest& request)
{
    Driver->Execute(request)
        .Get()
        .ThrowOnError();
}

IMapNodePtr TRequestExecutor::GetParameters()
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();

    BuildYsonFluently(builder.get())
        .BeginMap()
            .Do(BIND(&TRequestExecutor::BuildParameters, Unretained(this)))
        .EndMap();

    auto parameters = builder->EndTree()->AsMap();
    for (const auto& opt : OptArg.getValue()) {
        ApplyYPathOverride(parameters, opt);
    }
    return parameters;
}

TFormat TRequestExecutor::GetFormat(EDataType dataType, const TNullable<TYsonString>& yson)
{
    if (yson) {
        return ConvertTo<TFormat>(yson.Get());
    }

    switch (dataType) {
        case EDataType::Null:
        case EDataType::Binary:
            return TFormat(EFormatType::Null);

        case EDataType::Structured:
            return Config->FormatDefaults->Structured;

        case EDataType::Tabular:
            return Config->FormatDefaults->Tabular;

        default:
            YUNREACHABLE();
    }
}

void TRequestExecutor::BuildParameters(IYsonConsumer* consumer)
{
    UNUSED(consumer);
}

TInputStream* TRequestExecutor::GetInputStream()
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

void TTransactedExecutor::BuildParameters(IYsonConsumer* consumer)
{
    TNullable<TTransactionId> txId;
    if (LabeledTxArg.isSet()) {
        txId = LabeledTxArg.getValue();
    }
    if (UnlabeledTxArg.isSet()) {
        txId = UnlabeledTxArg.getValue();
    }

    if (PingAncestorTxsArg.getValue() && !txId) {
        THROW_ERROR_EXCEPTION("ping_ancestor_txs is set but no tx_id is given");
    }

    BuildYsonMapFluently(consumer)
        .DoIf(txId.HasValue(), [=] (TFluentMap fluent) {
            fluent.Item("transaction_id").Value(txId.Get());
        })
        .Item("ping_ancestor_transactions").Value(PingAncestorTxsArg.getValue());

    TRequestExecutor::BuildParameters(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
