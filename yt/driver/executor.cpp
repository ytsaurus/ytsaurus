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

#include <ytlib/object_server/object_service_proxy.h>

#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/scheduler/helpers.h>

#include <util/folder/dirut.h>
#include <util/stream/format.h>

namespace NYT {

using namespace NYTree;
using namespace NScheduler;
using namespace NDriver;
using namespace NObjectServer;
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

    TDriverRequest request;
    request.CommandName = GetDriverCommandName();
    request.InputStream = &StdInStream();
    request.InputFormat = GetFormat(descriptor->InputType, inputFormat);
    request.OutputStream = &StdOutStream();
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

TTransactedExecutor::TTransactedExecutor()
    : TxArg("", "tx", "set transaction id", false, "", "transaction_id")
{
    CmdLine.add(TxArg);
}

void TTransactedExecutor::BuildArgs(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .DoIf(TxArg.isSet(), [=] (TFluentMap fluent) {
            TYson txYson = TxArg.getValue();
            ValidateYson(txYson);
            fluent.Item("transaction_id").Node(txYson);
        });

    TExecutorBase::BuildArgs(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TGetExecutor::TGetExecutor()
    : PathArg("path", "path to an object in Cypress that must be retrieved", true, "", "path")
{
    CmdLine.add(PathArg);
}

void TGetExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TGetExecutor::GetDriverCommandName() const
{
    return "get";
}

////////////////////////////////////////////////////////////////////////////////

TSetExecutor::TSetExecutor()
    : PathArg("path", "path to an object in Cypress that must be set", true, "", "path")
    , ValueArg("value", "value to set", true, "", "yson")
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
}

void TSetExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path)
        .Item("value").Node(ValueArg.getValue());

    TTransactedExecutor::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TSetExecutor::GetDriverCommandName() const
{
    return "set";
}

////////////////////////////////////////////////////////////////////////////////

TRemoveExecutor::TRemoveExecutor()
    : PathArg("path", "path to an object in Cypress that must be removed", true, "", "path")
{
    CmdLine.add(PathArg);
}

void TRemoveExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TRemoveExecutor::GetDriverCommandName() const
{
    return "remove";
}

////////////////////////////////////////////////////////////////////////////////

TListExecutor::TListExecutor()
    : PathArg("path", "path to a object in Cypress whose children must be listed", true, "", "path")
{
    CmdLine.add(PathArg);
}

void TListExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);
 
    TTransactedExecutor::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TListExecutor::GetDriverCommandName() const
{
    return "list";
}

////////////////////////////////////////////////////////////////////////////////

TCreateExecutor::TCreateExecutor()
    : TypeArg("type", "type of node", true, NObjectServer::EObjectType::Null, "object type")
    , PathArg("path", "path for a new object in Cypress", true, "", "ypath")
{
    CmdLine.add(TypeArg);
    CmdLine.add(PathArg);
}

void TCreateExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path)
        .Item("type").Scalar(TypeArg.getValue().ToString());

    TTransactedExecutor::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TCreateExecutor::GetDriverCommandName() const
{
    return "create";
}

////////////////////////////////////////////////////////////////////////////////

TLockExecutor::TLockExecutor()
    : PathArg("path", "path to an object in Cypress that must be locked", true, "", "path")
    , ModeArg("", "mode", "lock mode", false, NCypress::ELockMode::Exclusive, "snapshot, shared, exclusive")
{
    CmdLine.add(PathArg);
    CmdLine.add(ModeArg);
}

void TLockExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path)
        .Item("mode").Scalar(ModeArg.getValue().ToString());

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TLockExecutor::GetDriverCommandName() const
{
    return "lock";
}

//////////////////////////////////////////////////////////////////////////////////

void TStartTxExecutor::BuildArgs(IYsonConsumer* consumer)
{
    TTransactedExecutor::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TStartTxExecutor::GetDriverCommandName() const
{
    return "start_tx";
}

//////////////////////////////////////////////////////////////////////////////////

Stroka TRenewTxExecutor::GetDriverCommandName() const
{
    return "renew_tx";
}

//////////////////////////////////////////////////////////////////////////////////

Stroka TCommitTxExecutor::GetDriverCommandName() const
{
    return "commit_tx";
}

//////////////////////////////////////////////////////////////////////////////////

Stroka TAbortTxExecutor::GetDriverCommandName() const
{
    return "abort_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TReadExecutor::TReadExecutor()
    : PathArg("path", "path to a table in Cypress that must be read", true, "", "ypath")
{
    CmdLine.add(PathArg);
}

void TReadExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("read")
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TReadExecutor::GetDriverCommandName() const
{
    return "read";
}

//////////////////////////////////////////////////////////////////////////////////

TWriteExecutor::TWriteExecutor()
    : PathArg("path", "path to a table in Cypress that must be written", true, "", "ypath")
    , ValueArg("value", "row(s) to write", false, "", "yson")
    , KeyColumnsArg("", "sorted", "key columns names (table must initially be empty, input data must be sorted)", false, "", "list_fragment")
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
    CmdLine.add(KeyColumnsArg);
}

void TWriteExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());
    auto value = ValueArg.getValue();
    // TODO(babenko): refactor
    auto keyColumns = DeserializeFromYson< yvector<Stroka> >("[" + KeyColumnsArg.getValue() + "]");

    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("write")
        .Item("path").Scalar(path)
        .DoIf(!keyColumns.empty(), [=] (TFluentMap fluent) {
            fluent.Item("sorted").Scalar(true);
            fluent.Item("key_columns").List(keyColumns);
        })
        .DoIf(!value.empty(), [=] (TFluentMap fluent) {
                fluent.Item("value").Node(value);
        });

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TWriteExecutor::GetDriverCommandName() const
{
    return "write";
}

//////////////////////////////////////////////////////////////////////////////////

TUploadExecutor::TUploadExecutor()
    : PathArg("path", "to a new file in Cypress that must be uploaded", true, "", "ypath")
{
    CmdLine.add(PathArg);
}

void TUploadExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TUploadExecutor::GetDriverCommandName() const
{
    return "upload";
}

//////////////////////////////////////////////////////////////////////////////////

TDownloadExecutor::TDownloadExecutor()
    : PathArg("path", "path to a file in Cypress that must be downloaded", true, "", "ypath")
{
    CmdLine.add(PathArg);
}

void TDownloadExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TDownloadExecutor::GetDriverCommandName() const
{
    return "download";
}

//////////////////////////////////////////////////////////////////////////////////

class TStartOpExecutor::TOperationTracker
{
public:
    TOperationTracker(
        TExecutorConfigPtr config,
        IDriverPtr driver,
        const TOperationId& operationId,
        EOperationType operationType)
        : Config(config)
        , Driver(driver)
        , OperationId(operationId)
        , OperationType(operationType)
    { }

    void Run()
    {
        TSchedulerServiceProxy proxy(Driver->GetSchedulerChannel());

        while (true)  {
            auto waitOpReq = proxy.WaitForOperation();
            *waitOpReq->mutable_operation_id() = OperationId.ToProto();
            waitOpReq->set_timeout(Config->OperationWaitTimeout.GetValue());

            // Override default timeout.
            waitOpReq->SetTimeout(Config->OperationWaitTimeout * 2);
            auto waitOpRsp = waitOpReq->Invoke().Get();

            if (!waitOpRsp->IsOK()) {
                ythrow yexception() << waitOpRsp->GetError().ToString();
            }

            if (waitOpRsp->finished())
                break;

            DumpProgress();
        }

        DumpResult();
    }

private:
    TExecutorConfigPtr Config;
    IDriverPtr Driver;
    TOperationId OperationId;
    EOperationType OperationType;

    // TODO(babenko): refactor
    // TODO(babenko): YPath and RPC responses currently share no base class.
    template <class TResponse>
    static void CheckResponse(TResponse response, const Stroka& failureMessage)
    {
        if (response->IsOK())
            return;

        ythrow yexception() << failureMessage + "\n" + response->GetError().ToString();
    }

    static void AppendPhaseProgress(Stroka* out, const Stroka& phase, const TYson& progress)
    {
        i64 jobsTotal = DeserializeFromYson<i64>(progress, "/total");
        if (jobsTotal == 0) {
            return;
        }
        
        i64 jobsCompleted = DeserializeFromYson<i64>(progress, "/completed");
        int percentComplete  = (jobsCompleted * 100) / jobsTotal;

        if (!out->empty()) {
            out->append(", ");
        }

        out->append(Sprintf("%3d%% ", percentComplete));
        if (!phase.empty()) {
            out->append(phase);
            out->append(' ');
        }

        out->append("done ");

        // Some simple pretty-printing.
        int totalWidth = ToString(jobsTotal).length();
        out->append("(");
        out->append(ToString(LeftPad(ToString(jobsCompleted), totalWidth)));
        out->append(" of ");
        out->append(ToString(jobsTotal));
        out->append(")");
    }

    Stroka FormatProgress(const TYson& progress)
    {
        // TODO(babenko): refactor
        auto progressAttributes = IAttributeDictionary::FromMap(DeserializeFromYson(progress)->AsMap());
        Stroka result;
        switch (OperationType) {
            case EOperationType::Map:
            case EOperationType::Merge:
            case EOperationType::Erase:
                AppendPhaseProgress(&result, "", progressAttributes->GetYson("jobs"));
                break;
                                        
            case EOperationType::Sort:
                AppendPhaseProgress(&result, "partition", progressAttributes->GetYson("partition_jobs"));
                AppendPhaseProgress(&result, "sort", progressAttributes->GetYson("sort_jobs"));
                AppendPhaseProgress(&result, "merge", progressAttributes->GetYson("merge_jobs"));
                break;

            default:
                YUNREACHABLE();
        }
        return result;
    }

    void DumpProgress()
    {
        auto operationPath = GetOperationPath(OperationId);

        TObjectServiceProxy proxy(Driver->GetMasterChannel());
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TYPathProxy::Get(operationPath + "/@state");
            batchReq->AddRequest(req, "get_state");
        }

        {
            auto req = TYPathProxy::Get(operationPath + "/@progress");
            batchReq->AddRequest(req, "get_progress");
        }

        auto batchRsp = batchReq->Invoke().Get();
        CheckResponse(batchRsp, "Error getting operation progress");

        EOperationState state;
        {
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_state");
            CheckResponse(rsp, "Error getting operation state");
            state = DeserializeFromYson<EOperationState>(rsp->value());
        }

        TYson progress;
        {
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_progress");
            CheckResponse(rsp, "Error getting operation progress");
            progress = rsp->value();
        }

        if (state == EOperationState::Running) {
            printf("%s: %s\n",
                ~state.ToString(),
                ~FormatProgress(progress));
        } else {
            printf("%s\n", ~state.ToString());
        }
    }

    void DumpResult()
    {
        auto operationPath = GetOperationPath(OperationId);

        TObjectServiceProxy proxy(Driver->GetMasterChannel());
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TYPathProxy::Get(operationPath + "/@result");
            batchReq->AddRequest(req, "get_result");
        }

        auto batchRsp = batchReq->Invoke().Get();
        CheckResponse(batchRsp, "Error getting operation result");

        {
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_result");
            CheckResponse(rsp, "Error getting operation result");
            // TODO(babenko): refactor!
            auto errorNode = DeserializeFromYson<INodePtr>(rsp->value(), "/error");
            auto error = TError::FromYson(errorNode);
            if (!error.IsOK()) {
                ythrow yexception() << error.ToString();
            }
        }

        printf("Operation completed successfully\n");
    }
};

//////////////////////////////////////////////////////////////////////////////////

TStartOpExecutor::TStartOpExecutor()
    : DontTrackArg("", "dont_track", "don't track operation progress")
{
    CmdLine.add(DontTrackArg);
}

void TStartOpExecutor::DoExecute(const TDriverRequest& request)
{
    if (DontTrackArg.getValue()) {
        TExecutorBase::DoExecute(request);
        return;
    }

    printf("Starting %s operation... ", ~GetDriverCommandName().Quote());

    auto requestCopy = request;

    TStringStream output;
    requestCopy.OutputStream = &output;

    auto response = Driver->Execute(requestCopy);
    if (!response.Error.IsOK()) {
        printf("failed\n");
        ythrow yexception() << response.Error.ToString();
    }

    auto operationId = DeserializeFromYson<TOperationId>(output.Str());
    printf("done, %s\n", ~operationId.ToString());

    TOperationTracker tracker(Config, Driver, operationId, GetOperationType());
    tracker.Run();
}

//////////////////////////////////////////////////////////////////////////////////

TMapExecutor::TMapExecutor()
    : InArg("", "in", "input tables", false, "ypath")
    , OutArg("", "out", "output tables", false, "ypath")
    , FilesArg("", "file", "additional files", false, "ypath")
    , MapperArg("", "mapper", "mapper shell command", true, "", "command")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(FilesArg);
    CmdLine.add(MapperArg);
}

void TMapExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto input = PreprocessYPaths(InArg.getValue());
    auto output = PreprocessYPaths(OutArg.getValue());
    auto files = PreprocessYPaths(FilesArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("mapper").Scalar(MapperArg.getValue())
            .Item("input_table_paths").List(input)
            .Item("output_table_paths").List(output)
            .Item("file_paths").List(files)
            .Do(BIND(&TMapExecutor::BuildOptions, Unretained(this)))
        .EndMap();

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TMapExecutor::GetDriverCommandName() const
{
    return "map";
}

EOperationType TMapExecutor::GetOperationType() const
{
    return EOperationType::Map;
}

//////////////////////////////////////////////////////////////////////////////////

TMergeExecutor::TMergeExecutor()
    : InArg("", "in", "input tables", false, "ypath")
    , OutArg("", "out", "output table", false, "", "ypath")
    , ModeArg("", "mode", "merge mode", false, TMode(EMergeMode::Unordered), "unordered, ordered, sorted")
    , CombineArg("", "combine", "combine small output chunks into larger ones")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(ModeArg);
    CmdLine.add(CombineArg);
}

void TMergeExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto input = PreprocessYPaths(InArg.getValue());
    auto output = PreprocessYPath(OutArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").List(input)
            .Item("output_table_path").Scalar(output)
            .Item("mode").Scalar(FormatEnum(ModeArg.getValue().Get()))
            .Item("combine_chunks").Scalar(CombineArg.getValue())
            .Do(BIND(&TMergeExecutor::BuildOptions, Unretained(this)))
        .EndMap();

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TMergeExecutor::GetDriverCommandName() const
{
    return "merge";
}

EOperationType TMergeExecutor::GetOperationType() const
{
    return EOperationType::Merge;
}

//////////////////////////////////////////////////////////////////////////////////

TSortExecutor::TSortExecutor()
    : InArg("", "in", "input tables", false, "ypath")
    , OutArg("", "out", "output table", false, "", "ypath")
    , KeyColumnsArg("", "key_columns", "key columns names", true, "", "list_fragment")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(KeyColumnsArg);
}

void TSortExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto input = PreprocessYPaths(InArg.getValue());
    auto output = PreprocessYPath(OutArg.getValue());
    // TODO(babenko): refactor
    auto keyColumns = DeserializeFromYson< yvector<Stroka> >("[" + KeyColumnsArg.getValue() + "]");

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").List(input)
            .Item("output_table_path").Scalar(output)
            .Item("key_columns").List(keyColumns)
            .Do(BIND(&TSortExecutor::BuildOptions, Unretained(this)))
        .EndMap();
}

Stroka TSortExecutor::GetDriverCommandName() const
{
    return "sort";
}

EOperationType TSortExecutor::GetOperationType() const
{
    return EOperationType::Sort;
}

//////////////////////////////////////////////////////////////////////////////////

TEraseExecutor::TEraseExecutor()
    : InArg("", "in", "input table", false, "", "ypath")
    , OutArg("", "out", "output table", false, "", "ypath")
    , CombineArg("", "combine", "combine small output chunks into larger ones")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(CombineArg);
}

void TEraseExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto input = PreprocessYPath(InArg.getValue());
    auto output = PreprocessYPath(OutArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_path").Scalar(input)
            .Item("output_table_path").Scalar(output)
            .Item("combine_chunks").Scalar(CombineArg.getValue())
            .Do(BIND(&TEraseExecutor::BuildOptions, Unretained(this)))
        .EndMap();

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TEraseExecutor::GetDriverCommandName() const
{
    return "erase";
}

EOperationType TEraseExecutor::GetOperationType() const
{
    return EOperationType::Erase;
}

//////////////////////////////////////////////////////////////////////////////////

TAbortOpExecutor::TAbortOpExecutor()
    : OpArg("", "op", "id of an operation that must be aborted", true, "", "operation_id")
{
    CmdLine.add(OpArg);
}

void TAbortOpExecutor::BuildArgs(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("operation_id").Scalar(OpArg.getValue());

    TExecutorBase::BuildArgs(consumer);
}

Stroka TAbortOpExecutor::GetDriverCommandName() const
{
    return "abort_op";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
