#include "arguments.h"
#include "preprocess.h"

#include <build.h>

#include <ytlib/misc/home.h>
#include <ytlib/misc/fs.h>

#include <ytlib/ytree/tokenizer.h>
#include <ytlib/ytree/yson_format.h>

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

////////////////////////////////////////////////////////////////////////////////

static const char* UserConfigFileName = ".ytdriver.conf";
static const char* SystemConfigFileName = "ytdriver.conf";
static const char* SystemConfigPath = "/etc/";
static const char* ConfigEnvVar = "YT_CONFIG";

////////////////////////////////////////////////////////////////////////////////

TArgsParserBase::TArgsParserBase()
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

IMapNodePtr TArgsParserBase::ParseArgs(const std::vector<std::string>& args)
{
    auto argsCopy = args;
    CmdLine.parse(argsCopy);

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    builder->OnBeginMap();
    BuildArgs(~builder);
    builder->OnEndMap();
    return builder->EndTree()->AsMap();
}

Stroka TArgsParserBase::GetConfigFileName()
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

TArgsParserBase::TConfigPtr TArgsParserBase::ParseConfig()
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
    auto config = New<TConfig>();
    try {
        config->Load(~configNode);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing configuration\n%s", ex.what());
    }

    // Now convert back YSON tree to populate defaults.
    configNode = DeserializeFromYson(BIND(&TConfigurable::Save, config));

    // Patch config from command line.
    ApplyConfigUpdates(configNode);

    // And finally parse it again.
    try {
        config->Load(~configNode);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing configuration\n%s", ex.what());
    }

    return config;
}

TError TArgsParserBase::Execute(const std::vector<std::string>& args)
{
    auto config = ParseConfig();

    NLog::TLogManager::Get()->Configure(~config->Logging);

    
    auto driver = CreateDriver(config);

    TDriverRequest request;
    request.CommandName = GetDriverCommandName();
    request.InputStream = &StdInStream();
    // TODO(babenko): fixme
    request.InputFormat = TFormat(EFormatType::Yson);
    request.OutputStream = &StdOutStream();
    // TODO(babenko): fixme
    request.OutputFormat = TFormat(EFormatType::Yson);
    request.Arguments = ParseArgs(args);

    auto response = driver->Execute(request);
    return response.Error;
}

void TArgsParserBase::ApplyConfigUpdates(IYPathServicePtr service)
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


TFormat TArgsParserBase::GetFormat(TConfigPtr config, EDataType dataType, const Stroka& custom)
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
            return TFormat(EFormatType::Null);

        case EDataType::Structured:
            return TFormat::FromYson(config->FormatDefaults->Structured);

        case EDataType::Tabular:
            return TFormat::FromYson(config->FormatDefaults->Tabular);

        default:
            YUNREACHABLE();
    }
}

void TArgsParserBase::BuildOptions(IYsonConsumer* consumer)
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

void TArgsParserBase::BuildArgs(IYsonConsumer* consumer)
{
    UNUSED(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TTransactedArgsParser::TTransactedArgsParser()
    : TxArg("", "tx", "set transaction id", false, "", "transaction_id")
{
    CmdLine.add(TxArg);
}

void TTransactedArgsParser::BuildArgs(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .DoIf(TxArg.isSet(), [=] (TFluentMap fluent) {
            TYson txYson = TxArg.getValue();
            ValidateYson(txYson);
            fluent.Item("transaction_id").Node(txYson);
        });

    TArgsParserBase::BuildArgs(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TGetArgsParser::TGetArgsParser()
    : PathArg("path", "path to an object in Cypress that must be retrieved", true, "", "path")
{
    CmdLine.add(PathArg);
}

void TGetArgsParser::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedArgsParser::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TGetArgsParser::GetDriverCommandName() const
{
    return "get";
}

////////////////////////////////////////////////////////////////////////////////

TSetArgsParser::TSetArgsParser()
    : PathArg("path", "path to an object in Cypress that must be set", true, "", "path")
    , ValueArg("value", "value to set", true, "", "yson")
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
}

void TSetArgsParser::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path)
        .Item("value").Node(ValueArg.getValue());

    TTransactedArgsParser::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TSetArgsParser::GetDriverCommandName() const
{
    return "set";
}

////////////////////////////////////////////////////////////////////////////////

TRemoveArgsParser::TRemoveArgsParser()
    : PathArg("path", "path to an object in Cypress that must be removed", true, "", "path")
{
    CmdLine.add(PathArg);
}

void TRemoveArgsParser::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedArgsParser::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TRemoveArgsParser::GetDriverCommandName() const
{
    return "remove";
}

////////////////////////////////////////////////////////////////////////////////

TListArgsParser::TListArgsParser()
    : PathArg("path", "path to a object in Cypress whose children must be listed", true, "", "path")
{
    CmdLine.add(PathArg);
}

void TListArgsParser::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);
 
    TTransactedArgsParser::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TListArgsParser::GetDriverCommandName() const
{
    return "list";
}

////////////////////////////////////////////////////////////////////////////////

TCreateArgsParser::TCreateArgsParser()
    : TypeArg("type", "type of node", true, NObjectServer::EObjectType::Null, "object type")
    , PathArg("path", "path for a new object in Cypress", true, "", "ypath")
{
    CmdLine.add(TypeArg);
    CmdLine.add(PathArg);
}

void TCreateArgsParser::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path)
        .Item("type").Scalar(TypeArg.getValue().ToString());

    TTransactedArgsParser::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TCreateArgsParser::GetDriverCommandName() const
{
    return "create";
}

////////////////////////////////////////////////////////////////////////////////

TLockArgsParser::TLockArgsParser()
    : PathArg("path", "path to an object in Cypress that must be locked", true, "", "path")
    , ModeArg("", "mode", "lock mode", false, NCypress::ELockMode::Exclusive, "snapshot, shared, exclusive")
{
    CmdLine.add(PathArg);
    CmdLine.add(ModeArg);
}

void TLockArgsParser::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path)
        .Item("mode").Scalar(ModeArg.getValue().ToString());

    TTransactedArgsParser::BuildArgs(consumer);
}

Stroka TLockArgsParser::GetDriverCommandName() const
{
    return "lock";
}

//////////////////////////////////////////////////////////////////////////////////

//void TStartTxArgsParser::BuildArgs(IYsonConsumer* consumer)
//{
//    TTransactedArgsParser::BuildArgs(consumer);
//    BuildOptions(consumer);
//}

//Stroka TStartTxArgsParser::GetDriverCommandName() const
//{
//    return "start_tx";
//}

//////////////////////////////////////////////////////////////////////////////////

//Stroka TRenewTxArgsParser::GetDriverCommandName() const
//{
//    return "renew_tx";
//}

//////////////////////////////////////////////////////////////////////////////////

//Stroka TCommitTxArgsParser::GetDriverCommandName() const
//{
//    return "commit_tx";
//}

//////////////////////////////////////////////////////////////////////////////////

//Stroka TAbortTxArgsParser::GetDriverCommandName() const
//{
//    return "abort_tx";
//}

//////////////////////////////////////////////////////////////////////////////////

//TReadArgsParser::TReadArgsParser()
//    : PathArg("path", "path to a table in Cypress that must be read", true, "", "ypath")
//{
//    CmdLine.add(PathArg);
//}

//void TReadArgsParser::BuildArgs(IYsonConsumer* consumer)
//{
//    auto path = PreprocessYPath(PathArg.getValue());

//    BuildYsonMapFluently(consumer)
//        .Item("do").Scalar("read")
//        .Item("path").Scalar(path);

//    TTransactedArgsParser::BuildArgs(consumer);
//}

//Stroka TReadArgsParser::GetDriverCommandName() const
//{
//    return "read";
//}

//////////////////////////////////////////////////////////////////////////////////

//TWriteArgsParser::TWriteArgsParser()
//    : PathArg("path", "path to a table in Cypress that must be written", true, "", "ypath")
//    , ValueArg("value", "row(s) to write", false, "", "yson")
//    , KeyColumnsArg("", "sorted", "key columns names (table must initially be empty, input data must be sorted)", false, "", "list_fragment")
//{
//    CmdLine.add(PathArg);
//    CmdLine.add(ValueArg);
//    CmdLine.add(KeyColumnsArg);
//}

//void TWriteArgsParser::BuildArgs(IYsonConsumer* consumer)
//{
//    auto path = PreprocessYPath(PathArg.getValue());
//    auto value = ValueArg.getValue();
//    // TODO(babenko): refactor
//    auto keyColumns = DeserializeFromYson< yvector<Stroka> >("[" + KeyColumnsArg.getValue() + "]");

//    BuildYsonMapFluently(consumer)
//        .Item("do").Scalar("write")
//        .Item("path").Scalar(path)
//        .DoIf(!keyColumns.empty(), [=] (TFluentMap fluent) {
//            fluent.Item("sorted").Scalar(true);
//            fluent.Item("key_columns").List(keyColumns);
//        })
//        .DoIf(!value.empty(), [=] (TFluentMap fluent) {
//                fluent.Item("value").Node(value);
//        });

//    TTransactedArgsParser::BuildArgs(consumer);
//}

//Stroka TWriteArgsParser::GetDriverCommandName() const
//{
//    return "write";
//}

//////////////////////////////////////////////////////////////////////////////////

//TUploadArgsParser::TUploadArgsParser()
//    : PathArg("path", "to a new file in Cypress that must be uploaded", true, "", "ypath")
//{
//    CmdLine.add(PathArg);
//}

//void TUploadArgsParser::BuildArgs(IYsonConsumer* consumer)
//{
//    auto path = PreprocessYPath(PathArg.getValue());

//    BuildYsonMapFluently(consumer)
//        .Item("path").Scalar(path);

//    TTransactedArgsParser::BuildArgs(consumer);
//}

//Stroka TUploadArgsParser::GetDriverCommandName() const
//{
//    return "upload";
//}

//////////////////////////////////////////////////////////////////////////////////

//TDownloadArgsParser::TDownloadArgsParser()
//    : PathArg("path", "path to a file in Cypress that must be downloaded", true, "", "ypath")
//{
//    CmdLine.add(PathArg);
//}

//void TDownloadArgsParser::BuildArgs(IYsonConsumer* consumer)
//{
//    auto path = PreprocessYPath(PathArg.getValue());

//    BuildYsonMapFluently(consumer)
//        .Item("path").Scalar(path);

//    TTransactedArgsParser::BuildArgs(consumer);
//}

//Stroka TDownloadArgsParser::GetDriverCommandName() const
//{
//    return "download";
//}

//////////////////////////////////////////////////////////////////////////////////

//class TStartOpArgsParser::TOperationTracker
//{
//public:
//    TOperationTracker(
//        TArgsParserBase::TConfigPtr config,
//        IDriverPtr driver,
//        const TOperationId& operationId,
//        EOperationType operationType)
//        : Config(config)
//        , Driver(driver)
//        , OperationId(operationId)
//        , OperationType(operationType)
//    { }

//    void Run()
//    {
//        TSchedulerServiceProxy proxy(Driver->GetCommandHost()->GetSchedulerChannel());

//        while (true)  {
//            auto waitOpReq = proxy.WaitForOperation();
//            *waitOpReq->mutable_operation_id() = OperationId.ToProto();
//            waitOpReq->set_timeout(Config->OperationWaitTimeout.GetValue());

//            // Override default timeout.
//            waitOpReq->SetTimeout(Config->OperationWaitTimeout * 2);
//            auto waitOpRsp = waitOpReq->Invoke().Get();

//            if (!waitOpRsp->IsOK()) {
//                ythrow yexception() << waitOpRsp->GetError().ToString();
//            }

//            if (waitOpRsp->finished())
//                break;

//            DumpProgress();
//        }

//        DumpResult();
//    }

//private:
//    TArgsParserBase::TConfigPtr Config;
//    IDriverPtr Driver;
//    TOperationId OperationId;
//    EOperationType OperationType;

//    // TODO(babenko): refactor
//    // TODO(babenko): YPath and RPC responses currently share no base class.
//    template <class TResponse>
//    static void CheckResponse(TResponse response, const Stroka& failureMessage)
//    {
//        if (response->IsOK())
//            return;

//        ythrow yexception() << failureMessage + "\n" + response->GetError().ToString();
//    }

//    static void AppendPhaseProgress(Stroka* out, const Stroka& phase, const TYson& progress)
//    {
//        i64 jobsTotal = DeserializeFromYson<i64>(progress, "/total");
//        if (jobsTotal == 0) {
//            return;
//        }
        
//        i64 jobsCompleted = DeserializeFromYson<i64>(progress, "/completed");
//        int percentComplete  = (jobsCompleted * 100) / jobsTotal;

//        if (!out->empty()) {
//            out->append(", ");
//        }

//        out->append(Sprintf("%3d%% ", percentComplete));
//        if (!phase.empty()) {
//            out->append(phase);
//            out->append(' ');
//        }

//        out->append("done ");

//        // Some simple pretty-printing.
//        int totalWidth = ToString(jobsTotal).length();
//        out->append("(");
//        out->append(ToString(LeftPad(ToString(jobsCompleted), totalWidth)));
//        out->append(" of ");
//        out->append(ToString(jobsTotal));
//        out->append(")");
//    }

//    Stroka FormatProgress(const TYson& progress)
//    {
//        // TODO(babenko): refactor
//        auto progressAttributes = IAttributeDictionary::FromMap(DeserializeFromYson(progress)->AsMap());
//        Stroka result;
//        switch (OperationType) {
//            case EOperationType::Map:
//            case EOperationType::Merge:
//            case EOperationType::Erase:
//                AppendPhaseProgress(&result, "", progressAttributes->GetYson("jobs"));
//                break;
                                        
//            case EOperationType::Sort:
//                AppendPhaseProgress(&result, "partition", progressAttributes->GetYson("partition_jobs"));
//                AppendPhaseProgress(&result, "sort", progressAttributes->GetYson("sort_jobs"));
//                AppendPhaseProgress(&result, "merge", progressAttributes->GetYson("merge_jobs"));
//                break;

//            default:
//                YUNREACHABLE();
//        }
//        return result;
//    }

//    void DumpProgress()
//    {
//        auto operationPath = GetOperationPath(OperationId);

//        TObjectServiceProxy proxy(Driver->GetCommandHost()->GetMasterChannel());
//        auto batchReq = proxy.ExecuteBatch();

//        {
//            auto req = TYPathProxy::Get(operationPath + "/@state");
//            batchReq->AddRequest(req, "get_state");
//        }

//        {
//            auto req = TYPathProxy::Get(operationPath + "/@progress");
//            batchReq->AddRequest(req, "get_progress");
//        }

//        auto batchRsp = batchReq->Invoke().Get();
//        CheckResponse(batchRsp, "Error getting operation progress");

//        EOperationState state;
//        {
//            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_state");
//            CheckResponse(rsp, "Error getting operation state");
//            state = DeserializeFromYson<EOperationState>(rsp->value());
//        }

//        TYson progress;
//        {
//            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_progress");
//            CheckResponse(rsp, "Error getting operation progress");
//            progress = rsp->value();
//        }

//        if (state == EOperationState::Running) {
//            printf("%s: %s\n",
//                ~state.ToString(),
//                ~FormatProgress(progress));
//        } else {
//            printf("%s\n", ~state.ToString());
//        }
//    }

//    void DumpResult()
//    {
//        auto operationPath = GetOperationPath(OperationId);

//        TObjectServiceProxy proxy(Driver->GetCommandHost()->GetMasterChannel());
//        auto batchReq = proxy.ExecuteBatch();

//        {
//            auto req = TYPathProxy::Get(operationPath + "/@result");
//            batchReq->AddRequest(req, "get_result");
//        }

//        auto batchRsp = batchReq->Invoke().Get();
//        CheckResponse(batchRsp, "Error getting operation result");

//        {
//            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_result");
//            CheckResponse(rsp, "Error getting operation result");
//            // TODO(babenko): refactor!
//            auto errorNode = DeserializeFromYson<INodePtr>(rsp->value(), "/error");
//            auto error = TError::FromYson(errorNode);
//            if (!error.IsOK()) {
//                ythrow yexception() << error.ToString();
//            }
//        }

//        printf("Operation completed successfully\n");
//    }
//};

//////////////////////////////////////////////////////////////////////////////////

//TStartOpArgsParser::TStartOpArgsParser()
//    : NoTrackArg("", "no_track", "don't track operation progress")
//{
//    CmdLine.add(NoTrackArg);
//}

//TError TStartOpArgsParser::Execute(const std::vector<std::string>& args)
//{
//    if (NoTrackArg.getValue()) {
//        return TArgsParserBase::Execute(args);
//    }

//    auto request = ParseArgs(args);
//    auto config = ParseConfig();

//    printf("Starting %s operation... ", ~GetDriverCommandName().Quote());

//    TInterceptingDriverHost driverHost;
//    auto driver = CreateDriver(config, &driverHost);
//    auto error = driver->Execute(GetDriverCommandName(), request);

//    if (!error.IsOK()) {
//        printf("failed\n");
//        ythrow yexception() << error.ToString();
//    }

//    auto operationId = DeserializeFromYson<TOperationId>(driverHost.GetOutput());
//    printf("done, %s\n", ~operationId.ToString());

//    TOperationTracker tracker(config, driver, operationId, GetOperationType());
//    tracker.Run();

//    return TError();
//}

//////////////////////////////////////////////////////////////////////////////////

//TMapArgsParser::TMapArgsParser()
//    : InArg("", "in", "input tables", false, "ypath")
//    , OutArg("", "out", "output tables", false, "ypath")
//    , FilesArg("", "file", "additional files", false, "ypath")
//    , MapperArg("", "mapper", "mapper shell command", true, "", "command")
//{
//    CmdLine.add(InArg);
//    CmdLine.add(OutArg);
//    CmdLine.add(FilesArg);
//    CmdLine.add(MapperArg);
//}

//void TMapArgsParser::BuildArgs(IYsonConsumer* consumer)
//{
//    auto input = PreprocessYPaths(InArg.getValue());
//    auto output = PreprocessYPaths(OutArg.getValue());
//    auto files = PreprocessYPaths(FilesArg.getValue());

//    BuildYsonMapFluently(consumer)
//        .Item("spec").BeginMap()
//            .Item("mapper").Scalar(MapperArg.getValue())
//            .Item("input_table_paths").List(input)
//            .Item("output_table_paths").List(output)
//            .Item("file_paths").List(files)
//            .Do(BIND(&TMapArgsParser::BuildOptions, Unretained(this)))
//        .EndMap();

//    TTransactedArgsParser::BuildArgs(consumer);
//}

//Stroka TMapArgsParser::GetDriverCommandName() const
//{
//    return "map";
//}

//EOperationType TMapArgsParser::GetOperationType() const
//{
//    return EOperationType::Map;
//}

//////////////////////////////////////////////////////////////////////////////////

//TMergeArgsParser::TMergeArgsParser()
//    : InArg("", "in", "input tables", false, "ypath")
//    , OutArg("", "out", "output table", false, "", "ypath")
//    , ModeArg("", "mode", "merge mode", false, TMode(EMergeMode::Unordered), "unordered, ordered, sorted")
//    , CombineArg("", "combine", "combine small output chunks into larger ones")
//{
//    CmdLine.add(InArg);
//    CmdLine.add(OutArg);
//    CmdLine.add(ModeArg);
//    CmdLine.add(CombineArg);
//}

//void TMergeArgsParser::BuildArgs(IYsonConsumer* consumer)
//{
//    auto input = PreprocessYPaths(InArg.getValue());
//    auto output = PreprocessYPath(OutArg.getValue());

//    BuildYsonMapFluently(consumer)
//        .Item("spec").BeginMap()
//            .Item("input_table_paths").List(input)
//            .Item("output_table_path").Scalar(output)
//            .Item("mode").Scalar(FormatEnum(ModeArg.getValue().Get()))
//            .Item("combine_chunks").Scalar(CombineArg.getValue())
//            .Do(BIND(&TMergeArgsParser::BuildOptions, Unretained(this)))
//        .EndMap();

//    TTransactedArgsParser::BuildArgs(consumer);
//}

//Stroka TMergeArgsParser::GetDriverCommandName() const
//{
//    return "merge";
//}

//EOperationType TMergeArgsParser::GetOperationType() const
//{
//    return EOperationType::Merge;
//}

//////////////////////////////////////////////////////////////////////////////////

//TSortArgsParser::TSortArgsParser()
//    : InArg("", "in", "input tables", false, "ypath")
//    , OutArg("", "out", "output table", false, "", "ypath")
//    , KeyColumnsArg("", "key_columns", "key columns names", true, "", "list_fragment")
//{
//    CmdLine.add(InArg);
//    CmdLine.add(OutArg);
//    CmdLine.add(KeyColumnsArg);
//}

//void TSortArgsParser::BuildArgs(IYsonConsumer* consumer)
//{
//    auto input = PreprocessYPaths(InArg.getValue());
//    auto output = PreprocessYPath(OutArg.getValue());
//    // TODO(babenko): refactor
//    auto keyColumns = DeserializeFromYson< yvector<Stroka> >("[" + KeyColumnsArg.getValue() + "]");

//    BuildYsonMapFluently(consumer)
//        .Item("spec").BeginMap()
//            .Item("input_table_paths").List(input)
//            .Item("output_table_path").Scalar(output)
//            .Item("key_columns").List(keyColumns)
//            .Do(BIND(&TSortArgsParser::BuildOptions, Unretained(this)))
//        .EndMap();
//}

//Stroka TSortArgsParser::GetDriverCommandName() const
//{
//    return "sort";
//}

//EOperationType TSortArgsParser::GetOperationType() const
//{
//    return EOperationType::Sort;
//}

//////////////////////////////////////////////////////////////////////////////////

//TEraseArgsParser::TEraseArgsParser()
//    : InArg("", "in", "input table", false, "", "ypath")
//    , OutArg("", "out", "output table", false, "", "ypath")
//    , CombineArg("", "combine", "combine small output chunks into larger ones")
//{
//    CmdLine.add(InArg);
//    CmdLine.add(OutArg);
//    CmdLine.add(CombineArg);
//}

//void TEraseArgsParser::BuildArgs(IYsonConsumer* consumer)
//{
//    auto input = PreprocessYPath(InArg.getValue());
//    auto output = PreprocessYPath(OutArg.getValue());

//    BuildYsonMapFluently(consumer)
//        .Item("spec").BeginMap()
//            .Item("input_table_path").Scalar(input)
//            .Item("output_table_path").Scalar(output)
//            .Item("combine_chunks").Scalar(CombineArg.getValue())
//            .Do(BIND(&TEraseArgsParser::BuildOptions, Unretained(this)))
//        .EndMap();

//    TTransactedArgsParser::BuildArgs(consumer);
//}

//Stroka TEraseArgsParser::GetDriverCommandName() const
//{
//    return "erase";
//}

//EOperationType TEraseArgsParser::GetOperationType() const
//{
//    return EOperationType::Erase;
//}

//////////////////////////////////////////////////////////////////////////////////

//TAbortOpArgsParser::TAbortOpArgsParser()
//    : OpArg("", "op", "id of an operation that must be aborted", true, "", "operation_id")
//{
//    CmdLine.add(OpArg);
//}

//void TAbortOpArgsParser::BuildArgs(IYsonConsumer* consumer)
//{
//    BuildYsonMapFluently(consumer)
//        .Item("operation_id").Scalar(OpArg.getValue());

//    TArgsParserBase::BuildArgs(consumer);
//}

//Stroka TAbortOpArgsParser::GetDriverCommandName() const
//{
//    return "abort_op";
//}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
