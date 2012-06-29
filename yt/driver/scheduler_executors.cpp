#include "scheduler_executors.h"
#include "preprocess.h"
#include "operation_tracker.h"

#include <ytlib/job_proxy/config.h>
#include <ytlib/driver/driver.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/scheduler/helpers.h>
#include <ytlib/logging/log_manager.h>
#include <ytlib/object_server/object_service_proxy.h>

#include <util/stream/format.h>

namespace NYT {
namespace NDriver {

using namespace NFormats;
using namespace NYTree;
using namespace NScheduler;
using namespace NObjectServer;

//////////////////////////////////////////////////////////////////////////////////

TStartOpExecutor::TStartOpExecutor()
    : DontTrackArg("", "dont_track", "don't track operation progress")
{
    CmdLine.add(DontTrackArg);
}

EExitCode TStartOpExecutor::DoExecute(const TDriverRequest& request)
{
    if (DontTrackArg.getValue()) {
        return TExecutor::DoExecute(request);
    }

    printf("Starting %s operation... ", ~GetCommandName().Quote());

    TDriverRequest requestCopy = request;

    TStringStream output;
    requestCopy.OutputFormat = TFormat(EFormatType::Yson);
    requestCopy.OutputStream = &output;

    auto response = Driver->Execute(requestCopy);
    if (!response.Error.IsOK()) {
        printf("failed\n");
        ythrow yexception() << response.Error.ToString();
    }

    // TODO(sandello): This is VERY weird.
    auto operationId = ConvertTo<TOperationId>(TYsonString(output.Str()));
    printf("done, %s\n", ~operationId.ToString());

    TOperationTracker tracker(Config, Driver, operationId);
    return tracker.Run();
}

//////////////////////////////////////////////////////////////////////////////////

TMapExecutor::TMapExecutor()
    : InArg("", "in", "input table path", false, "YPATH")
    , OutArg("", "out", "output table path", false, "YPATH")
    , FilesArg("", "file", "additional file path", false, "YPATH")
    , MapperArg("", "mapper", "mapper shell command", true, "", "STRING")
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
            .Item("input_table_paths").List(input)
            .Item("output_table_paths").List(output)
            .Item("mapper").BeginMap()
                .Item("command").Scalar(MapperArg.getValue())
                .Item("file_paths").List(files)
            .EndMap()
        .EndMap();

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TMapExecutor::GetCommandName() const
{
    return "map";
}

EOperationType TMapExecutor::GetOperationType() const
{
    return EOperationType::Map;
}

//////////////////////////////////////////////////////////////////////////////////

TMergeExecutor::TMergeExecutor()
    : InArg("", "in", "input table path", false, "YPATH")
    , OutArg("", "out", "output table path", false, "", "YPATH")
    , ModeArg("", "mode", "merge mode", false, TMode(EMergeMode::Unordered), "unordered, ordered, sorted")
    , CombineArg("", "combine", "combine small output chunks into larger ones")
    , KeyColumnsArg("", "key_columns", "key columns names (only used for sorted merge; "
        "if omitted then all input tables are assumed to have same key columns)",
        false, "", "YSON_LIST_FRAGMENT")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(ModeArg);
    CmdLine.add(CombineArg);
    CmdLine.add(KeyColumnsArg);
}

void TMergeExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto input = PreprocessYPaths(InArg.getValue());
    auto output = PreprocessYPath(OutArg.getValue());
    // TODO(babenko): refactor
    auto keyColumns = ConvertTo< std::vector<Stroka> >(TYsonString(KeyColumnsArg.getValue(), EYsonType::ListFragment));

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").List(input)
            .Item("output_table_path").Scalar(output)
            .Item("mode").Scalar(FormatEnum(ModeArg.getValue().Get()))
            .Item("combine_chunks").Scalar(CombineArg.getValue())
            .DoIf(!keyColumns.empty(), [=] (TFluentMap fluent) {
                fluent.Item("key_columns").List(keyColumns);
            })
        .EndMap();

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TMergeExecutor::GetCommandName() const
{
    return "merge";
}

EOperationType TMergeExecutor::GetOperationType() const
{
    return EOperationType::Merge;
}

//////////////////////////////////////////////////////////////////////////////////

TSortExecutor::TSortExecutor()
    : InArg("", "in", "input table path", false, "YPATH")
    , OutArg("", "out", "output table path", false, "", "YPATH")
    , KeyColumnsArg("", "key_columns", "key columns names", true, "", "YSON_LIST_FRAGMENT")
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
    auto keyColumns = ConvertTo< std::vector<Stroka> >(TYsonString(KeyColumnsArg.getValue(), EYsonType::ListFragment));

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").List(input)
            .Item("output_table_path").Scalar(output)
            .Item("key_columns").List(keyColumns)
        .EndMap();
}

Stroka TSortExecutor::GetCommandName() const
{
    return "sort";
}

EOperationType TSortExecutor::GetOperationType() const
{
    return EOperationType::Sort;
}

//////////////////////////////////////////////////////////////////////////////////

TEraseExecutor::TEraseExecutor()
    : PathArg("path", "path to a table where rows must be removed", true, "", "YPATH")
    , CombineArg("", "combine", "combine small output chunks into larger ones")
{
    CmdLine.add(PathArg);
    CmdLine.add(CombineArg);
}

void TEraseExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("table_path").Scalar(path)
            .Item("combine_chunks").Scalar(CombineArg.getValue())
        .EndMap();

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TEraseExecutor::GetCommandName() const
{
    return "erase";
}

EOperationType TEraseExecutor::GetOperationType() const
{
    return EOperationType::Erase;
}

//////////////////////////////////////////////////////////////////////////////////

TReduceExecutor::TReduceExecutor()
    : InArg("", "in", "input table path", false, "YPATH")
    , OutArg("", "out", "output table path", false, "YPATH")
    , FilesArg("", "file", "additional file path", false, "YPATH")
    , ReducerArg("", "reducer", "reducer shell command", true, "", "STRING")
    , KeyColumnsArg("", "key_columns", "key columns names "
        "(if omitted then all input tables are assumed to have same key columns)",
        false, "", "YSON_LIST_FRAGMENT")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(FilesArg);
    CmdLine.add(ReducerArg);
    CmdLine.add(KeyColumnsArg);
}

void TReduceExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto input = PreprocessYPaths(InArg.getValue());
    auto output = PreprocessYPaths(OutArg.getValue());
    auto files = PreprocessYPaths(FilesArg.getValue());
    auto keyColumns = KeyColumnsArg.getValue();

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").List(input)
            .Item("output_table_paths").List(output)
            .DoIf(!keyColumns.empty(), [=] (TFluentMap fluent) {
                fluent.Item("key_columns").List(keyColumns);
            })
            .Item("reducer").BeginMap()
                .Item("command").Scalar(ReducerArg.getValue())
                .Item("file_paths").List(files)
            .EndMap()
        .EndMap();

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TReduceExecutor::GetCommandName() const
{
    return "reduce";
}

EOperationType TReduceExecutor::GetOperationType() const
{
    return EOperationType::Reduce;
}

//////////////////////////////////////////////////////////////////////////////////

TAbortOpExecutor::TAbortOpExecutor()
    : OpArg("", "op", "id of an operation that must be aborted", true, "", "GUID")
{
    CmdLine.add(OpArg);
}

void TAbortOpExecutor::BuildArgs(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("operation_id").Scalar(OpArg.getValue());

    TExecutor::BuildArgs(consumer);
}

Stroka TAbortOpExecutor::GetCommandName() const
{
    return "abort_op";
}

////////////////////////////////////////////////////////////////////////////////

TTrackOpExecutor::TTrackOpExecutor()
    : OpArg("", "op", "id of an operation that must be tracked", true, "", "GUID")
{
    CmdLine.add(OpArg);
}

EExitCode TTrackOpExecutor::Execute(const std::vector<std::string>& args)
{
    // TODO(babenko): get rid of this copy-paste
    auto argsCopy = args;
    CmdLine.parse(argsCopy);

    InitConfig();

    NLog::TLogManager::Get()->Configure(~Config->Logging);

    Driver = CreateDriver(Config);

    auto operationId = TOperationId::FromString(OpArg.getValue());
    printf("Started tracking operation %s\n", ~operationId.ToString());

    TOperationTracker tracker(Config, Driver, operationId);
    return tracker.Run();
}

void TTrackOpExecutor::BuildArgs(IYsonConsumer* consumer)
{ }

Stroka TTrackOpExecutor::GetCommandName() const
{
    return "track_op";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
