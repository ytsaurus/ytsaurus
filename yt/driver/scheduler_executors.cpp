#include "scheduler_executors.h"
#include "preprocess.h"
#include "operation_tracker.h"

#include <server/job_proxy/config.h>
#include <ytlib/driver/driver.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/scheduler/helpers.h>
#include <ytlib/logging/log_manager.h>
#include <ytlib/object_client/object_service_proxy.h>

#include <util/stream/format.h>

namespace NYT {
namespace NDriver {

using namespace NFormats;
using namespace NYTree;
using namespace NScheduler;

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
        THROW_ERROR response.Error;
    }

    auto operationId = ConvertTo<TOperationId>(TYsonString(output.Str()));
    printf("done, %s\n", ~operationId.ToString());

    TOperationTracker tracker(Config, Driver, operationId);
    return tracker.Run();
}

//////////////////////////////////////////////////////////////////////////////////

TMapExecutor::TMapExecutor()
    : InArg("", "in", "input table path", false, "YPATH")
    , OutArg("", "out", "output table path", false, "YPATH")
    , CommandArg("", "command", "mapper shell command", true, "", "STRING")
    , FileArg("", "file", "additional file path", false, "YPATH")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(CommandArg);
    CmdLine.add(FileArg);
}

void TMapExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto inputs = PreprocessYPaths(InArg.getValue());
    auto outputs = PreprocessYPaths(OutArg.getValue());
    auto files = PreprocessYPaths(FileArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").List(inputs)
            .Item("output_table_paths").List(outputs)
            .Item("mapper").BeginMap()
                .Item("command").Scalar(CommandArg.getValue())
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
    , MergeByArg("", "merge_by", "columns to merge by (only used for sorted merge; "
        "if omitted then all input tables are assumed to have same key columns)",
        false, "", "YSON_LIST_FRAGMENT")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(ModeArg);
    CmdLine.add(CombineArg);
    CmdLine.add(MergeByArg);
}

void TMergeExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto inputs = PreprocessYPaths(InArg.getValue());
    auto outupts = PreprocessYPath(OutArg.getValue());
    auto mergeBy = ConvertTo< std::vector<Stroka> >(TYsonString(MergeByArg.getValue(), EYsonType::ListFragment));

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").List(inputs)
            .Item("output_table_path").Scalar(outupts)
            .Item("mode").Scalar(FormatEnum(ModeArg.getValue().Get()))
            .Item("combine_chunks").Scalar(CombineArg.getValue())
            .DoIf(!mergeBy.empty(), [=] (TFluentMap fluent) {
                fluent.Item("merge_by").List(mergeBy);
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
    , SortByArg("", "sort_by", "columns to sort by", true, "", "YSON_LIST_FRAGMENT")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(SortByArg);
}

void TSortExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto inputs = PreprocessYPaths(InArg.getValue());
    auto outputs = PreprocessYPath(OutArg.getValue());
    auto sortBy = ConvertTo< std::vector<Stroka> >(TYsonString(SortByArg.getValue(), EYsonType::ListFragment));

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").List(inputs)
            .Item("output_table_path").Scalar(outputs)
            .Item("sort_by").List(sortBy)
        .EndMap();

    TTransactedExecutor::BuildArgs(consumer);
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
    , CommandArg("", "command", "reducer shell command", true, "", "STRING")
    , FileArg("", "file", "additional file path", false, "YPATH")
    , ReduceByArg("", "reduce_by", "columns to reduce by"
        "(if omitted then all input tables are assumed to have same key columns)",
        false, "", "YSON_LIST_FRAGMENT")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(CommandArg);
    CmdLine.add(FileArg);
    CmdLine.add(ReduceByArg);
}

void TReduceExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto inputs = PreprocessYPaths(InArg.getValue());
    auto outputs = PreprocessYPaths(OutArg.getValue());
    auto files = PreprocessYPaths(FileArg.getValue());
    auto reduceBy = ConvertTo< std::vector<Stroka> >(TYsonString(ReduceByArg.getValue(), EYsonType::ListFragment));

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").List(inputs)
            .Item("output_table_paths").List(outputs)
            .DoIf(!reduceBy.empty(), [=] (TFluentMap fluent) {
                fluent.Item("reduce_by").List(reduceBy);
            })
            .Item("reducer").BeginMap()
                .Item("command").Scalar(CommandArg.getValue())
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

TMapReduceExecutor::TMapReduceExecutor()
    : InArg("", "in", "input table path", false, "YPATH")
    , OutArg("", "out", "output table path", false, "YPATH")
    , MapperCommandArg("", "mapper_command", "mapper shell command", false, "", "STRING")
    , MapperFileArg("", "mapper_file", "additional mapper file path", false, "YPATH")
    , ReducerCommandArg("", "reducer_command", "reducer shell command", true, "", "STRING")
    , ReducerFileArg("", "reducer_file", "additional reducer file path", false, "YPATH")
    , SortByArg("", "sort_by", "columns to sort by", true, "", "YSON_LIST_FRAGMENT")
    , ReduceByArg("", "reduce_by", "columns to reduce by (if not specified then assumed to be equal to \"sort_by\")", false, "", "YSON_LIST_FRAGMENT")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(MapperCommandArg);
    CmdLine.add(MapperFileArg);
    CmdLine.add(ReducerCommandArg);
    CmdLine.add(ReducerFileArg);
    CmdLine.add(SortByArg);
    CmdLine.add(ReduceByArg);
}

void TMapReduceExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto inputs = PreprocessYPaths(InArg.getValue());
    auto outputs = PreprocessYPaths(OutArg.getValue());
    auto mapperFiles = PreprocessYPaths(MapperFileArg.getValue());
    auto reducerFiles = PreprocessYPaths(ReducerFileArg.getValue());
    auto sortBy = ConvertTo< std::vector<Stroka> >(TYsonString(SortByArg.getValue(), EYsonType::ListFragment));
    auto reduceBy = ConvertTo< std::vector<Stroka> >(TYsonString(ReduceByArg.getValue(), EYsonType::ListFragment));

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").List(inputs)
            .Item("output_table_paths").List(outputs)
            .Item("sort_by").List(sortBy)
            .DoIf(!reduceBy.empty(), [&] (TFluentMap fluent) {
                fluent
                    .Item("reduce_by").List(reduceBy);
            })
            .DoIf(!MapperCommandArg.getValue().empty(), [&] (TFluentMap fluent) {
                fluent
                    .Item("mapper").BeginMap()
                        .Item("command").Scalar(MapperCommandArg.getValue())
                        .Item("file_paths").List(mapperFiles)
                    .EndMap();
            })
            .Item("reducer").BeginMap()
                .Item("command").Scalar(ReducerCommandArg.getValue())
                .Item("file_paths").List(reducerFiles)
            .EndMap()
       .EndMap();

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TMapReduceExecutor::GetCommandName() const
{
    return "map_reduce";
}

EOperationType TMapReduceExecutor::GetOperationType() const
{
    return EOperationType::MapReduce;
}

//////////////////////////////////////////////////////////////////////////////////

TAbortOpExecutor::TAbortOpExecutor()
    : OpArg("", "id of an operation to abort", true, "", "OP_ID")
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
    : OpArg("", "id of an operation to track", true, "", "OP_ID")
{
    CmdLine.add(OpArg);
}

EExitCode TTrackOpExecutor::Execute(const std::vector<std::string>& args)
{
    // TODO(babenko): get rid of this copy-paste
    auto argsCopy = args;
    CmdLine.parse(argsCopy);

    InitConfig();

    NLog::TLogManager::Get()->Configure(Config->Logging);

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
