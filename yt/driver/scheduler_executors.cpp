#include "scheduler_executors.h"
#include "preprocess.h"
#include "operation_tracker.h"

#include <server/job_proxy/config.h>
#include <ytlib/driver/driver.h>
#include <core/ytree/ypath_proxy.h>
#include <ytlib/scheduler/scheduler_service_proxy.h>
#include <ytlib/scheduler/helpers.h>
#include <core/logging/log_manager.h>
#include <ytlib/object_client/object_service_proxy.h>

#include <util/stream/format.h>

namespace NYT {
namespace NDriver {

using namespace NFormats;
using namespace NYTree;
using namespace NYson;
using namespace NScheduler;
using namespace NConcurrency;

//////////////////////////////////////////////////////////////////////////////////

TStartOpExecutor::TStartOpExecutor()
    : DontTrackArg("", "dont_track", "don't track operation progress")
{
    CmdLine.add(DontTrackArg);
}

void TStartOpExecutor::DoExecute(const TDriverRequest& request)
{
    if (DontTrackArg.getValue()) {
        TRequestExecutor::DoExecute(request);
        return;
    }

    printf("Starting %s operation... ", ~FormatEnum(GetOperationType()).Quote());

    TDriverRequest requestCopy = request;

    Stroka str;
    TStringOutput output(str);
    requestCopy.Parameters->AddChild(
        ConvertToNode(TFormat(EFormatType::Yson)),
        "input_format");
    requestCopy.Parameters->AddChild(
        ConvertToNode(TFormat(EFormatType::Yson)),
        "output_format");

    requestCopy.OutputStream = CreateAsyncAdapter(&output);

    auto error = Driver->Execute(requestCopy).Get();
    if (!error.IsOK()) {
        printf("failed\n");
        THROW_ERROR error;
    }

    auto operationId = ConvertTo<TOperationId>(TYsonString(str));
    printf("done, %s\n", ~ToString(operationId));

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

void TMapExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto inputs = PreprocessYPaths(InArg.getValue());
    auto outputs = PreprocessYPaths(OutArg.getValue());
    auto files = PreprocessYPaths(FileArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").Value(inputs)
            .Item("output_table_paths").Value(outputs)
            .Item("mapper").BeginMap()
                .Item("command").Value(CommandArg.getValue())
                .Item("file_paths").Value(files)
            .EndMap()
        .EndMap();

    TTransactedExecutor::BuildParameters(consumer);
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

void TMergeExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto inputs = PreprocessYPaths(InArg.getValue());
    auto outputs = PreprocessYPath(OutArg.getValue());
    auto mergeBy = ConvertTo< std::vector<Stroka> >(TYsonString(MergeByArg.getValue(), EYsonType::ListFragment));

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").Value(inputs)
            .Item("output_table_path").Value(outputs)
            .Item("mode").Value(FormatEnum(ModeArg.getValue().Get()))
            .Item("combine_chunks").Value(CombineArg.getValue())
            .DoIf(!mergeBy.empty(), [=] (TFluentMap fluent) {
                fluent.Item("merge_by").Value(mergeBy);
            })
        .EndMap();

    TTransactedExecutor::BuildParameters(consumer);
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

void TSortExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto inputs = PreprocessYPaths(InArg.getValue());
    auto outputs = PreprocessYPath(OutArg.getValue());
    auto sortBy = ConvertTo< std::vector<Stroka> >(TYsonString(SortByArg.getValue(), EYsonType::ListFragment));

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").Value(inputs)
            .Item("output_table_path").Value(outputs)
            .Item("sort_by").Value(sortBy)
        .EndMap();

    TTransactedExecutor::BuildParameters(consumer);
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
    : PathArg("path", "table path to erase", true, "", "YPATH")
    , CombineArg("", "combine", "combine small output chunks into larger ones")
{
    CmdLine.add(PathArg);
    CmdLine.add(CombineArg);
}

void TEraseExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("table_path").Value(path)
            .Item("combine_chunks").Value(CombineArg.getValue())
        .EndMap();

    TTransactedExecutor::BuildParameters(consumer);
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

void TReduceExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto inputs = PreprocessYPaths(InArg.getValue());
    auto outputs = PreprocessYPaths(OutArg.getValue());
    auto files = PreprocessYPaths(FileArg.getValue());
    auto reduceBy = ConvertTo< std::vector<Stroka> >(TYsonString(ReduceByArg.getValue(), EYsonType::ListFragment));

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").Value(inputs)
            .Item("output_table_paths").Value(outputs)
            .DoIf(!reduceBy.empty(), [=] (TFluentMap fluent) {
                fluent.Item("reduce_by").Value(reduceBy);
            })
            .Item("reducer").BeginMap()
                .Item("command").Value(CommandArg.getValue())
                .Item("file_paths").Value(files)
            .EndMap()
        .EndMap();

    TTransactedExecutor::BuildParameters(consumer);
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
    , ReduceCombinerCommandArg("", "reduce_combiner_command", "reduce_combiner shell command", false, "", "STRING")
    , ReduceCombinerFileArg("", "reduce_combiner_file", "additional reduce_combiner file path", false, "YPATH")
    , ReducerCommandArg("", "reducer_command", "reducer shell command", true, "", "STRING")
    , ReducerFileArg("", "reducer_file", "additional reducer file path", false, "YPATH")
    , SortByArg("", "sort_by", "columns to sort by", true, "", "YSON_LIST_FRAGMENT")
    , ReduceByArg("", "reduce_by", "columns to reduce by (if not specified then assumed to be equal to \"sort_by\")", false, "", "YSON_LIST_FRAGMENT")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(MapperCommandArg);
    CmdLine.add(MapperFileArg);
    CmdLine.add(ReduceCombinerCommandArg);
    CmdLine.add(ReduceCombinerFileArg);
    CmdLine.add(ReducerCommandArg);
    CmdLine.add(ReducerFileArg);
    CmdLine.add(SortByArg);
    CmdLine.add(ReduceByArg);
}

void TMapReduceExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto inputs = PreprocessYPaths(InArg.getValue());
    auto outputs = PreprocessYPaths(OutArg.getValue());
    auto mapperFiles = PreprocessYPaths(MapperFileArg.getValue());
    auto reduceCombinerFiles = PreprocessYPaths(ReduceCombinerFileArg.getValue());
    auto reducerFiles = PreprocessYPaths(ReducerFileArg.getValue());
    auto sortBy = ConvertTo< std::vector<Stroka> >(TYsonString(SortByArg.getValue(), EYsonType::ListFragment));
    auto reduceBy = ConvertTo< std::vector<Stroka> >(TYsonString(ReduceByArg.getValue(), EYsonType::ListFragment));

    BuildYsonMapFluently(consumer)
        .Item("spec").BeginMap()
            .Item("input_table_paths").Value(inputs)
            .Item("output_table_paths").Value(outputs)
            .Item("sort_by").Value(sortBy)
            .DoIf(!reduceBy.empty(), [&] (TFluentMap fluent) {
                fluent
                    .Item("reduce_by").Value(reduceBy);
            })
            .DoIf(!MapperCommandArg.getValue().empty(), [&] (TFluentMap fluent) {
                fluent
                    .Item("mapper").BeginMap()
                        .Item("command").Value(MapperCommandArg.getValue())
                        .Item("file_paths").Value(mapperFiles)
                    .EndMap();
            })
            .DoIf(!ReduceCombinerCommandArg.getValue().empty(), [&] (TFluentMap fluent) {
                fluent
                    .Item("reduce_combiner").BeginMap()
                        .Item("command").Value(ReduceCombinerCommandArg.getValue())
                        .Item("file_paths").Value(reduceCombinerFiles)
                    .EndMap();
            })
            .Item("reducer").BeginMap()
                .Item("command").Value(ReducerCommandArg.getValue())
                .Item("file_paths").Value(reducerFiles)
            .EndMap()
       .EndMap();

    TTransactedExecutor::BuildParameters(consumer);
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

TAbortOperationExecutor::TAbortOperationExecutor()
    : OpArg("", "id of an operation to abort", true, "", "OP_ID")
{
    CmdLine.add(OpArg);
}

void TAbortOperationExecutor::BuildParameters(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("operation_id").Value(OpArg.getValue());

    TRequestExecutor::BuildParameters(consumer);
}

Stroka TAbortOperationExecutor::GetCommandName() const
{
    return "abort_op";
}

////////////////////////////////////////////////////////////////////////////////

TSuspendOperationExecutor::TSuspendOperationExecutor()
    : OpArg("", "id of an operation to suspend", true, "", "OP_ID")
{
    CmdLine.add(OpArg);
}

void TSuspendOperationExecutor::BuildParameters(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("operation_id").Value(OpArg.getValue());

    TRequestExecutor::BuildParameters(consumer);
}

Stroka TSuspendOperationExecutor::GetCommandName() const
{
    return "suspend_op";
}

////////////////////////////////////////////////////////////////////////////////

TResumeOperationExecutor::TResumeOperationExecutor()
    : OpArg("", "id of an operation to resume", true, "", "OP_ID")
{
    CmdLine.add(OpArg);
}

void TResumeOperationExecutor::BuildParameters(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("operation_id").Value(OpArg.getValue());

    TRequestExecutor::BuildParameters(consumer);
}

Stroka TResumeOperationExecutor::GetCommandName() const
{
    return "resume_op";
}

////////////////////////////////////////////////////////////////////////////////

TTrackOperationExecutor::TTrackOperationExecutor()
    : OpArg("", "id of an operation to track", true, "", "OP_ID")
{
    CmdLine.add(OpArg);
}

void TTrackOperationExecutor::DoExecute()
{
    auto operationId = TOperationId::FromString(OpArg.getValue());
    printf("Started tracking operation %s\n", ~ToString(operationId));

    TOperationTracker tracker(Config, Driver, operationId);
    tracker.Run();
}

Stroka TTrackOperationExecutor::GetCommandName() const
{
    return "track_op";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
