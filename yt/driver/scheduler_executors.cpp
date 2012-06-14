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

using namespace NYTree;
using namespace NScheduler;
using namespace NDriver;
using namespace NObjectServer;

//////////////////////////////////////////////////////////////////////////////////

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

    TOperationTracker tracker(Config, Driver, operationId);
    tracker.Run();
}

//////////////////////////////////////////////////////////////////////////////////

TMapExecutor::TMapExecutor()
    : InArg("", "in", "input table path", false, "ypath")
    , OutArg("", "out", "output table path", false, "ypath")
    , FilesArg("", "file", "additional file path", false, "ypath")
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
    : InArg("", "in", "input table path", false, "ypath")
    , OutArg("", "out", "output table path", false, "", "ypath")
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
    : InArg("", "in", "input table path", false, "ypath")
    , OutArg("", "out", "output table path", false, "", "ypath")
    , KeyColumnsArg("", "key_columns", "key columns names", true, "", "yson_list_fragment")
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
    : PathArg("path", "path to a table where rows must be removed", true, "", "ypath")
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

TTrackOpExecutor::TTrackOpExecutor()
    : OpArg("", "op", "id of an operation that must be tracked", true, "", "operation_id")
{
    CmdLine.add(OpArg);
}

void TTrackOpExecutor::Execute(const std::vector<std::string>& args)
{
    auto argsCopy = args;
    CmdLine.parse(argsCopy);

    InitConfig();

    NLog::TLogManager::Get()->Configure(~Config->Logging);

    Driver = CreateDriver(Config);

    auto operationId = DeserializeFromYson<TOperationId>(OpArg.getValue());
    printf("Started tracking operation %s\n", ~operationId.ToString());

    TOperationTracker tracker(Config, Driver, operationId);
    tracker.Run();
}

void TTrackOpExecutor::BuildArgs(IYsonConsumer* consumer)
{ }

Stroka TTrackOpExecutor::GetDriverCommandName() const
{
    return "track_op";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
