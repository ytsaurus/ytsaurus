#include "scheduler_executors.h"
#include "preprocess.h"

#include <ytlib/job_proxy/config.h>
#include <ytlib/driver/driver.h>

#include <ytlib/ytree/ypath_proxy.h>

#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/scheduler/helpers.h>

#include <ytlib/object_server/object_service_proxy.h>

#include <util/stream/format.h>

namespace NYT {

using namespace NYTree;
using namespace NScheduler;
using namespace NDriver;
using namespace NObjectServer;

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
        i64 total = DeserializeFromYson<i64>(progress, "/total");
        if (total == 0) {
            return;
        }

        i64 completed = DeserializeFromYson<i64>(progress, "/completed");
        int percentComplete  = (completed * 100) / total;

        if (!out->empty()) {
            out->append(", ");
        }

        out->append(Sprintf("%3d%% ", percentComplete));
        if (!phase.empty()) {
            out->append(phase);
            out->append(' ');
        }

        // Some simple pretty-printing.
        int totalWidth = ToString(total).length();
        out->append("(");
        out->append(ToString(LeftPad(ToString(completed), totalWidth)));
        out->append("/");
        out->append(ToString(total));
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
                AppendPhaseProgress(&result, "sort", progressAttributes->GetYson("partitions"));
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
        auto jobsPath = GetJobsPath(OperationId);

        TObjectServiceProxy proxy(Driver->GetMasterChannel());
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TYPathProxy::Get(operationPath + "/@result");
            batchReq->AddRequest(req, "get_op_result");
        }

        {
            auto req = TYPathProxy::Get(jobsPath);
            req->Attributes().Set("with_attributes", "true");
            batchReq->AddRequest(req, "get_jobs");
        }

        auto batchRsp = batchReq->Invoke().Get();
        CheckResponse(batchRsp, "Error getting operation result");

        {
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_op_result");
            CheckResponse(rsp, "Error getting operation result");
            // TODO(babenko): refactor!
            auto errorNode = DeserializeFromYson<INodePtr>(rsp->value(), "/error");
            auto error = TError::FromYson(errorNode);
            if (error.IsOK()) {
                printf("Operation completed successfully\n");
            } else {
                printf("Operation failed\n%s\n", ~error.ToString());
            }
        }

        {
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_jobs");
            CheckResponse(rsp, "Error getting operation jobs info");

            size_t jobTypeCount = EJobType::GetDomainSize();
            std::vector<int> totalJobCount(jobTypeCount);
            std::vector<int> completedJobCount(jobTypeCount);
            std::vector<int> failedJobCount(jobTypeCount);

            auto jobs = DeserializeFromYson(rsp->value())->AsMap();
            std::list<TJobId> failedJobIds;
            std::list<TJobId> stdErrJobIds;
            FOREACH (const auto& pair, jobs->GetChildren()) {
                auto jobId = TJobId::FromString(pair.first);
                auto job = pair.second->AsMap();
                
                auto jobType = job->Attributes().Get<EJobType>("job_type").ToValue();
                YCHECK(jobType >= 0 && jobType < jobTypeCount);
                
                auto jobState = job->Attributes().Get<EJobState>("job_state");
                ++totalJobCount[jobType];
                switch (jobState) {
                    case EJobState::Completed:
                        ++completedJobCount[jobType];
                        break;
                    case EJobState::Failed:
                        ++failedJobCount[jobType];
                        failedJobIds.push_back(jobId);
                        break;
                    default:
                        YUNREACHABLE();
                }

                if (job->FindChild("stderr")) {
                    stdErrJobIds.push_back(jobId);
                }
            }

            printf("\n");
            printf("Job statistics:\n");
            printf("%10s %10s %10s\n", "", "Total", "Completed", "Failed");
            for (int jobType = 0; jobType < jobTypeCount; ++jobType) {
                if (totalJobCount[jobType] > 0) {   
                    printf ("%10s %10d %10d %10d\n",
                        ~EJobType(jobType).ToString(),
                        totalJobCount[jobType],
                        completedJobCount[jobType],
                        failedJobCount[jobType]);
                }
            }

            if (!failedJobIds.empty()) {
                printf("\n");
                printf("%s job(s) have failed:");
                printf("%35s %15s\n", "Id", "Address");
                FOREACH (const auto& jobId, failedJobIds) {
                    auto job = jobs->GetChild(jobId.ToString());
                    printf("%35s %15s\n",
                        ~jobId.ToString(),
                        ~job->Attributes().Get<Stroka>("address"));
                }
            }

            if (!stdErrJobIds.empty()) {
                printf("\n");
                printf("%s stderr(s) have been captured, use the following commands to view:");
                FOREACH (const auto& jobId, stdErrJobIds) {
                    printf("yt download %s\n",
                        GetStdErrPath(OperationId, jobId));
                }
            }
        }
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

} // namespace NYT
