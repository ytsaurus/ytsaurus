#include "operation_tracker.h"

#include <ytlib/ytree/ypath_proxy.h>

#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/scheduler/helpers.h>

#include <ytlib/object_server/object_service_proxy.h>

#include <util/stream/format.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NScheduler;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TOperationTracker::TOperationTracker(
    TExecutorConfigPtr config,
    IDriverPtr driver,
    const TOperationId& operationId)
    : Config(config)
    , Driver(driver)
    , OperationId(operationId)
{ }

EExitCode TOperationTracker::Run()
{
    OperationType = GetOperationType(OperationId);
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

    return DumpResult();
}

void TOperationTracker::AppendPhaseProgress(
    Stroka* out,
    const Stroka& phase,
    const TYson& progress)
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

Stroka TOperationTracker::FormatProgress(const TYson& progress)
{
    // TODO(babenko): refactor
    auto progressAttributes = IAttributeDictionary::FromMap(DeserializeFromYson(progress)->AsMap());

    Stroka result;
    switch (OperationType) {
        case EOperationType::Map:
        case EOperationType::Merge:
        case EOperationType::Erase:
        case EOperationType::Reduce:
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

void TOperationTracker::DumpProgress()
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

EExitCode TOperationTracker::DumpResult()
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
        req->Attributes().Set<Stroka>("with_attributes", "true");
        batchReq->AddRequest(req, "get_jobs");
    }

    EExitCode exitCode;
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
            exitCode = EExitCode::OK;
        } else {
            printf("%s\n", ~error.ToString());
            exitCode = EExitCode::Error;
        }
    }

    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_jobs");
        CheckResponse(rsp, "Error getting operation jobs info");

        size_t jobTypeCount = EJobType::GetDomainSize();
        std::vector<int> totalJobCount(jobTypeCount);
        std::vector<int> completedJobCount(jobTypeCount);
        std::vector<int> failedJobCount(jobTypeCount);
        std::vector<int> abortedJobCount(jobTypeCount);

        auto jobs = DeserializeFromYson(rsp->value())->AsMap();
        if (jobs->GetChildCount() == 0) {
            return exitCode;
        }

        std::list<TJobId> failedJobIds;
        std::list<TJobId> stdErrJobIds;
        FOREACH (const auto& pair, jobs->GetChildren()) {
            auto jobId = TJobId::FromString(pair.first);
            auto job = pair.second->AsMap();

            auto jobType = job->Attributes().Get<EJobType>("job_type").ToValue();
            YCHECK(jobType >= 0 && jobType < jobTypeCount);

            auto jobState = job->Attributes().Get<EJobState>("state");
            ++totalJobCount[jobType];
            switch (jobState) {
                case EJobState::Completed:
                    ++completedJobCount[jobType];
                    break;
                case EJobState::Failed:
                    ++failedJobCount[jobType];
                    failedJobIds.push_back(jobId);
                    break;
                case EJobState::Aborted:
                    ++abortedJobCount[jobType];
                    break;
                default:
                    YUNREACHABLE();
            }

            if (job->FindChild("stderr")) {
                stdErrJobIds.push_back(jobId);
            }
        }

        printf("\n");
        printf("%-14s %10s %10s %10s %10s\n", "Job type", "Total", "Completed", "Failed", "Aborted");
        for (int jobType = 0; jobType < jobTypeCount; ++jobType) {
            if (totalJobCount[jobType] > 0) {
                printf("%-14s %10d %10d %10d %10d\n",
                    ~EJobType(jobType).ToString(),
                    totalJobCount[jobType],
                    completedJobCount[jobType],
                    failedJobCount[jobType],
                    abortedJobCount[jobType]);
            }
        }

        if (!failedJobIds.empty()) {
            printf("\n");
            printf("%" PRISZT " job(s) have failed:\n", failedJobIds.size());
            FOREACH (const auto& jobId, failedJobIds) {
                auto job = jobs->GetChild(jobId.ToString());
                // TODO(babenko): refactor
                auto error = TError::FromYson(job->Attributes().Get<INodePtr>("error"));
                printf("\n");
                printf("Job %s on %s\n%s\n",
                    ~jobId.ToString(),
                    ~job->Attributes().Get<Stroka>("address"),
                    ~error.ToString());
            }
        }

        if (!stdErrJobIds.empty()) {
            printf("\n");
            printf("%" PRISZT " stderr(s) have been captured, use the following commands to view:\n",
                stdErrJobIds.size());
            FOREACH (const auto& jobId, stdErrJobIds) {
                printf("yt download '%s'\n",
                    ~GetStdErrPath(OperationId, jobId));
            }
        }
    }
    return exitCode;
}

EOperationType TOperationTracker::GetOperationType(const TOperationId& operationId)
{
    auto operationPath = GetOperationPath(OperationId);
    TObjectServiceProxy proxy(Driver->GetMasterChannel());
    auto req = TYPathProxy::Get(operationPath + "/@operation_type");
    auto rsp = proxy.Execute(req).Get();
    CheckResponse(rsp, "Error getting operation type");
    return DeserializeFromYson<EOperationType>(rsp->value());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
