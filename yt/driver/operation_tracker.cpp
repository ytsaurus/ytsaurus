#include "operation_tracker.h"

#include <core/ytree/ypath_proxy.h>

#include <ytlib/scheduler/helpers.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/api/connection.h>

#include <util/stream/format.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NScheduler;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TOperationTracker::TOperationTracker(
    TExecutorConfigPtr config,
    IDriverPtr driver,
    const TOperationId& operationId)
    : Config(config)
    , Driver(driver)
    , OperationId(operationId)
{ }

void TOperationTracker::Run()
{
    OperationType = GetOperationType(OperationId);
    while (!CheckFinished())  {
        DumpProgress();
        Sleep(Config->OperationPollPeriod);
    }
    DumpResult();
}

void TOperationTracker::AppendPhaseProgress(
    Stroka* out,
    const Stroka& phase,
    const TYsonString& progress)
{
    auto progressNode = ConvertToNode(progress);
    i64 total = ConvertTo<i64>(GetNodeByYPath(progressNode, "/total"));
    if (total == 0) {
        return;
    }

    i64 completed = ConvertTo<i64>(GetNodeByYPath(progressNode, "/completed"));
    int percentComplete  = (completed * 100) / total;

    if (!out->empty()) {
        out->append(", ");
    }

    out->append(Format("%3d%% ", percentComplete));
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

Stroka TOperationTracker::FormatProgress(const TYsonString& progress)
{
    auto progressAttributes = ConvertToAttributes(progress);

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

        case EOperationType::MapReduce:
            AppendPhaseProgress(&result, "map", progressAttributes->GetYson("map_jobs"));
            AppendPhaseProgress(&result, "reduce", progressAttributes->GetYson("partitions"));
            break;

        default:
            YUNREACHABLE();
    }
    return result;
}

void TOperationTracker::DumpProgress()
{
    auto operationPath = GetOperationPath(OperationId);

    TObjectServiceProxy proxy(Driver->GetConnection()->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TYPathProxy::Get(operationPath + "/@state");
        batchReq->AddRequest(req, "get_state");
    }

    {
        auto req = TYPathProxy::Get(operationPath + "/@progress");
        batchReq->AddRequest(req, "get_progress");
    }

    auto batchRspOrError = batchReq->Invoke().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error getting operation progress");
    const auto& batchRsp = batchRspOrError.Value();

    EOperationState state;
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_state");
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting operation state");
        const auto& rsp = rspOrError.Value();
        state = ConvertTo<EOperationState>(TYsonString(rsp->value()));
    }

    TYsonString progress;
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_progress");
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting operation progress");
        const auto& rsp = rspOrError.Value();
        progress = TYsonString(rsp->value());
    }

    if (!PrevProgress || *PrevProgress != progress) {
        if (state == EOperationState::Running) {
            printf("%s: %s\n",
                ~ToString(state),
                ~FormatProgress(progress));
        } else {
            printf("%s\n", ~ToString(state));
        }
        PrevProgress = progress;
    }
}

void TOperationTracker::DumpResult()
{
    auto operationPath = GetOperationPath(OperationId);
    auto jobsPath = GetJobsPath(OperationId);

    TObjectServiceProxy proxy(Driver->GetConnection()->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TYPathProxy::Get(operationPath + "/@result");
        batchReq->AddRequest(req, "get_op_result");
    }

    {
        auto req = TYPathProxy::Get(operationPath + "/@start_time");
        batchReq->AddRequest(req, "get_op_start_time");
    }
    {
        auto req = TYPathProxy::Get(operationPath + "/@finish_time");
        batchReq->AddRequest(req, "get_op_finish_time");
    }

    {
        auto req = TYPathProxy::Get(jobsPath);
        auto* attributeFilter = req->mutable_attribute_filter();
        attributeFilter->set_mode(static_cast<int>(EAttributeFilterMode::MatchingOnly));
        attributeFilter->add_keys("job_type");
        attributeFilter->add_keys("state");
        attributeFilter->add_keys("address");
        attributeFilter->add_keys("error");
        batchReq->AddRequest(req, "get_jobs");
    }

    auto batchRspOrError = batchReq->Invoke().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error getting operation result");
    const auto& batchRsp = batchRspOrError.Value();
    {
        {
            auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_op_result");
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting operation result");
            const auto& rsp = rspOrError.Value();
            auto resultNode = ConvertToNode(TYsonString(rsp->value()));
            auto error = ConvertTo<TError>(GetNodeByYPath(resultNode, "/error"));
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }

        TInstant startTime;
        {
            auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_op_start_time");
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting operation start time");
            const auto& rsp = rspOrError.Value();
            startTime = ConvertTo<TInstant>(TYsonString(rsp->value()));
        }

        TInstant endTime;
        {
            auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_op_finish_time");
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting operation finish time");
            const auto& rsp = rspOrError.Value();
            endTime = ConvertTo<TInstant>(TYsonString(rsp->value()));
        }
        TDuration duration = endTime - startTime;

        printf("Operation completed successfully in %s\n", ~ToString(duration));
    }

    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_jobs");
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting operation jobs info");
        const auto& rsp = rspOrError.Value();

        TEnumIndexedVector<int, EJobType> totalJobCount;
        TEnumIndexedVector<int, EJobType> completedJobCount;
        TEnumIndexedVector<int, EJobType> failedJobCount;
        TEnumIndexedVector<int, EJobType> abortedJobCount;

        auto jobs = ConvertToNode(TYsonString(rsp->value()))->AsMap();
        if (jobs->GetChildCount() == 0) {
            return;
        }

        std::list<TJobId> failedJobIds;
        std::list<TJobId> stdErrJobIds;
        for (const auto& pair : jobs->GetChildren()) {
            auto jobId = TJobId::FromString(pair.first);
            auto job = pair.second->AsMap();

            auto jobType = job->Attributes().Get<EJobType>("job_type");
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
        printf("%-16s %10s %10s %10s %10s\n", "Job type", "Total", "Completed", "Failed", "Aborted");
        for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
            if (totalJobCount[jobType] > 0) {
                printf("%-16s %10d %10d %10d %10d\n",
                    ~ToString(EJobType(jobType)),
                    totalJobCount[jobType],
                    completedJobCount[jobType],
                    failedJobCount[jobType],
                    abortedJobCount[jobType]);
            }
        }

        if (!failedJobIds.empty()) {
            printf("\n");
            printf("%" PRISZT " job(s) have failed:\n", failedJobIds.size());
            for (const auto& jobId : failedJobIds) {
                auto job = jobs->GetChild(ToString(jobId));
                auto error = ConvertTo<TError>(job->Attributes().Get<INodePtr>("error"));
                printf("\n");
                printf("Job %s on %s\n%s\n",
                    ~ToString(jobId),
                    ~job->Attributes().Get<Stroka>("address"),
                    ~ToString(error));
            }
        }

        if (!stdErrJobIds.empty()) {
            printf("\n");
            printf("%" PRISZT " stderr(s) have been captured, use the following commands to view:\n",
                stdErrJobIds.size());
            for (const auto& jobId : stdErrJobIds) {
                printf("yt download '%s'\n",
                    ~GetStderrPath(OperationId, jobId));
            }
        }
    }
}

EOperationType TOperationTracker::GetOperationType(const TOperationId& operationId)
{
    auto operationPath = GetOperationPath(OperationId);
    TObjectServiceProxy proxy(Driver->GetConnection()->GetMasterChannel());
    auto req = TYPathProxy::Get(operationPath + "/@operation_type");
    auto rspOrError = proxy.Execute(req).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting operation type");
    const auto& rsp = rspOrError.Value();
    return ConvertTo<EOperationType>(TYsonString(rsp->value()));
}

bool TOperationTracker::CheckFinished()
{
    TObjectServiceProxy proxy(Driver->GetConnection()->GetMasterChannel());
    auto operationPath = GetOperationPath(OperationId);
    auto req = TYPathProxy::Get(operationPath + "/@state");
    auto rspOrError = proxy.Execute(req).Get();
    if (!rspOrError.IsOK()) {
        return false;
    }
    const auto& rsp = rspOrError.Value();
    auto state = ConvertTo<EOperationState>(TYsonString(rsp->value()));
    if (!IsOperationFinished(state)) {
        return false;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
