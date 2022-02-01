#include "operation_controller.h"
#include "bootstrap.h"
#include "private.h"
#include "fair_share_tree_element.h"

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NYson;

using std::placeholders::_1;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

TError CheckPendingJobCountAndNeededResources(int pendingJobCount, const NVectorHdrf::TJobResources& neededResources)
{
    bool hasPendingJobs = pendingJobCount > 0;
    if (!Dominates(neededResources, TJobResources())) {
        return TError("Controller has reported negative needed resources")
            << TErrorAttribute("needed_resources", neededResources);
    }
    if (hasPendingJobs != (neededResources != TJobResources())) {
        return TError("Controller has reported inconsistent values for pending job count and needed resources")
            << TErrorAttribute("pending_job_count", pendingJobCount)
            << TErrorAttribute("needed_resources", neededResources);
    }
    return TError();
}

TError CheckControllerRuntimeData(const TControllerRuntimeDataPtr& runtimeData)
{
    auto compositePendingJobCount = runtimeData->GetPendingJobCount();
    auto compositeNeededResources = runtimeData->GetNeededResources();
    auto error = CheckPendingJobCountAndNeededResources(compositePendingJobCount.DefaultCount, compositeNeededResources.DefaultResources);
    if (!error.IsOK()) {
        return error;
    }

    if (compositePendingJobCount.CountByPoolTree.size() != compositeNeededResources.ResourcesByPoolTree.size()) {
            return TError("Controller has reported inconsistent pending job count and needed resources")
                << TErrorAttribute("pending_job_count_by_tree", compositePendingJobCount.CountByPoolTree)
                << TErrorAttribute("needed_resources_by_tree", compositeNeededResources.ResourcesByPoolTree);
    }

    for (const auto& [tree, jobCount] : compositePendingJobCount.CountByPoolTree) {
        auto resourcesIt = compositeNeededResources.ResourcesByPoolTree.find(tree);
        if (resourcesIt == compositeNeededResources.ResourcesByPoolTree.end()) {
            return TError("Controller has reported pending job count without needed resources")
                << TErrorAttribute("pending_job_count", jobCount)
                << TErrorAttribute("pool_tree", tree);
        }

        error = CheckPendingJobCountAndNeededResources(jobCount, resourcesIt->second);
        if (!error.IsOK()) {
            return error;
        }
    }

    for (const auto& jobResources : runtimeData->MinNeededJobResources()) {
        if (!Dominates(jobResources.ToJobResources(), TJobResources())) {
            return TError("Controller has reported negative min needed job resources element")
                << TErrorAttribute("min_needed_job_resources", runtimeData->MinNeededJobResources());
        }
    }
    return TError();
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TOperationControllerInitializeResult* result,
    const NControllerAgent::NProto::TInitializeOperationResult& resultProto,
    TOperationId operationId,
    TBootstrap* bootstrap,
    TDuration operationTransactionPingPeriod)
{
    try {
        TForbidContextSwitchGuard contextSwitchGuard;

        FromProto(
            &result->Transactions,
            resultProto.transaction_ids(),
            std::bind(&TBootstrap::GetRemoteMasterClient, bootstrap, _1),
            operationTransactionPingPeriod);
    } catch (const std::exception& ex) {
        YT_LOG_INFO(ex, "Failed to attach operation transactions", operationId);
    }

    result->Attributes = TOperationControllerInitializeAttributes{
        TYsonString(resultProto.mutable_attributes(), EYsonType::MapFragment),
        TYsonString(resultProto.brief_spec(), EYsonType::MapFragment),
        TYsonString(resultProto.full_spec(), EYsonType::Node),
        TYsonString(resultProto.unrecognized_spec(), EYsonType::Node)
    };
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TOperationControllerPrepareResult* result, const NControllerAgent::NProto::TPrepareOperationResult& resultProto)
{
    result->Attributes = resultProto.has_attributes()
        ? TYsonString(resultProto.attributes(), EYsonType::MapFragment)
        : TYsonString();
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TOperationControllerMaterializeResult* result, const NControllerAgent::NProto::TMaterializeOperationResult& resultProto)
{
    result->Suspend = resultProto.suspend();
    result->InitialNeededResources = FromProto<TCompositeNeededResources>(resultProto.initial_composite_needed_resources());
    result->InitialAggregatedMinNeededResources = FromProto<TJobResources>(resultProto.initial_aggregated_min_needed_resources());
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TOperationControllerReviveResult* result,
    const NControllerAgent::NProto::TReviveOperationResult& resultProto,
    TOperationId operationId,
    TIncarnationId incarnationId,
    EPreemptionMode preemptionMode)
{
    result->Attributes = TYsonString(resultProto.attributes(), EYsonType::MapFragment);
    result->RevivedFromSnapshot = resultProto.revived_from_snapshot();
    for (const auto& jobProto : resultProto.revived_jobs()) {
        auto job = New<TJob>(
            FromProto<TJobId>(jobProto.job_id()),
            static_cast<EJobType>(jobProto.job_type()),
            operationId,
            incarnationId,
            resultProto.controller_epoch(),
            nullptr /* execNode */,
            FromProto<TInstant>(jobProto.start_time()),
            FromProto<TJobResources>(jobProto.resource_limits()),
            FromProto<TDiskQuota>(jobProto.disk_quota()),
            jobProto.interruptible(),
            preemptionMode,
            jobProto.tree_id(),
            UndefinedSchedulingIndex,
            /*schedulingStage*/ std::nullopt,
            jobProto.node_id(),
            jobProto.node_address());
        job->SetState(EJobState::Running);
        result->RevivedJobs.push_back(job);
    }
    result->RevivedBannedTreeIds = FromProto<THashSet<TString>>(resultProto.revived_banned_tree_ids());
    result->NeededResources = FromProto<TCompositeNeededResources>(resultProto.composite_needed_resources());
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TOperationControllerCommitResult* /* result */, const NControllerAgent::NProto::TCommitOperationResult& /* resultProto */)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
