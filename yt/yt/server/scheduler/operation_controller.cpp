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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

TError CheckControllerRuntimeData(const TControllerRuntimeDataPtr& runtimeData)
{
    auto compositeNeededResources = runtimeData->GetNeededResources();
    if (!Dominates(compositeNeededResources.DefaultResources, TJobResources())) {
        return TError("Controller has reported negative needed resources")
            << TErrorAttribute("needed_resources", FormatResources(compositeNeededResources));
    }

    for (const auto& [tree, neededResources] : compositeNeededResources.ResourcesByPoolTree) {
        if (!Dominates(neededResources, TJobResources())) {
            return TError("Controller has reported negative needed resources")
                << TErrorAttribute("pool_tree", tree)
                << TErrorAttribute("needed_resources", FormatResources(compositeNeededResources));
        }
    }

    for (const auto& jobResources : runtimeData->MinNeededResources()) {
        if (!Dominates(jobResources.ToJobResources(), TJobResources())) {
            return TError("Controller has reported negative min needed job resources element")
                << TErrorAttribute("min_needed_job_resources", runtimeData->MinNeededResources());
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
            std::bind(&TBootstrap::GetRemoteClient, bootstrap, _1),
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

    result->EraseOffloadingTrees = resultProto.erase_offloading_trees();
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
    result->InitialMinNeededResources = FromProto<TJobResourcesWithQuotaList>(resultProto.initial_min_needed_resources());
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
            operationId,
            incarnationId,
            resultProto.controller_epoch(),
            /*execNode*/ nullptr,
            FromProto<TInstant>(jobProto.start_time()),
            FromProto<TJobResources>(jobProto.resource_limits()),
            FromProto<TDiskQuota>(jobProto.disk_quota()),
            jobProto.interruptible(),
            preemptionMode,
            jobProto.tree_id(),
            UndefinedSchedulingIndex,
            /*schedulingStage*/ std::nullopt,
            FromProto<NNodeTrackerClient::TNodeId>(jobProto.node_id()),
            jobProto.node_address());
        job->SetAllocationState(EAllocationState::Running);
        result->RevivedJobs.push_back(job);
    }
    result->RevivedBannedTreeIds = FromProto<THashSet<TString>>(resultProto.revived_banned_tree_ids());
    result->NeededResources = FromProto<TCompositeNeededResources>(resultProto.composite_needed_resources());
    result->MinNeededResources = FromProto<TJobResourcesWithQuotaList>(resultProto.min_needed_resources());
    result->InitialMinNeededResources = FromProto<TJobResourcesWithQuotaList>(resultProto.initial_min_needed_resources());
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TOperationControllerCommitResult* /* result */, const NControllerAgent::NProto::TCommitOperationResult& /* resultProto */)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
