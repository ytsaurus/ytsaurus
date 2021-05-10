#include "operation_controller.h"
#include "bootstrap.h"
#include "private.h"

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NYson;

using std::placeholders::_1;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

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
    result->InitialNeededResources = FromProto<TJobResources>(resultProto.initial_needed_resources());
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
            jobProto.interruptible(),
            preemptionMode,
            jobProto.tree_id(),
            jobProto.node_id(),
            jobProto.node_address());
        job->SetState(EJobState::Running);
        result->RevivedJobs.push_back(job);
    }
    result->RevivedBannedTreeIds = FromProto<THashSet<TString>>(resultProto.revived_banned_tree_ids());
    result->NeededResources = FromProto<TJobResources>(resultProto.needed_resources());
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TOperationControllerCommitResult* /* result */, const NControllerAgent::NProto::TCommitOperationResult& /* resultProto */)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
