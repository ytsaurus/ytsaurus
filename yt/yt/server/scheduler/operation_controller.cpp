#include "operation_controller.h"

#include "bootstrap.h"
#include "private.h"
#include "fair_share_tree_element.h"

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

namespace NYT::NScheduler {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYson;

using std::placeholders::_1;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = SchedulerLogger;

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

    for (const auto& [_, allocationGroupResources] : runtimeData->GroupedNeededResources()) {
        if (!Dominates(allocationGroupResources.MinNeededResources.ToJobResources(), TJobResources())) {
            return TError("Controller has reported negative min needed allocation resources")
                << TErrorAttribute("grouped_needed_resources", runtimeData->GroupedNeededResources());
        }

        if (allocationGroupResources.AllocationCount < 0) {
            return TError("Controller has reported negative needed allocation count")
                << TErrorAttribute("grouped_needed_resources", runtimeData->GroupedNeededResources());
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
    TForbidContextSwitchGuard contextSwitchGuard;

    auto attachTransaction = [&] (TTransactionId transactionId, const TString& /*name*/) -> ITransactionPtr {
        if (!transactionId) {
            return nullptr;
        }

        try {
            auto client = bootstrap->GetRemoteClient(CellTagFromId(transactionId));

            TTransactionAttachOptions options;
            options.Ping = true;
            options.PingAncestors = false;
            options.PingPeriod = operationTransactionPingPeriod;

            auto transaction = client->AttachTransaction(transactionId, options);
            return transaction;
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error attaching operation transaction (OperationId: %v, TransactionId: %v)", operationId, transactionId);
        }

        return nullptr;
    };

    auto transactionIds = FromProto<TControllerTransactionIds>(resultProto.transaction_ids());
    result->Transactions = AttachControllerTransactions(attachTransaction, std::move(transactionIds));

    result->Attributes = TOperationControllerInitializeAttributes{
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
    result->InitialGroupedNeededResources = FromProto<TAllocationGroupResourcesMap>(resultProto.initial_grouped_needed_resources());
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
    for (const auto& allocationProto : resultProto.revived_allocations()) {
        auto allocationId = FromProto<TAllocationId>(allocationProto.allocation_id());
        auto allocation = New<TAllocation>(
            allocationId,
            operationId,
            incarnationId,
            TControllerEpoch(resultProto.controller_epoch()),
            /*execNode*/ nullptr,
            FromProto<TInstant>(allocationProto.start_time()),
            TAllocationStartDescriptor{
                .Id = allocationId,
                .ResourceLimits = FromProto<TJobResourcesWithQuota>(allocationProto.resource_limits()),
            },
            preemptionMode,
            allocationProto.tree_id(),
            UndefinedSchedulingIndex,
            /*schedulingStage*/ std::nullopt,
            FromProto<NNodeTrackerClient::TNodeId>(allocationProto.node_id()),
            allocationProto.node_address());
        allocation->SetState(EAllocationState::Running);
        result->RevivedAllocations.push_back(allocation);

        if (allocationProto.has_preemptible_progress_start_time()) {
            allocation->SetPreemptibleProgressStartTime(FromProto<TInstant>(allocationProto.preemptible_progress_start_time()));
        }
    }
    result->RevivedBannedTreeIds = FromProto<THashSet<TString>>(resultProto.revived_banned_tree_ids());
    result->NeededResources = FromProto<TCompositeNeededResources>(resultProto.composite_needed_resources());
    result->GroupedNeededResources = FromProto<TAllocationGroupResourcesMap>(resultProto.grouped_needed_resources());
    result->InitialGroupedNeededResources = FromProto<TAllocationGroupResourcesMap>(resultProto.initial_grouped_needed_resources());
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TOperationControllerCommitResult* /*result*/, const NControllerAgent::NProto::TCommitOperationResult& /*resultProto*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
