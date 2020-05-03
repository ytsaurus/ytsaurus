#include "helpers.h"
#include "public.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"

#include <yt/server/lib/scheduler/config.h>
#include <yt/server/lib/scheduler/helpers.h>

#include <yt/server/lib/core_dump/helpers.h>

#include <yt/ytlib/core_dump/proto/core_info.pb.h>

#include <yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/security_client/helpers.h>

#include <yt/client/api/transaction.h>

#include <yt/core/ytree/node.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYPath;
using namespace NCoreDump::NProto;
using namespace NYson;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NChunkClient;
using namespace NApi;
using namespace NLogging;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

void BuildMinimalOperationAttributes(TOperationPtr operation, TFluentMap fluent)
{
    fluent
        .Item("operation_type").Value(operation->GetType())
        .Item("start_time").Value(operation->GetStartTime())
        .Item("spec").Value(operation->GetSpecString())
        .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
        .Item("mutation_id").Value(operation->GetMutationId())
        .Item("user_transaction_id").Value(operation->GetUserTransactionId())
        .Item("state").Value(operation->GetState())
        .Item("suspended").Value(operation->GetSuspended());
}

void BuildFullOperationAttributes(TOperationPtr operation, TFluentMap fluent)
{
    const auto& initializationAttributes = operation->ControllerAttributes().InitializeAttributes;
    const auto& prepareAttributes = operation->ControllerAttributes().PrepareAttributes;
    fluent
        .Item("operation_id").Value(operation->GetId())
        .Item("operation_type").Value(operation->GetType())
        .Item("start_time").Value(operation->GetStartTime())
        .Item("spec").Value(operation->GetSpecString())
        .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
        .Item("mutation_id").Value(operation->GetMutationId())
        .Item("user_transaction_id").Value(operation->GetUserTransactionId())
        .DoIf(static_cast<bool>(initializationAttributes), [&] (TFluentMap fluent) {
            fluent
                .Item("unrecognized_spec").Value(initializationAttributes->UnrecognizedSpec)
                .Item("full_spec").Value(initializationAttributes->FullSpec);
        })
        .DoIf(static_cast<bool>(prepareAttributes), [&] (TFluentMap fluent) {
            fluent
                .Items(prepareAttributes);
        })
        .Do(BIND(&BuildMutableOperationAttributes, operation));
}

void BuildMutableOperationAttributes(TOperationPtr operation, TFluentMap fluent)
{
    auto initializationAttributes = operation->ControllerAttributes().InitializeAttributes;
    fluent
        .Item("state").Value(operation->GetState())
        .Item("suspended").Value(operation->GetSuspended())
        .Item("events").Value(operation->Events())
        .Item("slot_index_per_pool_tree").Value(operation->GetSlotIndices())
        .DoIf(static_cast<bool>(initializationAttributes), [&] (TFluentMap fluent) {
            fluent
                .Items(initializationAttributes->Mutable);
        });
}

////////////////////////////////////////////////////////////////////////////////

EAbortReason GetAbortReason(const TError& resultError)
{
    try {
        return resultError.Attributes().Get<EAbortReason>("abort_reason", EAbortReason::Scheduler);
    } catch (const std::exception& ex) {
        // Process unknown abort reason from node.
        YT_LOG_WARNING(ex, "Found unknown abort_reason in job result");
        return EAbortReason::Unknown;
    }
}

TJobStatus JobStatusFromError(const TError& error)
{
    auto status = TJobStatus();
    ToProto(status.mutable_result()->mutable_error(), error);
    return status;
}

////////////////////////////////////////////////////////////////////////////////

TString MakeOperationCodicilString(TOperationId operationId)
{
    return Format("OperationId: %v", operationId);
}

TCodicilGuard MakeOperationCodicilGuard(TOperationId operationId)
{
    return TCodicilGuard(MakeOperationCodicilString(operationId));
}

////////////////////////////////////////////////////////////////////////////////

TListOperationsResult ListOperations(
    TCallback<TObjectServiceProxy::TReqExecuteBatchPtr()> createBatchRequest)
{
    using NYT::ToProto;

    static const std::vector<TString> attributeKeys = {
        "state"
    };

    auto batchReq = createBatchRequest();

    for (int hash = 0x0; hash <= 0xFF; ++hash) {
        auto hashStr = Format("%02x", hash);
        auto req = TYPathProxy::List("//sys/operations/" + hashStr);
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
        batchReq->AddRequest(req, "list_operations_" + hashStr);
    }

    {
        auto req = TYPathProxy::List("//sys/operations");
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
        batchReq->AddRequest(req, "list_operations");
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    auto rootOperationsRspOrError = batchRsp->GetResponse<TYPathProxy::TRspList>("list_operations");
    auto rootOperationsRsp = rootOperationsRspOrError.ValueOrThrow();

    auto rootOperationsNode = ConvertToNode(TYsonString(rootOperationsRsp->value()));

    TListOperationsResult result;

    THashSet<TOperationId> operationSet;
    THashMap<TOperationId, EOperationState> rootOperationIdToState;

    for (const auto& operationNode : rootOperationsNode->AsList()->GetChildren()) {
        auto key = operationNode->GetValue<TString>();
        // Hash-bucket case.
        if (key.size() == 2) {
            continue;
        }

        auto id = TOperationId::FromString(key);
        auto state = operationNode->Attributes().Get<EOperationState>("state");
        YT_VERIFY(rootOperationIdToState.emplace(id, state).second);
    }

    for (int hash = 0x0; hash <= 0xFF; ++hash) {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspList>(
            "list_operations_" + Format("%02x", hash));

        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            continue;
        }

        auto hashBucketRsp = rspOrError.ValueOrThrow();
        auto hashBucketListNode = ConvertToNode(TYsonString(hashBucketRsp->value()));
        auto hashBucketList = hashBucketListNode->AsList();

        for (const auto& operationNode : hashBucketList->GetChildren()) {
            auto id = TOperationId::FromString(operationNode->GetValue<TString>());
            YT_VERIFY((id.Parts32[0] & 0xff) == hash);

            auto state = operationNode->Attributes().Get<EOperationState>("state");
            YT_VERIFY(operationSet.insert(id).second);

            if (IsOperationInProgress(state)) {
                result.OperationsToRevive.push_back({id, state});
            } else {
                result.OperationsToArchive.push_back(id);
            }
        }
    }

    for (const auto& [operationId, operationState] : rootOperationIdToState) {
        if (operationSet.find(operationId) == operationSet.end()) {
            result.OperationsToRemove.push_back(operationId);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TJobResources ComputeAvailableResources(
    const TJobResources& resourceLimits,
    const TJobResources& resourceUsage,
    const TJobResources& resourceDiscount)
{
    return resourceLimits - resourceUsage + resourceDiscount;
}

////////////////////////////////////////////////////////////////////////////////

TOperationFairShareTreeRuntimeParametersPtr GetSchedulingOptionsPerPoolTree(IOperationStrategyHost* operation, const TString& treeId)
{
    return GetOrCrash(operation->GetRuntimeParameters()->SchedulingOptionsPerPoolTree, treeId);
}

} // namespace NYT::NScheduler

