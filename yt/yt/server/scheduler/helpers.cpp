#include "helpers.h"
#include "public.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/experiments.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/server/lib/core_dump/helpers.h>

#include <yt/yt/ytlib/core_dump/proto/core_info.pb.h>

#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/key_set.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/security_client/helpers.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/serialize.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYPath;
using namespace NCoreDump::NProto;
using namespace NYson;
using namespace NObjectClient;
using namespace NTableClient;
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
        .Item("experiment_assignments").Value(operation->ExperimentAssignments())
        .Item("experiment_assignment_names").Value(operation->GetExperimentAssignmentNames())
        .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
        .Item("mutation_id").Value(operation->GetMutationId())
        .Item("user_transaction_id").Value(operation->GetUserTransactionId())
        .Item("state").Value(operation->GetState())
        .Item("suspended").Value(operation->GetSuspended());
}

void BuildFullOperationAttributes(TOperationPtr operation, bool includeOperationId, TFluentMap fluent)
{
    const auto& initializationAttributes = operation->ControllerAttributes().InitializeAttributes;
    const auto& prepareAttributes = operation->ControllerAttributes().PrepareAttributes;
    fluent
        .DoIf(includeOperationId, [&] (TFluentMap fluent) {
            fluent.Item("operation_id").Value(operation->GetId());
        })
        .Item("operation_type").Value(operation->GetType())
        .Item("start_time").Value(operation->GetStartTime())
        .Item("spec").Value(operation->GetSpecString())
        .Item("experiment_assignments").Value(operation->ExperimentAssignments())
        .Item("experiment_assignment_names").Value(operation->GetExperimentAssignmentNames())
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
        .Item("task_names").Value(operation->GetTaskNames())
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

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    TListOperationsResult result;

    THashSet<TOperationId> operationSet;
    for (int hash = 0x0; hash <= 0xFF; ++hash) {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspList>(
            "list_operations_" + Format("%02x", hash));

        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            continue;
        }

        auto hashBucketRsp = rspOrError.ValueOrThrow();

        try {
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
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing operations from //sys/operations/%02x", hash)
                << ex;
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

////////////////////////////////////////////////////////////////////////////////

namespace {

void FromBytes(std::vector<TLegacyOwningKey>* keys, TStringBuf bytes)
{
    TKeySetReader reader(TSharedRef::FromString(TString(bytes)));
    for (auto key : reader.GetKeys()) {
        keys->push_back(TLegacyOwningKey(key));
    }
}

void ToBytes(TString* bytes, const std::vector<TLegacyOwningKey>& keys)
{
    auto keySetWriter = New<TKeySetWriter>();
    for (const auto& key : keys) {
        keySetWriter->WriteKey(key);
    }
    auto serializedKeys = keySetWriter->Finish();
    *bytes = TString(serializedKeys.begin(), serializedKeys.end());
}

// TODO(gritukan): Why does not it compile without these helpers?
void Serialize(const std::vector<TLegacyOwningKey>& keys, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .DoListFor(keys, [] (TFluentList fluent, const TLegacyOwningKey& key) {
            Serialize(key, fluent.GetConsumer());
        });
}

void Deserialize(std::vector<TLegacyOwningKey>& keys, INodePtr node)
{
    for (const auto& child : node->AsList()->GetChildren()) {
        TLegacyOwningKey key;
        Deserialize(key, child);
        keys.push_back(key);
    }
}

REGISTER_INTERMEDIATE_PROTO_INTEROP_BYTES_FIELD_REPRESENTATION(NProto::TPartitionJobSpecExt, /*wire_partition_keys*/8, std::vector<TLegacyOwningKey>)

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

TString GuessGpuType(const TString& treeId)
{
    if (treeId.StartsWith("gpu_")) {
        return TString(TStringBuf(treeId).SubStr(4));
    }
    return "unknown";
}

std::vector<std::pair<TInstant, TInstant>> SplitTimeIntervalByHours(TInstant startTime, TInstant finishTime)
{
    YT_VERIFY(startTime <= finishTime);

    std::vector<std::pair<TInstant, TInstant>> timeIntervals;
    {
        i64 startTimeHours = startTime.Seconds() / 3600;
        i64 finishTimeHours = finishTime.Seconds() / 3600;
        TInstant currentStartTime = startTime;
        while (startTimeHours < finishTimeHours) {
            ++startTimeHours;
            auto hourBound = TInstant::Hours(startTimeHours);
            YT_VERIFY(currentStartTime <= hourBound);
            timeIntervals.push_back(std::make_pair(currentStartTime, hourBound));
            currentStartTime = hourBound;
        }
        YT_VERIFY(currentStartTime <= finishTime);
        if (currentStartTime < finishTime) {
            timeIntervals.push_back(std::make_pair(currentStartTime, finishTime));
        }
    }
    return timeIntervals;
}

////////////////////////////////////////////////////////////////////////////////

THashSet<int> GetDiskQuotaMedia(const TDiskQuota& diskQuota)
{
    THashSet<int> media;
    for (const auto& [index, _] : diskQuota.DiskSpacePerMedium) {
        media.insert(index);
    }
    return media;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

