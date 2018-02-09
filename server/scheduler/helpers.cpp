#include "helpers.h"
#include "public.h"
#include "exec_node.h"
#include "config.h"
#include "job.h"
#include "operation.h"

#include <yt/server/controller_agent/operation_controller.h>

#include <yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/ytlib/core_dump/core_info.pb.h>
#include <yt/ytlib/core_dump/helpers.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/ytlib/api/transaction.h>

namespace NYT {
namespace NScheduler {

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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

void BuildFullOperationAttributes(TOperationPtr operation, TFluentMap fluent)
{
    auto initializationAttributes = operation->ControllerAttributes().InitializationAttributes;
    auto attributes = operation->ControllerAttributes().Attributes;
    fluent
        .Item("operation_type").Value(operation->GetType())
        .Item("start_time").Value(operation->GetStartTime())
        .Item("spec").Value(operation->GetSpec())
        .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
        .Item("mutation_id").Value(operation->GetMutationId())
        .DoIf(static_cast<bool>(initializationAttributes), [&] (TFluentMap fluent) {
            fluent
                .Items(initializationAttributes->Immutable);
        })
        .DoIf(static_cast<bool>(attributes), [&] (TFluentMap fluent) {
            fluent
                .Items(*attributes);
        })
        .Do(BIND(&BuildMutableOperationAttributes, operation));
}

void BuildMutableOperationAttributes(TOperationPtr operation, TFluentMap fluent)
{
    auto initializationAttributes = operation->ControllerAttributes().InitializationAttributes;
    fluent
        .Item("state").Value(operation->GetState())
        .Item("suspended").Value(operation->GetSuspended())
        .Item("events").Value(operation->GetEvents())
        .Item("slot_index_per_pool_tree").Value(operation->GetSlotIndices())
        .DoIf(static_cast<bool>(initializationAttributes), [&] (TFluentMap fluent) {
            fluent
                .Items(initializationAttributes->Mutable);
        });
}

void BuildExecNodeAttributes(TExecNodePtr node, TFluentMap fluent)
{
    fluent
        .Item("state").Value(node->GetMasterState())
        .Item("resource_usage").Value(node->GetResourceUsage())
        .Item("resource_limits").Value(node->GetResourceLimits());
}

////////////////////////////////////////////////////////////////////////////////

EAbortReason GetAbortReason(const NJobTrackerClient::NProto::TJobResult& result)
{
    auto error = FromProto<TError>(result.error());
    try {
        return error.Attributes().Get<EAbortReason>("abort_reason", EAbortReason::Scheduler);
    } catch (const std::exception& ex) {
        // Process unknown abort reason from node.
        LOG_WARNING(ex, "Found unknown abort_reason in job result");
        return EAbortReason::Unknown;
    }
}

////////////////////////////////////////////////////////////////////////////////

TString MakeOperationCodicilString(const TOperationId& operationId)
{
    return Format("OperationId: %v", operationId);
}

TCodicilGuard MakeOperationCodicilGuard(const TOperationId& operationId)
{
    return TCodicilGuard(MakeOperationCodicilString(operationId));
}

////////////////////////////////////////////////////////////////////////////////

TOperationRuntimeParamsPtr BuildOperationRuntimeParams(const TOperationSpecBasePtr& spec)
{
    auto result = New<TOperationRuntimeParams>();
    result->Weight = spec->Weight.Get(1.0);
    result->ResourceLimits = spec->ResourceLimits;
    result->Owners = spec->Owners;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

