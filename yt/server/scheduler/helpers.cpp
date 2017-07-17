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

using namespace NProto;
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

void BuildInitializingOperationAttributes(TOperationPtr operation, bool buildSpec, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("operation_type").Value(operation->GetType())
        .Item("start_time").Value(operation->GetStartTime())
        .DoIf(buildSpec, [&] (TFluentMap fluent) {
            fluent
                .Item("spec").Value(operation->GetSpec());
        })
        .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
        .Item("mutation_id").Value(operation->GetMutationId())
        .Do(BIND(&BuildRunningOperationAttributes, operation));
}

void BuildRunningOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer)
{
    auto controller = operation->GetController();
    BuildYsonMapFluently(consumer)
        .Item("state").Value(operation->GetState())
        .Item("suspended").Value(operation->GetSuspended())
        .Item("events").Value(operation->GetEvents())
        .Item("slot_index").Value(operation->GetSlotIndex())
        .DoIf(static_cast<bool>(controller), BIND(&NControllerAgent::IOperationController::BuildOperationAttributes, controller));
}

void BuildExecNodeAttributes(TExecNodePtr node, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
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

} // namespace NScheduler
} // namespace NYT

