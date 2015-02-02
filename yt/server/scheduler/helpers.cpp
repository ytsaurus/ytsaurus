#include "stdafx.h"
#include "helpers.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"
#include "operation_controller.h"

#include <ytlib/object_client/helpers.h>

#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/security_client/public.h>

#include <ytlib/api/connection.h>

#include <core/concurrency/scheduler.h>

#include <core/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////

void BuildInitializingOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("operation_type").Value(operation->GetType())
        .Item("start_time").Value(operation->GetStartTime())
        .Item("spec").Value(operation->GetSpec())
        .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
        .Item("mutation_id").Value(operation->GetMutationId())
        .Do(BIND(&BuildRunningOperationAttributes, operation));
}

void BuildRunningOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer)
{
    auto userTransaction = operation->GetUserTransaction();
    auto syncTransaction = operation->GetSyncSchedulerTransaction();
    auto asyncTransaction = operation->GetAsyncSchedulerTransaction();
    auto inputTransaction = operation->GetInputTransaction();
    auto outputTransaction = operation->GetOutputTransaction();
    BuildYsonMapFluently(consumer)
        .Item("state").Value(operation->GetState())
        .Item("suspended").Value(operation->GetSuspended())
        .Item("user_transaction_id").Value(userTransaction ? userTransaction->GetId() : NullTransactionId)
        .Item("sync_scheduler_transaction_id").Value(syncTransaction ? syncTransaction->GetId() : NullTransactionId)
        .Item("async_scheduler_transaction_id").Value(asyncTransaction ? asyncTransaction->GetId() : NullTransactionId)
        .Item("input_transaction_id").Value(inputTransaction ? inputTransaction->GetId() : NullTransactionId)
        .Item("output_transaction_id").Value(outputTransaction ? outputTransaction->GetId() : NullTransactionId);
}

void BuildJobAttributes(TJobPtr job, NYson::IYsonConsumer* consumer)
{
    auto state = job->GetState();
    BuildYsonMapFluently(consumer)
        .Item("job_type").Value(FormatEnum(job->GetType()))
        .Item("state").Value(FormatEnum(state))
        .Item("address").Value(job->GetNode()->GetAddress())
        .Item("start_time").Value(job->GetStartTime())
        .Item("account").Value(TmpAccountName)
        .Item("progress").Value(job->GetProgress())
        .DoIf(job->GetFinishTime().HasValue(), [=] (TFluentMap fluent) {
            fluent.Item("finish_time").Value(job->GetFinishTime().Get());
        })
        .DoIf(state == EJobState::Failed, [=] (TFluentMap fluent) {
            auto error = FromProto<TError>(job->Result().error());
            fluent.Item("error").Value(error);
        });
}

void BuildExecNodeAttributes(TExecNodePtr node, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("resource_usage").Value(node->ResourceUsage())
        .Item("resource_limits").Value(node->ResourceLimits());
}

////////////////////////////////////////////////////////////////////////////////

i64 Clamp(i64 value, i64 minValue, i64 maxValue)
{
    value = std::min(value, maxValue);
    value = std::max(value, minValue);
    return value;
}

Stroka TrimCommandForBriefSpec(const Stroka& command)
{
    const int MaxBriefSpecCommandLength = 256;
    return
        command.length() <= MaxBriefSpecCommandLength
        ? command
        : command.substr(0, MaxBriefSpecCommandLength) + "...";
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

