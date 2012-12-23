#include "stdafx.h"
#include "helpers.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"
#include "operation_controller.h"
#include "job_resources.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////

void BuildOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("operation_type").Value(operation->GetType())
        .Item("user_transaction_id").Value(operation->GetUserTransaction() ? operation->GetUserTransaction()->GetId() : NullTransactionId)
        .Item("scheduler_transaction_id").Value(operation->GetUserTransaction() ? operation->GetSchedulerTransaction()->GetId() : NullTransactionId)
        .Item("state").Value(FormatEnum(operation->GetState()))
        .Item("start_time").Value(operation->GetStartTime())
        .Item("spec").Node(operation->GetSpec());
}

void BuildJobAttributes(TJobPtr job, NYson::IYsonConsumer* consumer)
{
    auto state = job->GetState();
    BuildYsonMapFluently(consumer)
        .Item("job_type").Value(FormatEnum(job->GetType()))
        .Item("state").Value(FormatEnum(state))
        .Item("address").Value(job->GetNode()->GetAddress())
        .DoIf(state == EJobState::Failed, [=] (TFluentMap fluent) {
            auto error = FromProto(job->Result().error());
            fluent.Item("error").Value(error);
        });
}

void BuildExecNodeAttributes(TExecNodePtr node, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("resource_usage").Value(node->ResourceUsage())
        .Item("resource_limits").Value(node->ResourceLimits());
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

