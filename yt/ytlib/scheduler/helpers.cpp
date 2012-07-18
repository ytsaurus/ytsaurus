#include "stdafx.h"
#include "helpers.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"
#include "operation_controller.h"

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/fluent.h>

#include <ytlib/exec_agent/helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NExecAgent;

////////////////////////////////////////////////////////////////////

TYPath GetOperationsPath()
{
    return "//sys/operations";
}

TYPath GetOperationPath(const TOperationId& operationId)
{
    return
        GetOperationsPath() + "/" +
        EscapeYPathToken(operationId.ToString());
}

TYPath GetJobsPath(const TOperationId& operationId)
{
    return
        GetOperationPath(operationId) +
        "/jobs";
}

TYPath GetJobPath(const TOperationId& operationId, const TJobId& jobId)
{
    return
        GetJobsPath(operationId) + "/" +
        EscapeYPathToken(jobId.ToString());
}

TYPath GetStdErrPath(const TOperationId& operationId, const TJobId& jobId)
{
    return
        GetJobPath(operationId, jobId)
        + "/stderr";
}

void BuildOperationAttributes(TOperationPtr operation, IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("operation_type").Scalar(CamelCaseToUnderscoreCase(operation->GetType().ToString()))
        .Item("transaction_id").Scalar(operation->GetTransactionId())
        .Item("state").Scalar(FormatEnum(operation->GetState()))
        .Item("start_time").Scalar(operation->GetStartTime())
        .Item("progress").BeginMap().EndMap()
        .Item("spec").Node(operation->GetSpec());
}

void BuildJobAttributes(TJobPtr job, NYTree::IYsonConsumer* consumer)
{
    // TODO(babenko): refactor this once new TError is ready
    auto state = job->GetState();
    auto error = TError::FromProto(job->Result().error());
    BuildYsonMapFluently(consumer)
        .Item("job_type").Scalar(FormatEnum(job->GetType()))
        .Item("state").Scalar(FormatEnum(state))
        .Item("address").Scalar(job->GetNode()->GetAddress())
        .DoIf(state == EJobState::Failed, [=] (TFluentMap fluent) {
            fluent.Item("error").BeginMap()
                .Item("code").Scalar(error.GetCode())
                .Item("message").Scalar(error.GetMessage())
            .EndMap();
        });
}

void BuildExecNodeAttributes(TExecNodePtr node, IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("resource_utilization").Do(BIND(&BuildNodeResourcesYson, node->ResourceUtilization()))
        .Item("resource_limits").Do(BIND(&BuildNodeResourcesYson, node->ResourceLimits()))
        .Item("job_count").Scalar(static_cast<int>(node->Jobs().size()));
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

