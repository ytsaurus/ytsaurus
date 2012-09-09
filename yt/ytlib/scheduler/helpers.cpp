#include "stdafx.h"
#include "helpers.h"

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;

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

bool IsOperationFinished(EOperationState state)
{
    return
        state == EOperationState::Completed ||
        state == EOperationState::Aborted ||
        state == EOperationState::Failed;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

