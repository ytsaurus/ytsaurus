#include "stdafx.h"
#include "helpers.h"

#include <ytlib/ytree/fluent.h>

#include <ytlib/ypath/token.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////

TYPath GetOperationsPath()
{
    return "//sys/operations";
}

TYPath GetOperationPath(const TOperationId& operationId)
{
    return
        GetOperationsPath() + "/" +
        ToYPathLiteral(operationId.ToString());
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
        ToYPathLiteral(jobId.ToString());
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

bool IsOperationFinalizing(EOperationState state)
{
    return
        state == EOperationState::Completing ||
        state == EOperationState::Aborting ||
        state == EOperationState::Failing;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

