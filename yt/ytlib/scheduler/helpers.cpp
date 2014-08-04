#include "stdafx.h"
#include "helpers.h"

#include <core/ypath/token.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////

TYPath GetOperationPath(const TOperationId& operationId)
{
    return
        "//sys/operations/" +
        ToYPathLiteral(ToString(operationId));
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
        ToYPathLiteral(ToString(jobId));
}

TYPath GetStderrPath(const TOperationId& operationId, const TJobId& jobId)
{
    return
        GetJobPath(operationId, jobId)
        + "/stderr";
}

TYPath GetFailContextRootPath(const TOperationId& operationId, const TJobId& jobId)
{
    return
        GetJobPath(operationId, jobId)
        + "/fail_contexts";
}

TYPath GetFailContextPath(const TOperationId& operationId, const TJobId& jobId, size_t index)
{
    return
        GetFailContextRootPath(operationId, jobId)
        + "/"
        + ToString(index);
}

TYPath GetSnapshotPath(const TOperationId& operationId)
{
    return
        GetOperationPath(operationId)
        + "/snapshot";
}

TYPath GetLivePreviewOutputPath(const TOperationId& operationId, int tableIndex)
{
    return
        GetOperationPath(operationId)
        + "/output_" + ToString(tableIndex);
}

TYPath GetLivePreviewIntermediatePath(const TOperationId& operationId)
{
    return
        GetOperationPath(operationId)
        + "/intermediate";
}

bool IsOperationFinished(EOperationState state)
{
    return
        state == EOperationState::Completed ||
        state == EOperationState::Aborted ||
        state == EOperationState::Failed;
}

bool IsOperationFinishing(EOperationState state)
{
    return
        state == EOperationState::Completing ||
        state == EOperationState::Aborting ||
        state == EOperationState::Failing;
}

bool IsOperationInProgress(EOperationState state)
{
    return
        state == EOperationState::Initializing ||
        state == EOperationState::Preparing ||
        state == EOperationState::Reviving ||
        state == EOperationState::Running ||
        state == EOperationState::Completing ||
        state == EOperationState::Failing;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

