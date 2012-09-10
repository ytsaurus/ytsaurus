#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

NYTree::TYPath GetOperationsPath();
NYTree::TYPath GetOperationPath(const TOperationId& operationId);
NYTree::TYPath GetJobsPath(const TOperationId& operationId);
NYTree::TYPath GetJobPath(const TOperationId& operationId, const TJobId& jobId);
NYTree::TYPath GetStdErrPath(const TOperationId& operationId, const TJobId& jobId);

bool IsOperationFinished(EOperationState state);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
