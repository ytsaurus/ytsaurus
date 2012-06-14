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

void BuildOperationAttributes(TOperationPtr operation, NYTree::IYsonConsumer* consumer);
void BuildJobAttributes(TJobPtr job, NYTree::IYsonConsumer* consumer);
void BuildExecNodeAttributes(TExecNodePtr node, NYTree::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
