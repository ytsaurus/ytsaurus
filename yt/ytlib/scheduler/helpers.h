#pragma once

#include "public.h"

#include <core/ytree/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetOperationPath(const TOperationId& operationId);
NYPath::TYPath GetJobsPath(const TOperationId& operationId);
NYPath::TYPath GetJobPath(const TOperationId& operationId, const TJobId& jobId);
NYPath::TYPath GetStderrPath(const TOperationId& operationId, const TJobId& jobId);
NYPath::TYPath GetFailContextRootPath(const TOperationId& operationId, const TJobId& jobId);
NYPath::TYPath GetFailContextPath(const TOperationId& operationId, const TJobId& jobId, size_t index);
NYPath::TYPath GetLivePreviewOutputPath(const TOperationId& operationId, int tableIndex);
NYPath::TYPath GetLivePreviewIntermediatePath(const TOperationId& operationId);
NYPath::TYPath GetSnapshotPath(const TOperationId& operationId);

bool IsOperationFinished(EOperationState state);
bool IsOperationFinishing(EOperationState state);
bool IsOperationInProgress(EOperationState state);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
