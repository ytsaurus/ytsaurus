#pragma once

#include "public.h"

#include <yt/core/ytree/public.h>

#include <yt/ytlib/transaction_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetOperationPath(const TOperationId& operationId);
NYPath::TYPath GetJobsPath(const TOperationId& operationId);
NYPath::TYPath GetJobPath(const TOperationId& operationId, const TJobId& jobId);
NYPath::TYPath GetStderrPath(const TOperationId& operationId, const TJobId& jobId);
NYPath::TYPath GetFailContextPath(const TOperationId& operationId, const TJobId& jobId);
NYPath::TYPath GetLivePreviewOutputPath(const TOperationId& operationId, int tableIndex);
NYPath::TYPath GetLivePreviewStderrTablePath(const TOperationId& operationId);
NYPath::TYPath GetLivePreviewIntermediatePath(const TOperationId& operationId);
NYPath::TYPath GetSnapshotPath(const TOperationId& operationId);
NYPath::TYPath GetSecureVaultPath(const TOperationId& operationId);

const NYPath::TYPath& GetPoolsPath();
const NYPath::TYPath& GetOperationsArchiveVersionPath();
const NYPath::TYPath& GetOperationsArchiveJobsPath();
const NYPath::TYPath& GetOperationsArchiveJobSpecsPath();

bool IsOperationFinished(EOperationState state);
bool IsOperationFinishing(EOperationState state);
bool IsOperationInProgress(EOperationState state);

void ValidateEnvironmentVariableName(const TStringBuf& name);

int GetJobSpecVersion();

bool IsSchedulingReason(EAbortReason reason);
bool IsNonSchedulingReason(EAbortReason reason);
bool IsSentinelReason(EAbortReason reason);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
