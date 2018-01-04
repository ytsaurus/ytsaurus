#include "helpers.h"

#include <yt/core/misc/error.h>
#include <yt/core/ypath/token.h>

#include <util/string/ascii.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYPath;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TYPath GetOperationsPath()
{
    return "//sys/operations";
}

TYPath GetOperationPath(const TOperationId& operationId)
{
    return "//sys/operations/" + ToYPathLiteral(ToString(operationId));
}

TYPath GetNewOperationPath(const TOperationId& operationId)
{
    int hashByte = operationId.Parts32[0] & 0xff;
    return
        "//sys/operations/" +
        Format("%02x", hashByte) +
        "/" +
        ToYPathLiteral(ToString(operationId));
}

TYPath GetOperationProgressFromOrchid(const TOperationId& operationId)
{
    return
        "//sys/scheduler/orchid/scheduler/operations/" +
        ToYPathLiteral(ToString(operationId)) +
        "/progress";
}

TYPath GetJobsPath(const TOperationId& operationId)
{
    return
        GetOperationPath(operationId) +
        "/jobs";
}

TYPath GetNewJobsPath(const TOperationId& operationId)
{
    return
        GetNewOperationPath(operationId) +
        "/jobs";
}

TYPath GetJobPath(const TOperationId& operationId, const TJobId& jobId)
{
    return
        GetJobsPath(operationId) + "/" +
        ToYPathLiteral(ToString(jobId));
}

TYPath GetNewJobPath(const TOperationId& operationId, const TJobId& jobId)
{
    return
        GetNewJobsPath(operationId) + "/" +
        ToYPathLiteral(ToString(jobId));
}

TYPath GetStderrPath(const TOperationId& operationId, const TJobId& jobId)
{
    return
        GetJobPath(operationId, jobId)
        + "/stderr";
}

TYPath GetNewStderrPath(const TOperationId& operationId, const TJobId& jobId)
{
    return
        GetNewJobPath(operationId, jobId)
        + "/stderr";
}

TYPath GetFailContextPath(const TOperationId& operationId, const TJobId& jobId)
{
    return
        GetJobPath(operationId, jobId)
        + "/fail_context";
}

TYPath GetSnapshotPath(const TOperationId& operationId)
{
    return
        GetOperationPath(operationId)
        + "/snapshot";
}

TYPath GetNewSnapshotPath(const TOperationId& operationId)
{
    return
        GetNewOperationPath(operationId)
        + "/snapshot";
}

TYPath GetSecureVaultPath(const TOperationId& operationId)
{
    return
        GetOperationPath(operationId)
        + "/secure_vault";
}

TYPath GetNewSecureVaultPath(const TOperationId& operationId)
{
    return
        GetNewOperationPath(operationId)
        + "/secure_vault";
}

std::vector<NYPath::TYPath> GetCompatibilityJobPaths(
    const TOperationId& operationId,
    const TJobId& jobId,
    EOperationCypressStorageMode mode,
    const TString& resourceName)
{
    TString suffix;
    if (!resourceName.empty()) {
        suffix = "/" + resourceName;
    }

    switch (mode) {
        case EOperationCypressStorageMode::Compatible:
            return {GetJobPath(operationId, jobId) + suffix, GetNewJobPath(operationId, jobId) + suffix};
        case EOperationCypressStorageMode::SimpleHashBuckets:
            return {GetJobPath(operationId, jobId) + suffix};
        case EOperationCypressStorageMode::HashBuckets:
            return {GetNewJobPath(operationId, jobId) + suffix};
        default:
            Y_UNREACHABLE();
    }
}

std::vector<NYPath::TYPath> GetCompatibilityOperationPaths(
    const TOperationId& operationId,
    EOperationCypressStorageMode mode,
    const TString& resourceName)
{
    TString suffix;
    if (!resourceName.empty()) {
        suffix = "/" + resourceName;
    }

    switch (mode) {
        case EOperationCypressStorageMode::Compatible:
            return {GetOperationPath(operationId) + suffix, GetNewOperationPath(operationId) + suffix};
        case EOperationCypressStorageMode::SimpleHashBuckets:
            return {GetOperationPath(operationId) + suffix};
        case EOperationCypressStorageMode::HashBuckets:
            return {GetNewOperationPath(operationId) + suffix};
        default:
            Y_UNREACHABLE();
    }
}

const TYPath& GetPoolsPath()
{
    static TYPath path =  "//sys/pool_trees";
    return path;
}

const TYPath& GetOperationsArchivePathOrderedById()
{
    static TYPath path = "//sys/operations_archive/ordered_by_id";
    return path;
}

const TYPath& GetOperationsArchivePathOrderedByStartTime()
{
    static TYPath path = "//sys/operations_archive/ordered_by_start_time";
    return path;
}

const TYPath& GetOperationsArchiveVersionPath()
{
    static TYPath path = "//sys/operations_archive/@version";
    return path;
}

const TYPath& GetOperationsArchiveJobsPath()
{
    static TYPath path = "//sys/operations_archive/jobs";
    return path;
}

const TYPath& GetOperationsArchiveJobSpecsPath()
{
    static TYPath path =  "//sys/operations_archive/job_specs";
    return path;
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
        state == EOperationState::Materializing ||
        state == EOperationState::Pending ||
        state == EOperationState::Reviving ||
        state == EOperationState::Running ||
        state == EOperationState::Completing ||
        state == EOperationState::Failing ||
        state == EOperationState::Aborting;
}

void ValidateEnvironmentVariableName(const TStringBuf& name)
{
    static const int MaximumNameLength = 1 << 16; // 64 kilobytes.
    if (name.size() > MaximumNameLength) {
        THROW_ERROR_EXCEPTION("Maximum length of the name for an environment variable violated: %v > %v",
            name.size(),
            MaximumNameLength);
    }
    for (char c : name) {
        if (!IsAsciiAlnum(c) && c != '_') {
            THROW_ERROR_EXCEPTION("Only alphanumeric characters and underscore are allowed in environment variable names")
                << TErrorAttribute("name", name);
        }
    }
}

int GetJobSpecVersion()
{
    return 2;
}

bool IsSchedulingReason(EAbortReason reason)
{
    return reason > EAbortReason::SchedulingFirst && reason < EAbortReason::SchedulingLast;
}

bool IsNonSchedulingReason(EAbortReason reason)
{
    return reason < EAbortReason::SchedulingFirst;
}

bool IsSentinelReason(EAbortReason reason)
{
    return
        reason == EAbortReason::SchedulingFirst ||
        reason == EAbortReason::SchedulingLast;
}

TError GetSchedulerTransactionAbortedError(const TTransactionId& transactionId)
{
    return TError("Scheduler transaction %v has expired or was aborted", transactionId);
}

TError GetUserTransactionAbortedError(const TTransactionId& transactionId)
{
    return TError("User transaction %v has expired or was aborted", transactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

