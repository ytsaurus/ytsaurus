#pragma once

#include "public.h"

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/permission.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using TCoreInfos = std::vector<NProto::TCoreInfo>;

namespace NProto {

void Serialize(const TCoreInfo& coreInfo, NYson::IYsonConsumer* consumer);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetPoolTreesLockPath();
NYPath::TYPath GetOperationsPath();
NYPath::TYPath GetOperationPath(TOperationId operationId);
NYPath::TYPath GetJobsPath(TOperationId operationId);
NYPath::TYPath GetJobPath(TOperationId operationId, TJobId jobId);
NYPath::TYPath GetStderrPath(TOperationId operationId, TJobId jobId);
NYPath::TYPath GetSnapshotPath(TOperationId operationId);
NYPath::TYPath GetSecureVaultPath(TOperationId operationId);
NYPath::TYPath GetFailContextPath(TOperationId operationId, TJobId jobId);

NYPath::TYPath GetSchedulerOrchidOperationPath(TOperationId operationId);
NYPath::TYPath GetSchedulerOrchidAliasPath(const TString& alias);
NYPath::TYPath GetControllerAgentOrchidOperationPath(
    TStringBuf controllerAgentAddress,
    TOperationId operationId);
std::optional<TString> FindControllerAgentAddressFromCypress(
    TOperationId operationId,
    const NApi::NNative::IClientPtr& client);

NYPath::TYPath GetJobPath(
    TOperationId operationId,
    TJobId jobId,
    const TString& resourceName);

// TODO(ignat): move it to proper place.
const NYPath::TYPath& GetClusterNamePath();

const NYPath::TYPath& GetOperationsArchiveOrderedByIdPath();
const NYPath::TYPath& GetOperationsArchiveOperationAliasesPath();
const NYPath::TYPath& GetOperationsArchiveOrderedByStartTimePath();
const NYPath::TYPath& GetOperationsArchiveVersionPath();
const NYPath::TYPath& GetOperationsArchiveJobsPath();
const NYPath::TYPath& GetOperationsArchiveJobSpecsPath();
const NYPath::TYPath& GetOperationsArchiveJobStderrsPath();
const NYPath::TYPath& GetOperationsArchiveJobProfilesPath();
const NYPath::TYPath& GetOperationsArchiveJobFailContextsPath();
const NYPath::TYPath& GetOperationsArchiveOperationIdsPath();

const NYPath::TYPath& GetUserToDefaultPoolMapPath();

bool IsOperationFinished(EOperationState state);
bool IsOperationFinishing(EOperationState state);
bool IsOperationInProgress(EOperationState state);

void ValidateEnvironmentVariableName(TStringBuf name);
bool IsOperationWithUserJobs(EOperationType operationType);

int GetJobSpecVersion();

bool IsSchedulingReason(EAbortReason reason);
bool IsNonSchedulingReason(EAbortReason reason);
bool IsSentinelReason(EAbortReason reason);
bool IsJobAbsenceGuaranteed(EAbortReason reason);

TError GetSchedulerTransactionsAbortedError(const std::vector<NObjectClient::TTransactionId>& transactionIds);
TError GetUserTransactionAbortedError(NObjectClient::TTransactionId transactionId);

////////////////////////////////////////////////////////////////////////////////

struct TJobFile
{
    TJobId JobId;
    NYPath::TYPath Path;
    NChunkClient::TChunkId ChunkId;
    TString DescriptionType;
};

void SaveJobFiles(
    const NApi::NNative::IClientPtr& client,
    TOperationId operationId,
    const std::vector<TJobFile>& files);

////////////////////////////////////////////////////////////////////////////////

void ValidateOperationAccess(
    const std::optional<TString>& user,
    TOperationId operationId,
    TJobId jobId,
    NYTree::EPermissionSet permissionSet,
    const NSecurityClient::TSerializableAccessControlList& acl,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

TErrorOr<NApi::IUnversionedRowsetPtr> LookupOperationsInArchive(
    const NApi::NNative::IClientPtr& client,
    const std::vector<TOperationId>& ids,
    const NTableClient::TColumnFilter& columnFilter,
    std::optional<TDuration> timeout = {});

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPoolNameValidationLevel,
    (NonStrict)
    (Strict)
);

TError CheckPoolName(const TString& poolName, EPoolNameValidationLevel validationLevel = EPoolNameValidationLevel::NonStrict);
void ValidatePoolName(const TString& poolName, EPoolNameValidationLevel validationLevel = EPoolNameValidationLevel::NonStrict);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
