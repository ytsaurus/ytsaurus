#pragma once

#include "public.h"

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>
#include <yt/yt/ytlib/scheduler/proto/scheduler_service.pb.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/permission.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/security_client/acl.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

void ApplyJobShellOptionsUpdate(TJobShellOptionsMap* origin, const TJobShellOptionsUpdeteMap& update);

class TJobShellInfo
{
public:
    TJobShellInfo(TJobShellPtr jobShell, TOperationJobShellRuntimeParametersPtr jobShellRuntimeParameters);

    const std::vector<TString>& GetOwners();

    const TString& GetSubcontainerName();

private:
    const TJobShellPtr JobShell_;
    const TOperationJobShellRuntimeParametersPtr JobShellRuntimeParameters_;
};

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

bool IsSchedulingReason(EAbortReason reason);
bool IsNonSchedulingReason(EAbortReason reason);
bool IsSentinelReason(EAbortReason reason);

TError GetSchedulerTransactionsAbortedError(const std::vector<NObjectClient::TTransactionId>& transactionIds);
TError GetUserTransactionAbortedError(NObjectClient::TTransactionId transactionId);

////////////////////////////////////////////////////////////////////////////////

void ValidateOperationAccess(
    const std::optional<TString>& user,
    TOperationId operationId,
    TAllocationId allocationId,
    NYTree::EPermissionSet permissionSet,
    const NSecurityClient::TSerializableAccessControlList& acl,
    const NApi::IClientPtr& client,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

TErrorOr<NApi::IUnversionedRowsetPtr> LookupOperationsInArchive(
    const NApi::NNative::IClientPtr& client,
    const std::vector<TOperationId>& ids,
    const NTableClient::TColumnFilter& columnFilter,
    std::optional<TDuration> timeout = {});

////////////////////////////////////////////////////////////////////////////////

TError CheckPoolName(const TString& poolName, const re2::RE2& regex);
void ValidatePoolName(const TString& poolName, const re2::RE2& regex);

////////////////////////////////////////////////////////////////////////////////

struct TAllocationBriefInfo
{
    NScheduler::TAllocationId AllocationId;
    NJobTrackerClient::TOperationId OperationId;
    std::optional<NSecurityClient::TSerializableAccessControlList> OperationAcl;
    NControllerAgent::TControllerAgentDescriptor ControllerAgentDescriptor;
    NNodeTrackerClient::TNodeDescriptor NodeDescriptor;
};

struct TAllocationInfoToRequest
{
    bool OperationId = false;
    bool OperationAcl = false;
    bool ControllerAgentDescriptor = false;
    bool NodeDescriptor = false;
};

void FromProto(
    TAllocationBriefInfo* allocationBriefInfo,
    const NProto::TAllocationBriefInfo& allocationBriefInfoProto);

void ToProto(
    NProto::TAllocationBriefInfo* allocationBriefInfoProto,
    const TAllocationBriefInfo& allocationBriefInfo);

void FromProto(
    TAllocationInfoToRequest* requestedAllocationInfo,
    const NProto::TReqGetAllocationBriefInfo::TRequestedInfo& requestedAllocationInfoProto);

void ToProto(
    NProto::TReqGetAllocationBriefInfo::TRequestedInfo* allocationInfoToRequestProto,
    const TAllocationInfoToRequest& allocationInfoToRequest);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
