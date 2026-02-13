#pragma once

#include "public.h"

#include <yt/yt/ytlib/controller_agent/helpers.h>
#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>
#include <yt/yt/ytlib/scheduler/proto/scheduler_service.pb.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/permission.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/logging/log.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TAllocationId AllocationIdFromJobId(TJobId jobId);

////////////////////////////////////////////////////////////////////////////////

void ApplyJobShellOptionsUpdate(TJobShellOptionsMap* origin, const TJobShellOptionsUpdateMap& update);

class TJobShellInfo
{
public:
    TJobShellInfo(TJobShellPtr jobShell, TOperationJobShellRuntimeParametersPtr jobShellRuntimeParameters);

    const std::vector<std::string>& GetOwners();

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
std::optional<std::string> FindControllerAgentAddressFromCypress(
    TOperationId operationId,
    const NApi::NNative::IClientPtr& client);

NYPath::TYPath GetJobPath(
    TOperationId operationId,
    TJobId jobId,
    const TString& resourceName);

// TODO(ignat): move it to proper place.
const NYPath::TYPath& GetClusterNamePath();

const NYPath::TYPath& GetOperationsArchivePath();
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
const NYPath::TYPath& GetOperationsArchiveJobTraceEventsPath();
const NYPath::TYPath& GetOperationsArchiveJobTracesPath();
const NYPath::TYPath& GetOperationsArchiveOperationEventsPath();

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

NYPath::TYPath GetOperationsAcoPrincipalPath(TStringBuf acoName);

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString GetAclFromAcoName(
    const NApi::NNative::IClientPtr& client,
    const std::string& acoName);

////////////////////////////////////////////////////////////////////////////////

// Either of ACL or ACO name.
class TAccessControlRule
{
public:
    TAccessControlRule() = default;
    TAccessControlRule(const TAccessControlRule&) = default;
    TAccessControlRule(TAccessControlRule&&) = default;

    TAccessControlRule& operator=(const TAccessControlRule&) = default;
    TAccessControlRule& operator=(TAccessControlRule&&) = default;

    explicit TAccessControlRule(NSecurityClient::TSerializableAccessControlList acl);
    explicit TAccessControlRule(std::string acoName);

    bool IsAcoName() const;
    bool IsAcl() const;

    std::string GetAcoName() const;
    void SetAcoName(std::string aco);

    NSecurityClient::TSerializableAccessControlList GetAcl() const;
    void SetAcl(NSecurityClient::TSerializableAccessControlList acl);

    NSecurityClient::TSerializableAccessControlList GetOrLookupAcl(const NApi::NNative::IClientPtr& client) const;

    TString GetAclString() const;

private:
    std::variant<NSecurityClient::TSerializableAccessControlList, std::string> AccessControlRule_;
};

////////////////////////////////////////////////////////////////////////////////

std::optional<TAccessControlRule> TryGetAccessControlRuleFromOperation(const NApi::TOperation& operation);

////////////////////////////////////////////////////////////////////////////////

TError CheckOperationAccessByAco(
    const std::optional<std::string>& user,
    TOperationId operationId,
    TJobId jobId,
    NYTree::EPermissionSet permissionSet,
    const std::string& acoName,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger);

TError CheckOperationAccessByAcl(
    const std::optional<std::string>& user,
    TOperationId operationId,
    TJobId jobId,
    NYTree::EPermissionSet permissionSet,
    const NSecurityClient::TSerializableAccessControlList& acl,
    const NApi::IClientPtr& client,
    const NLogging::TLogger& logger);

void ValidateOperationAccess(
    const std::optional<std::string>& user,
    TOperationId operationId,
    TJobId jobId,
    NYTree::EPermissionSet permissionSet,
    const TAccessControlRule& accessControlRule,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger);

void ValidateOperationAccess(
    const std::optional<std::string>& user,
    TOperationId operationId,
    NYTree::EPermissionSet permissionSet,
    const TAccessControlRule& accessControlRule,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger);

// Unlike ValidateOperationAccess returns an error rather than throwing it.
TError CheckOperationAccess(
    const std::optional<std::string>& user,
    TOperationId operationId,
    NYTree::EPermissionSet permissionSet,
    const TAccessControlRule& accessControlRule,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

TErrorOr<NApi::IUnversionedRowsetPtr> LookupOperationsInArchive(
    const NApi::NNative::IClientPtr& client,
    const std::vector<TOperationId>& ids,
    const NTableClient::TColumnFilter& columnFilter,
    std::optional<TDuration> timeout = {});

////////////////////////////////////////////////////////////////////////////////

TError CheckPoolName(const std::string& poolName, const re2::RE2& regex);
void ValidatePoolName(const std::string& poolName, const re2::RE2& regex);

////////////////////////////////////////////////////////////////////////////////

struct TAllocationBriefInfo
{
    NScheduler::TAllocationId AllocationId;
    NJobTrackerClient::TOperationId OperationId;
    std::optional<NSecurityClient::TSerializableAccessControlList> OperationAcl;
    std::optional<std::string> OperationAcoName;
    NControllerAgent::TControllerAgentDescriptor ControllerAgentDescriptor;
    NNodeTrackerClient::TNodeDescriptor NodeDescriptor;
};

TAccessControlRule GetAcrFromAllocationBriefInfo(const TAllocationBriefInfo& allocationBriefInfo);

struct TAllocationInfoToRequest
{
    bool OperationId = false;
    bool OperationAcl = false;
    bool OperationAcoName = false;
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

void FromProto(
    TGracefulShutdownSpec* gracefulShutdownSpec,
    const NControllerAgent::NProto::TGracefulShutdownSpec& gracefulShutdownSpecProto);

void ToProto(
    NControllerAgent::NProto::TGracefulShutdownSpec* gracefulShutdownSpecProto,
    const TGracefulShutdownSpec& gracefulShutdownSpec);

void FromProto(
    TSidecarJobSpec* sidecarJobSpec,
    const NControllerAgent::NProto::TSidecarJobSpec& sidecarJobSpecProto);

void ToProto(
    NControllerAgent::NProto::TSidecarJobSpec* sidecarJobSpecProto,
    const TSidecarJobSpec& sidecarJobSpec);

void FromProto(
    TVolume* volume,
    const NControllerAgent::NProto::TVolume& volumeProto);

void ToProto(
    NControllerAgent::NProto::TVolume* volumeProto,
    const TVolume& volume);

void FromProto(
    TVolumeMount* volumeMount,
    const NControllerAgent::NProto::TVolumeMount& volumeMountProto);

void ToProto(
    NControllerAgent::NProto::TVolumeMount* volumeMountProto,
    const TVolumeMount& volumeMount);

void FromProto(
    TTmpfsVolumeConfig* tmpfsVolumeConfig,
    const NControllerAgent::NProto::TTmpfsVolume& protoTmpfsVolume);

void ToProto(
    NControllerAgent::NProto::TTmpfsVolume* protoTmpfsVolume,
    const TTmpfsVolumeConfig& tmpfsVolumeConfig);

void ToProto(
    NProto::TNbdDisk* protoNbdDisk,
    const TNbdDiskConfig& nbdDiskConfig);

void FromProto(
    TNbdDiskConfig* nbdDiskConfig,
    const NProto::TNbdDisk& protoNbdDisk);

void FromProto(
    TStorageRequestConfig* diskRequestConfig,
    const NProto::TDiskRequest& protoDiskRequest);

void ToProto(
    NProto::TDiskRequest* protoDiskRequest,
    const TStorageRequestConfig& diskRequestConfig);

template <class TProtoDiskRequest>
void FromProto(
    TNbdDiskRequest* diskRequestConfig,
    const TProtoDiskRequest& protoDiskRequestConfig);

template <class TProtoDiskRequest>
void ToProto(
    TProtoDiskRequest* protoDiskRequestConfig,
    const TNbdDiskRequest& diskRequestConfig);

template <class TProtoDiskRequest>
void FromProto(
    TLocalDiskRequest* diskRequestConfig,
    const TProtoDiskRequest& protoDiskRequestConfig);

template <class TProtoDiskRequest>
void ToProto(
    TProtoDiskRequest* protoDiskRequestConfig,
    const TLocalDiskRequest& diskRequestConfig);

template <class TProtoDiskRequest>
void FromProto(
    TDiskRequestConfig* diskRequestConfig,
    const TProtoDiskRequest& protoDiskRequestConfig);

template <class TProtoDiskRequest>
void ToProto(
    TProtoDiskRequest* protoDiskRequestConfig,
    const TDiskRequestConfig& diskRequestConfig);

void FromProto(
    TTmpfsStorageRequest* diskRequestConfig,
    const NProto::TTmpfsStorageRequest& protoDiskRequestConfig);

void ToProto(
    NProto::TTmpfsStorageRequest* protoDiskRequestConfig,
    const TTmpfsStorageRequest& diskRequestConfig);

void FromProto(
    TStorageRequestConfig* diskRequestConfig,
    const NProto::TDeprecatedDiskRequest& protoDiskRequestConfig);

template <class TProtoDiskRequest>
void FromProto(
    TStorageRequestBase* diskRequestConfig,
    const TProtoDiskRequest& protoDiskRequestConfig);

template <class TProtoDiskRequest>
void ToProto(
    TProtoDiskRequest* protoDiskRequestConfig,
    const TStorageRequestBase& diskRequestConfig);

////////////////////////////////////////////////////////////////////////////////

void ValidateTmpfsPaths(const std::vector<std::string_view>& tmpfsPaths);

int CountNonTmpfsVolumes(const THashMap<std::string, TVolumePtr>& volumes);

////////////////////////////////////////////////////////////////////////////////

bool IsDiskRequestTmpfs(const std::optional<NScheduler::TStorageRequestConfig>& diskRequest);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

#define HELPERS_INL_H
#include "helpers-inl.h"
#undef HELPERS_INL_H
