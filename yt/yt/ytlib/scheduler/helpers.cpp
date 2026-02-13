#include "helpers.h"

#include "config.h"
#include "yt/yt/client/table_client/record_helpers.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/scheduler/records/ordered_by_id.record.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/security_client/access_control.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/ypath_resolver.h>

#include <yt/yt/library/re2/re2.h>

#include <util/folder/path.h>


namespace NYT::NScheduler {

using namespace NApi;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NSecurityClient;
using namespace NLogging;
using namespace NRpc;
using namespace NTracing;
using namespace NSecurityClient;

using NYT::FromProto;
using NYT::ToProto;

using NJobTrackerClient::NullJobId;

////////////////////////////////////////////////////////////////////////////////

TAllocationId AllocationIdFromJobId(TJobId jobId)
{
    auto allocationIdGuid = jobId.Underlying();
    allocationIdGuid.Parts32[0] &= (1 << 24) - 1;
    return TAllocationId(allocationIdGuid);
}

////////////////////////////////////////////////////////////////////////////////

void ApplyJobShellOptionsUpdate(TJobShellOptionsMap* origin, const TJobShellOptionsUpdateMap& update)
{
    for (const auto& [jobShellName, options] : update) {
        if (!options) {
            origin->erase(jobShellName);
        } else {
            (*origin)[jobShellName] = *options;
        }
    }
}

TJobShellInfo::TJobShellInfo(
    TJobShellPtr jobShell,
    TOperationJobShellRuntimeParametersPtr jobShellRuntimeParameters)
    : JobShell_(std::move(jobShell))
    , JobShellRuntimeParameters_(std::move(jobShellRuntimeParameters))
{
    YT_VERIFY(JobShell_);
}

const std::vector<std::string>& TJobShellInfo::GetOwners()
{
    if (JobShellRuntimeParameters_) {
        return JobShellRuntimeParameters_->Owners;
    }

    return JobShell_->Owners;
}

const TString& TJobShellInfo::GetSubcontainerName()
{
    return JobShell_->Subcontainer;
}

////////////////////////////////////////////////////////////////////////////////

TYPath GetPoolTreesLockPath()
{
    return "//sys/scheduler/pool_trees_lock";
}

TYPath GetOperationsPath()
{
    return "//sys/operations";
}

TYPath GetOperationPath(TOperationId operationId)
{
    int hashByte = operationId.Underlying().Parts32[0] & 0xff;
    return
        "//sys/operations/" +
        Format("%02x", hashByte) +
        "/" +
        ToYPathLiteral(ToString(operationId));
}

TYPath GetJobsPath(TOperationId operationId)
{
    return
        GetOperationPath(operationId) +
        "/jobs";
}

TYPath GetJobPath(TOperationId operationId, TJobId jobId)
{
    return
        GetJobsPath(operationId) + "/" +
        ToYPathLiteral(ToString(jobId));
}

TYPath GetStderrPath(TOperationId operationId, TJobId jobId)
{
    return
        GetJobPath(operationId, jobId)
        + "/stderr";
}

TYPath GetFailContextPath(TOperationId operationId, TJobId jobId)
{
    return
        GetJobPath(operationId, jobId)
        + "/fail_context";
}

TYPath GetSchedulerOrchidOperationPath(TOperationId operationId)
{
    return
        "//sys/scheduler/orchid/scheduler/operations/" +
        ToYPathLiteral(ToString(operationId));
}

TYPath GetSchedulerOrchidAliasPath(const TString& alias)
{
    return
        "//sys/scheduler/orchid/scheduler/operations/" +
        ToYPathLiteral(alias);
}

TYPath GetControllerAgentOrchidOperationPath(
    TStringBuf controllerAgentAddress,
    TOperationId operationId)
{
    return
        "//sys/controller_agents/instances/" +
        ToYPathLiteral(controllerAgentAddress) +
        "/orchid/controller_agent/operations/" +
        ToYPathLiteral(ToString(operationId));
}

const TYPath& GetUserToDefaultPoolMapPath()
{
    static const TYPath path = "//sys/scheduler/user_to_default_pool";
    return path;
}

std::optional<std::string> FindControllerAgentAddressFromCypress(
    TOperationId operationId,
    const NApi::NNative::IClientPtr& client)
{
    using NYT::ToProto;

    static const std::vector<std::string> attributes = {"controller_agent_address"};

    auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower);
    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TYPathProxy::Get(GetOperationPath(operationId) + "/@controller_agent_address");
        ToProto(req->mutable_attributes()->mutable_keys(), attributes);
        batchReq->AddRequest(req, "get_controller_agent_address");
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    auto responseOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_controller_agent_address");
    if (responseOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
        return std::nullopt;
    }

    const auto& response = responseOrError.ValueOrThrow();
    return ConvertTo<TString>(TYsonString(response->value()));
}

TYPath GetSnapshotPath(TOperationId operationId)
{
    return
        GetOperationPath(operationId)
        + "/snapshot";
}

TYPath GetSecureVaultPath(TOperationId operationId)
{
    return
        GetOperationPath(operationId)
        + "/secure_vault";
}

NYPath::TYPath GetJobPath(
    TOperationId operationId,
    TJobId jobId,
    const TString& resourceName)
{
    TString suffix;
    if (!resourceName.empty()) {
        suffix = "/" + resourceName;
    }

    return GetJobPath(operationId, jobId) + suffix;
}

const TYPath& GetClusterNamePath()
{
    static const TYPath path = "//sys/@cluster_name";
    return path;
}

const TYPath& GetOperationsArchivePath()
{
    static const TYPath path = "//sys/operations_archive";
    return path;
}

const TYPath& GetOperationsArchiveOrderedByIdPath()
{
    static const TYPath path = GetOperationsArchivePath() + "/ordered_by_id";
    return path;
}

const TYPath& GetOperationsArchiveOrderedByStartTimePath()
{
    static const TYPath path = GetOperationsArchivePath() + "/ordered_by_start_time";
    return path;
}

const TYPath& GetOperationsArchiveOperationAliasesPath()
{
    static const TYPath path = GetOperationsArchivePath() + "/operation_aliases";
    return path;
}

const TYPath& GetOperationsArchiveVersionPath()
{
    static const TYPath path = GetOperationsArchivePath() + "/@version";
    return path;
}

const TYPath& GetOperationsArchiveJobsPath()
{
    static const TYPath path = GetOperationsArchivePath() + "/jobs";
    return path;
}

const TYPath& GetOperationsArchiveJobSpecsPath()
{
    static const TYPath path = GetOperationsArchivePath() + "/job_specs";
    return path;
}

const TYPath& GetOperationsArchiveJobStderrsPath()
{
    static const TYPath path = GetOperationsArchivePath() + "/stderrs";
    return path;
}

const TYPath& GetOperationsArchiveJobProfilesPath()
{
    static const TYPath path = GetOperationsArchivePath() + "/job_profiles";
    return path;
}

const TYPath& GetOperationsArchiveJobFailContextsPath()
{
    static const TYPath path = GetOperationsArchivePath() + "/fail_contexts";
    return path;
}

const NYPath::TYPath& GetOperationsArchiveOperationIdsPath()
{
    static const TYPath path = GetOperationsArchivePath() + "/operation_ids";
    return path;
}

const NYPath::TYPath& GetOperationsArchiveJobTraceEventsPath()
{
    static const TYPath path = GetOperationsArchivePath() + "/job_trace_events";
    return path;
}

const NYPath::TYPath& GetOperationsArchiveJobTracesPath()
{
    static const TYPath path = GetOperationsArchivePath() + "/job_traces";
    return path;
}

const NYPath::TYPath& GetOperationsArchiveOperationEventsPath()
{
    static const TYPath path = GetOperationsArchivePath() + "/operation_events";
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
        state == EOperationState::Starting ||
        state == EOperationState::WaitingForAgent ||
        state == EOperationState::Orphaned ||
        state == EOperationState::Initializing ||
        state == EOperationState::Preparing ||
        state == EOperationState::Materializing ||
        state == EOperationState::Pending ||
        state == EOperationState::ReviveInitializing ||
        state == EOperationState::Reviving ||
        state == EOperationState::RevivingJobs ||
        state == EOperationState::Running ||
        state == EOperationState::Completing ||
        state == EOperationState::Failing ||
        state == EOperationState::Aborting;
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

TError GetSchedulerTransactionsAbortedError(const std::vector<TTransactionId>& transactionIds)
{
    return TError(
        NTransactionClient::EErrorCode::NoSuchTransaction,
        "Scheduler transactions %v have expired or were aborted",
        transactionIds);
}

TError GetUserTransactionAbortedError(TTransactionId transactionId)
{
    return TError(
        NTransactionClient::EErrorCode::NoSuchTransaction,
        "User transaction %v has expired or was aborted",
        transactionId);
}

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetOperationsAcoPrincipalPath(TStringBuf acoName)
{
    static constexpr TStringBuf OperationsAccessControlObjectPrincipalPath = "//sys/access_control_object_namespaces/operations/%v/principal";
    return Format(OperationsAccessControlObjectPrincipalPath, ToYPathLiteral(acoName));
}

////////////////////////////////////////////////////////////////////////////////

TYsonString GetAclFromAcoName(const NApi::NNative::IClientPtr& client, const std::string& acoName)
{
    TGetNodeOptions getNodeOptions;
    getNodeOptions.ReadFrom = EMasterChannelKind::ClientSideCache;
    return NConcurrency::WaitFor(
        client->GetNode(
            GetOperationsAcoPrincipalPath(acoName) + "/@acl"))
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

TAccessControlRule::TAccessControlRule(TSerializableAccessControlList acl)
    : AccessControlRule_(std::move(acl))
{ }

TAccessControlRule::TAccessControlRule(std::string acoName)
    : AccessControlRule_(std::move(acoName))
{ }

bool TAccessControlRule::IsAcoName() const
{
    return std::holds_alternative<std::string>(AccessControlRule_);
}

bool TAccessControlRule::IsAcl() const
{
    return !IsAcoName();
}

std::string TAccessControlRule::GetAcoName() const
{
    return std::get<std::string>(AccessControlRule_);
}

void TAccessControlRule::SetAcoName(std::string aco)
{
    AccessControlRule_ = std::move(aco);
}

TSerializableAccessControlList TAccessControlRule::GetAcl() const
{
    return std::get<TSerializableAccessControlList>(AccessControlRule_);
}

void TAccessControlRule::SetAcl(TSerializableAccessControlList acl)
{
    AccessControlRule_ = std::move(acl);
}

TSerializableAccessControlList TAccessControlRule::GetOrLookupAcl(const NApi::NNative::IClientPtr& client) const
{
    if (IsAcl()) {
        return GetAcl();
    } else {
        auto aclYson = GetAclFromAcoName(client, GetAcoName());
        return ConvertTo<TSerializableAccessControlList>(aclYson);
    }
}

TString TAccessControlRule::GetAclString() const
{
    if (IsAcoName()) {
        return GetAcoName();
    } else {
        return BuildYsonStringFluently(EYsonFormat::Text)
            .Value(GetAcl())
            .ToString();
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TCheckPermissionResults>
TError ValidateCheckPermissionsResults(
    const std::optional<std::string>& user,
    TOperationId operationId,
    TJobId jobId,
    EPermissionSet permissionSet,
    const TCheckPermissionResults& results,
    const TAccessControlRule& accessControlRule,
    const TLogger& logger)
{
    auto Logger = logger;

    if (operationId) {
        Logger.AddTag("OperationId: %v", operationId);
    }
    if (jobId) {
        Logger.AddTag("JobId: %v", jobId);
    }

    for (const auto& result : results) {
        if (result.Action == ESecurityAction::Allow) {
            continue;
        }
        auto userStr = user.value_or(GetCurrentAuthenticationIdentity().User);
        auto error = TError(
            NSecurityClient::EErrorCode::AuthorizationError,
            "Operation access denied")
            << TErrorAttribute("user", userStr)
            << TErrorAttribute("required_permissions", permissionSet);
        if (operationId) {
            error = error
                << TErrorAttribute("operation_id", operationId);
        }
        if (jobId) {
            error = error
                << TErrorAttribute("job_id", jobId);
        }
        return error;
    }

    YT_LOG_DEBUG("Operation access successfully validated (User: %v, Permissions: %v, AccessControlRule: %v)",
        user,
        permissionSet,
        accessControlRule.GetAclString());

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::optional<TAccessControlRule> TryGetAccessControlRuleFromOperation(const TOperation& operation)
{
    auto acoName = TryGetString(operation.RuntimeParameters.AsStringBuf(), "/aco_name");
    auto aclYson = TryGetAny(operation.RuntimeParameters.AsStringBuf(), "/acl");

    if (aclYson) {
        auto acl = ConvertTo<NSecurityClient::TSerializableAccessControlList>(TYsonStringBuf(*aclYson));
        return TAccessControlRule(acl);
    }

    if (acoName) {
        return TAccessControlRule(*acoName);
    }

    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

TError CheckOperationAccessByAco(
    const std::optional<std::string>& user,
    TOperationId operationId,
    TJobId jobId,
    EPermissionSet permissionSet,
    const std::string& acoName,
    const NNative::IClientPtr& client,
    const TLogger& logger)
{
    auto authenticatedUser = user.value_or(GetCurrentAuthenticationIdentity().User);

    std::vector<TFuture<TCheckPermissionResponse>> futures;
    for (auto permission : TEnumTraits<EPermission>::GetDomainValues()) {
        if (Any(permission & permissionSet)) {
            futures.push_back(client->CheckPermission(authenticatedUser, GetOperationsAcoPrincipalPath(acoName), permission));
        }
    }

    auto results = WaitFor(AllSucceeded(futures))
        .ValueOrThrow();

    return ValidateCheckPermissionsResults(
        authenticatedUser,
        operationId,
        jobId,
        permissionSet,
        results,
        TAccessControlRule(acoName),
        logger);
}

TError CheckOperationAccessByAcl(
    const std::optional<std::string>& user,
    TOperationId operationId,
    TJobId jobId,
    EPermissionSet permissionSet,
    const TSerializableAccessControlList& acl,
    const IClientPtr& client,
    const TLogger& logger)
{
    auto Logger = logger;

    if (operationId) {
        Logger.AddTag("OperationId: %v", operationId);
    }
    if (jobId) {
        Logger.AddTag("JobId: %v", jobId);
    }

    TCheckPermissionByAclOptions options;
    options.IgnoreMissingSubjects = true;
    auto aclNode = ConvertToNode(acl);

    std::vector<TFuture<TCheckPermissionByAclResult>> futures;
    for (auto permission : TEnumTraits<EPermission>::GetDomainValues()) {
        if (Any(permission & permissionSet)) {
            futures.push_back(client->CheckPermissionByAcl(user, permission, aclNode, options));
        }
    }

    auto results = WaitFor(AllSucceeded(futures))
        .ValueOrThrow();

    if (!results.empty() && !results.front().MissingSubjects.empty()) {
        YT_LOG_DEBUG(
            "Operation has missing subjects in ACL (MissingSubjects: %v)",
            results.front().MissingSubjects);
    }

    return ValidateCheckPermissionsResults(
        user,
        operationId,
        jobId,
        permissionSet,
        results,
        TAccessControlRule(acl),
        logger);
}

void ValidateOperationAccess(
    const std::optional<std::string>& user,
    TOperationId operationId,
    TJobId jobId,
    NYTree::EPermissionSet permissionSet,
    const TAccessControlRule& accessControlRule,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger)
{
    TError error;
    if (accessControlRule.IsAcoName()) {
        error = NScheduler::CheckOperationAccessByAco(
            user,
            operationId,
            jobId,
            permissionSet,
            accessControlRule.GetAcoName(),
            client,
            logger);
    } else {
        error = NScheduler::CheckOperationAccessByAcl(
            user,
            operationId,
            jobId,
            permissionSet,
            accessControlRule.GetAcl(),
            client,
            logger);
    }

    error.ThrowOnError();
}


void ValidateOperationAccess(
    const std::optional<std::string>& user,
    TOperationId operationId,
    NYTree::EPermissionSet permissionSet,
    const TAccessControlRule& accessControlRule,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger)
{
    ValidateOperationAccess(
        user,
        operationId,
        NullJobId,
        permissionSet,
        accessControlRule,
        client,
        logger);
}

TError CheckOperationAccess(
    const std::optional<std::string>& user,
    TOperationId operationId,
    NYTree::EPermissionSet permissionSet,
    const TAccessControlRule& accessControlRule,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger)
{
    if (accessControlRule.IsAcoName()) {
        return NScheduler::CheckOperationAccessByAco(
            user,
            operationId,
            NullJobId,
            permissionSet,
            accessControlRule.GetAcoName(),
            client,
            logger);
    }
    return NScheduler::CheckOperationAccessByAcl(
        user,
        operationId,
        NullJobId,
        permissionSet,
        accessControlRule.GetAcl(),
        client,
        logger);
}
////////////////////////////////////////////////////////////////////////////////

static NRecords::TOrderedByIdKey CreateOperationKey(const TOperationId& operationId)
{
    auto operationIdAsGuid = operationId.Underlying();
    NRecords::TOrderedByIdKey recordKey{
        .IdHi = operationIdAsGuid.Parts64[0],
        .IdLo = operationIdAsGuid.Parts64[1],
    };
    return recordKey;
}

TErrorOr<IUnversionedRowsetPtr> LookupOperationsInArchive(
    const NNative::IClientPtr& client,
    const std::vector<TOperationId>& ids,
    const NTableClient::TColumnFilter& columnFilter,
    std::optional<TDuration> timeout)
{
    std::vector<NRecords::TOrderedByIdKey> keys;
    keys.reserve(ids.size());
    for (const auto& id : ids) {
        keys.push_back(CreateOperationKey(id));
    }

    TLookupRowsOptions lookupOptions;
    lookupOptions.ColumnFilter = columnFilter;
    lookupOptions.Timeout = timeout;
    lookupOptions.KeepMissingRows = true;
    auto resultOrError = WaitFor(client->LookupRows(
        GetOperationsArchiveOrderedByIdPath(),
        NRecords::TOrderedByIdDescriptor::Get()->GetNameTable(),
        FromRecordKeys(MakeSharedRange(std::move(keys))),
        lookupOptions));
    if (!resultOrError.IsOK()) {
        return TError(resultOrError);
    }
    return resultOrError.Value().Rowset;
}

////////////////////////////////////////////////////////////////////////////////

const int PoolNameMaxLength = 100;

TError CheckPoolName(const std::string& poolName, const re2::RE2& regex)
{
    if (poolName == RootPoolName) {
        return TError("Pool name cannot be equal to root pool name")
            << TErrorAttribute("root_pool_name", RootPoolName);
    }

    if (poolName.length() > PoolNameMaxLength) {
        return TError("Pool name %Qv is too long", poolName)
            << TErrorAttribute("length", poolName.length())
            << TErrorAttribute("max_length", PoolNameMaxLength);
    }

    if (!NRe2::TRe2::FullMatch(NRe2::StringPiece(poolName), regex)) {
        return TError("Pool name %Qv must match regular expression %Qv", poolName, regex.pattern());
    }

    return TError();
}

void ValidatePoolName(const std::string& poolName, const re2::RE2& regex)
{
    CheckPoolName(poolName, regex).ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TAccessControlRule GetAcrFromAllocationBriefInfo(const TAllocationBriefInfo& allocationBriefInfo)
{
    return allocationBriefInfo.OperationAcoName
        ? TAccessControlRule(*allocationBriefInfo.OperationAcoName)
        : TAccessControlRule(*allocationBriefInfo.OperationAcl);
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TAllocationBriefInfo* allocationBriefInfo,
    const NProto::TAllocationBriefInfo& allocationBriefInfoProto)
{
    allocationBriefInfo->AllocationId = FromProto<TAllocationId>(allocationBriefInfoProto.allocation_id());

    if (allocationBriefInfoProto.has_operation_id()) {
        allocationBriefInfo->OperationId = FromProto<TOperationId>(allocationBriefInfoProto.operation_id());
    }

    if (allocationBriefInfoProto.has_operation_acl()) {
        TYsonString aclYson(allocationBriefInfoProto.operation_acl());
        allocationBriefInfo->OperationAcl = ConvertTo<TSerializableAccessControlList>(aclYson);
    }
    if (allocationBriefInfoProto.has_operation_aco_name()) {
        allocationBriefInfo->OperationAcoName = allocationBriefInfoProto.operation_aco_name();
    }

    if (allocationBriefInfoProto.has_controller_agent_descriptor()) {
        FromProto(
            &allocationBriefInfo->ControllerAgentDescriptor,
            allocationBriefInfoProto.controller_agent_descriptor());
    }

    if (allocationBriefInfoProto.has_node_descriptor()) {
        FromProto(
            &allocationBriefInfo->NodeDescriptor,
            allocationBriefInfoProto.node_descriptor());
    }
}

void ToProto(
    NProto::TAllocationBriefInfo* allocationBriefInfoProto,
    const TAllocationBriefInfo& allocationBriefInfo)
{
    ToProto(
        allocationBriefInfoProto->mutable_allocation_id(),
        allocationBriefInfo.AllocationId);
    ToProto(
        allocationBriefInfoProto->mutable_operation_id(),
        allocationBriefInfo.OperationId);


    if (allocationBriefInfo.OperationAcl) {
        auto aclYson = ConvertToYsonString(*allocationBriefInfo.OperationAcl);
        allocationBriefInfoProto->set_operation_acl(ToProto(aclYson));
    }
    if (allocationBriefInfo.OperationAcoName) {
        allocationBriefInfoProto->set_operation_aco_name(*allocationBriefInfo.OperationAcoName);
    }

    if (allocationBriefInfo.ControllerAgentDescriptor) {
        ToProto(
            allocationBriefInfoProto->mutable_controller_agent_descriptor(),
            allocationBriefInfo.ControllerAgentDescriptor);
    }

    if (!allocationBriefInfo.NodeDescriptor.IsNull()) {
        ToProto(
            allocationBriefInfoProto->mutable_node_descriptor(),
            allocationBriefInfo.NodeDescriptor);
    }
}

void FromProto(
    TAllocationInfoToRequest* requestedAllocationInfo,
    const NProto::TReqGetAllocationBriefInfo::TRequestedInfo& requestedAllocationInfoProto)
{
    requestedAllocationInfo->OperationId = requestedAllocationInfoProto.operation_id();
    requestedAllocationInfo->OperationAcl = requestedAllocationInfoProto.operation_acl();
    requestedAllocationInfo->OperationAcoName = requestedAllocationInfoProto.operation_aco_name();
    requestedAllocationInfo->ControllerAgentDescriptor = requestedAllocationInfoProto.controller_agent_descriptor();
    requestedAllocationInfo->NodeDescriptor = requestedAllocationInfoProto.node_descriptor();
}

void ToProto(
    NProto::TReqGetAllocationBriefInfo::TRequestedInfo* allocationInfoToRequestProto,
    const TAllocationInfoToRequest& allocationInfoToRequest)
{
    if (allocationInfoToRequest.OperationId) {
        allocationInfoToRequestProto->set_operation_id(true);
    }

    if (allocationInfoToRequest.OperationAcl) {
        allocationInfoToRequestProto->set_operation_acl(true);
    }

    if (allocationInfoToRequest.OperationAcoName) {
        allocationInfoToRequestProto->set_operation_aco_name(true);
    }

    if (allocationInfoToRequest.ControllerAgentDescriptor) {
        allocationInfoToRequestProto->set_controller_agent_descriptor(true);
    }

    if (allocationInfoToRequest.NodeDescriptor) {
        allocationInfoToRequestProto->set_node_descriptor(true);
    }
}

void FromProto(
    TGracefulShutdownSpec* gracefulShutdownSpec,
    const NControllerAgent::NProto::TGracefulShutdownSpec& gracefulShutdownSpecProto)
{
    gracefulShutdownSpec->Signal = gracefulShutdownSpecProto.signal();

    if (gracefulShutdownSpecProto.has_timeout()) {
        gracefulShutdownSpec->Timeout = FromProto<TDuration>(gracefulShutdownSpecProto.timeout());
    }
}

void ToProto(
    NControllerAgent::NProto::TGracefulShutdownSpec* gracefulShutdownSpecProto,
    const TGracefulShutdownSpec& gracefulShutdownSpec)
{
    gracefulShutdownSpecProto->set_signal(gracefulShutdownSpec.Signal);

    YT_OPTIONAL_SET_PROTO(gracefulShutdownSpecProto, timeout, gracefulShutdownSpec.Timeout);
}

void FromProto(
    TSidecarJobSpec* sidecarJobSpec,
    const NControllerAgent::NProto::TSidecarJobSpec& sidecarJobSpecProto)
{
    sidecarJobSpec->Command = sidecarJobSpecProto.command();

    sidecarJobSpec->CpuLimit = YT_OPTIONAL_FROM_PROTO(sidecarJobSpecProto, cpu_limit);

    sidecarJobSpec->MemoryLimit = YT_OPTIONAL_FROM_PROTO(sidecarJobSpecProto, memory_limit);

    sidecarJobSpec->DockerImage = YT_OPTIONAL_FROM_PROTO(sidecarJobSpecProto, docker_image);

    sidecarJobSpec->RestartPolicy = ConvertTo<ESidecarRestartPolicy>(sidecarJobSpecProto.restart_policy());

    if (sidecarJobSpecProto.has_graceful_shutdown()) {
        sidecarJobSpec->GracefulShutdown = New<TGracefulShutdownSpec>();
        FromProto(&(*sidecarJobSpec->GracefulShutdown), sidecarJobSpecProto.graceful_shutdown());
    }
}

void ToProto(
    NControllerAgent::NProto::TSidecarJobSpec* sidecarJobSpecProto,
    const TSidecarJobSpec& sidecarJobSpec)
{
    sidecarJobSpecProto->set_command(sidecarJobSpec.Command);

    YT_OPTIONAL_SET_PROTO(sidecarJobSpecProto, cpu_limit, sidecarJobSpec.CpuLimit);

    YT_OPTIONAL_SET_PROTO(sidecarJobSpecProto, memory_limit, sidecarJobSpec.MemoryLimit);

    YT_OPTIONAL_TO_PROTO(sidecarJobSpecProto, docker_image, sidecarJobSpec.DockerImage);

    sidecarJobSpecProto->set_restart_policy(ToProto(sidecarJobSpec.RestartPolicy));

    if (sidecarJobSpec.GracefulShutdown) {
        ToProto(sidecarJobSpecProto->mutable_graceful_shutdown(), *sidecarJobSpec.GracefulShutdown);
    }
}

void ToProto(NControllerAgent::NProto::TVolume* volumeProto, const TVolume& volume)
{
    if (volume.DiskRequest) {
        if (auto nbdDiskRequest = volume.DiskRequest->TryGetConcrete<TNbdDiskRequest>()) {
            auto protoDiskRequest = volumeProto->mutable_nbd_disk_request();
            ToProto(protoDiskRequest, *nbdDiskRequest);
        } else if (auto localDiskRequest = volume.DiskRequest->TryGetConcrete<TLocalDiskRequest>()) {
            auto protoDiskRequest = volumeProto->mutable_local_disk_request();
            ToProto(protoDiskRequest, *localDiskRequest);
        } else if (auto tmpfsDiskRequest = volume.DiskRequest->TryGetConcrete<TTmpfsStorageRequest>()) {
            ToProto(volumeProto->mutable_tmpfs_storage_request(), *tmpfsDiskRequest);
        } else {
            YT_ABORT();
        }
    }
}

void FromProto(TVolume* volume, const NControllerAgent::NProto::TVolume& volumeProto)
{
    using TProtoMessage = NControllerAgent::NProto::TVolume::DiskRequestCase;
    switch (volumeProto.disk_request_case()) {
        case TProtoMessage::kLocalDiskRequest:
            volume->DiskRequest = TStorageRequestConfig(NExecNode::EVolumeType::Local);
            FromProto(
                &(*volume->DiskRequest->TryGetConcrete<TLocalDiskRequest>()),
                volumeProto.local_disk_request());
            break;
        case TProtoMessage::kNbdDiskRequest:
            volume->DiskRequest = TStorageRequestConfig(NExecNode::EVolumeType::Nbd);
            FromProto(
                &(*volume->DiskRequest->TryGetConcrete<TNbdDiskRequest>()),
                volumeProto.nbd_disk_request());
            break;
        case TProtoMessage::kTmpfsStorageRequest:
            volume->DiskRequest = TStorageRequestConfig(NExecNode::EVolumeType::Tmpfs);
            FromProto(
                &(*volume->DiskRequest->TryGetConcrete<TTmpfsStorageRequest>()),
                volumeProto.tmpfs_storage_request());
            break;
        case TProtoMessage::DISK_REQUEST_NOT_SET:
            YT_ABORT();
    }
}

void FromProto(
    TVolumeMount* volumeMount,
    const NControllerAgent::NProto::TVolumeMount& volumeMountProto)
{
    volumeMount->VolumeId = volumeMountProto.volume_id();
    volumeMount->MountPath = volumeMountProto.mount_path();
    volumeMount->ReadOnly = volumeMountProto.read_only();
}

void ToProto(
    NControllerAgent::NProto::TVolumeMount* volumeMountProto,
    const TVolumeMount& volumeMount)
{
    volumeMountProto->set_volume_id(volumeMount.VolumeId);
    volumeMountProto->set_mount_path(volumeMount.MountPath);
    volumeMountProto->set_read_only(volumeMount.ReadOnly);
}

void FromProto(
    TTmpfsVolumeConfig* tmpfsVolumeConfig,
    const NControllerAgent::NProto::TTmpfsVolume& protoTmpfsVolume)
{
    tmpfsVolumeConfig->Size = protoTmpfsVolume.size();
    tmpfsVolumeConfig->Path = protoTmpfsVolume.path();
}

void ToProto(NControllerAgent::NProto::TTmpfsVolume* protoTmpfsVolume, const TTmpfsVolumeConfig& tmpfsVolumeConfig)
{
    protoTmpfsVolume->set_size(tmpfsVolumeConfig.Size);
    protoTmpfsVolume->set_path(tmpfsVolumeConfig.Path);
}

void FromProto(TStorageRequestConfig* diskRequestConfig, const NProto::TDeprecatedDiskRequest& protoDiskRequestConfig)
{
    switch (static_cast<NExecNode::EVolumeType>(protoDiskRequestConfig.type())) {
        case NExecNode::EVolumeType::Nbd:
            *diskRequestConfig = TStorageRequestConfig(NExecNode::EVolumeType::Nbd);
            FromProto(&(*diskRequestConfig->TryGetConcrete<TNbdDiskRequest>()), protoDiskRequestConfig);
            break;
        case NExecNode::EVolumeType::Local:
            *diskRequestConfig = TStorageRequestConfig(NExecNode::EVolumeType::Local);
            FromProto(&(*diskRequestConfig->TryGetConcrete<TLocalDiskRequest>()), protoDiskRequestConfig);
            break;
        case NExecNode::EVolumeType::Tmpfs:
            break;
    }
}

void ToProto(NProto::TDeprecatedDiskRequest* protoDiskRequest, const TStorageRequestConfig& diskRequestConfig)
{
    if (auto nbdDiskRequest = diskRequestConfig.TryGetConcrete<TNbdDiskRequest>()) {
        protoDiskRequest->set_type(static_cast<int>(NExecNode::EVolumeType::Nbd));
        ToProto(protoDiskRequest, *nbdDiskRequest);
    } else if (auto localDiskRequest = diskRequestConfig.TryGetConcrete<TLocalDiskRequest>()) {
        protoDiskRequest->set_type(static_cast<int>(NExecNode::EVolumeType::Local));
        ToProto(protoDiskRequest, *localDiskRequest);
    } else if (auto tmpfsDiskRequest = diskRequestConfig.TryGetConcrete<TTmpfsStorageRequest>()) {
        protoDiskRequest->set_type(static_cast<int>(NExecNode::EVolumeType::Tmpfs));
        ToProto(protoDiskRequest, *tmpfsDiskRequest);
    } else {
        YT_ABORT();
    }

}

void FromProto(TNbdDiskConfig* nbdDiskConfig, const NProto::TNbdDisk& protoNbdDisk)
{
    if (protoNbdDisk.has_data_node_address()) {
        nbdDiskConfig->DataNodeAddress = protoNbdDisk.data_node_address();
    }

    nbdDiskConfig->DataNodeRpcTimeout = FromProto<TDuration>(protoNbdDisk.data_node_rpc_timeout());
    nbdDiskConfig->MasterRpcTimeout = FromProto<TDuration>(protoNbdDisk.master_rpc_timeout());
    nbdDiskConfig->DataNodeNbdServiceRpcTimeout = FromProto<TDuration>(protoNbdDisk.data_node_nbd_service_rpc_timeout());
    nbdDiskConfig->DataNodeNbdServiceMakeTimeout = FromProto<TDuration>(protoNbdDisk.data_node_nbd_service_make_timeout());
    nbdDiskConfig->MinDataNodeCount = protoNbdDisk.min_data_node_count();
    nbdDiskConfig->MaxDataNodeCount = protoNbdDisk.max_data_node_count();
}

void ToProto(NProto::TNbdDisk* protoNbdDisk, const TNbdDiskConfig& nbdDiskConfig)
{
    if (nbdDiskConfig.DataNodeAddress) {
        protoNbdDisk->set_data_node_address(*nbdDiskConfig.DataNodeAddress);
    }
    protoNbdDisk->set_data_node_rpc_timeout(ToProto(nbdDiskConfig.DataNodeRpcTimeout));
    protoNbdDisk->set_master_rpc_timeout(ToProto(nbdDiskConfig.MasterRpcTimeout));
    protoNbdDisk->set_min_data_node_count(nbdDiskConfig.MinDataNodeCount);
    protoNbdDisk->set_max_data_node_count(nbdDiskConfig.MaxDataNodeCount);
    protoNbdDisk->set_data_node_nbd_service_rpc_timeout(ToProto(nbdDiskConfig.DataNodeNbdServiceRpcTimeout));
    protoNbdDisk->set_data_node_nbd_service_make_timeout(ToProto(nbdDiskConfig.DataNodeNbdServiceMakeTimeout));
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TTmpfsStorageRequest* diskRequestConfig, const NProto::TTmpfsStorageRequest& protoDiskRequestConfig)
{
    FromProto(static_cast<TStorageRequestBase*>(diskRequestConfig), protoDiskRequestConfig.storage_request_common_parameters());

    // COMPAT(krasovav): remove after YT-26820.
    YT_VERIFY(protoDiskRequestConfig.has_tmpfs_index());
    diskRequestConfig->TmpfsIndex = protoDiskRequestConfig.tmpfs_index();
}

void ToProto(NProto::TTmpfsStorageRequest* protoDiskRequestConfig, const TTmpfsStorageRequest& diskRequestConfig)
{
    ToProto(protoDiskRequestConfig->mutable_storage_request_common_parameters(), static_cast<const TStorageRequestBase&>(diskRequestConfig));

    // COMPAT(krasovav): remove after YT-26820.
    YT_VERIFY(diskRequestConfig.TmpfsIndex);
    protoDiskRequestConfig->set_tmpfs_index(*diskRequestConfig.TmpfsIndex);
}

////////////////////////////////////////////////////////////////////////////////

//! Check that no volume path is a prefix of another volume path. Throw if check has failed.
void ValidateTmpfsPaths(const std::vector<std::string_view>& tmpfsPaths)
{
    for (int i = 0; i < std::ssize(tmpfsPaths); ++i) {
        for (int j = 0; j < std::ssize(tmpfsPaths); ++j) {
            if (i == j) {
                continue;
            }

            auto lhsFsPath = TFsPath(tmpfsPaths[i]);
            auto rhsFsPath = TFsPath(tmpfsPaths[j]);

            if (lhsFsPath.IsSubpathOf(rhsFsPath)) {
                THROW_ERROR_EXCEPTION("Path of tmpfs volume %Qv is a prefix of another tmpfs volume %Qv",
                    tmpfsPaths[i],
                    tmpfsPaths[j]);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

int CountNonTmpfsVolumes(const THashMap<std::string, TVolumePtr>& volumes)
{
    int count = 0;

    for (const auto& [_, volume] : volumes) {
        if (volume->DiskRequest && volume->DiskRequest->GetCurrentType() != NExecNode::EVolumeType::Tmpfs) {
            ++count;
        }
    }

    return count;
}

////////////////////////////////////////////////////////////////////////////////

bool IsDiskRequestTmpfs(const std::optional<TStorageRequestConfig>& diskRequest)
{
    return diskRequest && diskRequest->GetCurrentType() == NExecNode::EVolumeType::Tmpfs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
