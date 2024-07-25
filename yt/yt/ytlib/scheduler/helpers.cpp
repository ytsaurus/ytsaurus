#include "helpers.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/client/api/operations_archive_schema.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/library/re2/re2.h>

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

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void ApplyJobShellOptionsUpdate(TJobShellOptionsMap* origin, const TJobShellOptionsUpdeteMap& update)
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

const std::vector<TString>& TJobShellInfo::GetOwners()
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

std::optional<TString> FindControllerAgentAddressFromCypress(
    TOperationId operationId,
    const NApi::NNative::IClientPtr& client)
{
    using NYT::ToProto;

    static const std::vector<TString> attributes = {"controller_agent_address"};

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

void ValidateOperationAccess(
    const std::optional<TString>& user,
    TOperationId operationId,
    TAllocationId allocationId,
    EPermissionSet permissionSet,
    const NSecurityClient::TSerializableAccessControlList& acl,
    const IClientPtr& client,
    const TLogger& logger)
{
    const auto& Logger = logger;

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
            "Operation has missing subjects in ACL (OperationId: %v, AllocationId: %v, MissingSubjects: %v)",
            operationId ? ToString(operationId) : "<unknown>",
            allocationId ? ToString(allocationId) : "<unknown>",
            results.front().MissingSubjects);
    }

    for (const auto& result : results) {
        if (result.Action != ESecurityAction::Allow) {
            auto userStr = user.value_or(GetCurrentAuthenticationIdentity().User);
            auto error = TError(
                NSecurityClient::EErrorCode::AuthorizationError,
                "Operation access denied, user %Qv does not have %Qlv permissions",
                userStr,
                FormatPermissions(permissionSet))
                << TErrorAttribute("user", userStr)
                << TErrorAttribute("required_permissions", permissionSet)
                << TErrorAttribute("acl", acl);
            if (operationId) {
                error = error << TErrorAttribute("operation_id", operationId);
            }
            if (allocationId) {
                error = error << TErrorAttribute("allocation_id", allocationId);
            }
            THROW_ERROR error;
        }
    }

    YT_LOG_DEBUG(
        "Operation access successfully validated (OperationId: %v, AllocationId: %v, User: %v, Permissions: %v, Acl: %v)",
        operationId ? ToString(operationId) : "<unknown>",
        allocationId ? ToString(allocationId) : "<unknown>",
        user,
        permissionSet,
        ConvertToYsonString(acl, EYsonFormat::Text));
}

////////////////////////////////////////////////////////////////////////////////

static TUnversionedRow CreateOperationKey(
    const TOperationId& operationId,
    const TOrderedByIdTableDescriptor::TIndex& index,
    const TRowBufferPtr& rowBuffer)
{
    auto operationIdAsGuid = operationId.Underlying();
    auto key = rowBuffer->AllocateUnversioned(2);
    key[0] = MakeUnversionedUint64Value(operationIdAsGuid.Parts64[0], index.IdHi);
    key[1] = MakeUnversionedUint64Value(operationIdAsGuid.Parts64[1], index.IdLo);
    return key;
}

TErrorOr<IUnversionedRowsetPtr> LookupOperationsInArchive(
    const NNative::IClientPtr& client,
    const std::vector<TOperationId>& ids,
    const NTableClient::TColumnFilter& columnFilter,
    std::optional<TDuration> timeout)
{
    static const TOrderedByIdTableDescriptor tableDescriptor;
    auto rowBuffer = New<TRowBuffer>();
    std::vector<TUnversionedRow> keys;
    keys.reserve(ids.size());
    for (const auto& id : ids) {
        keys.push_back(CreateOperationKey(id, tableDescriptor.Index, rowBuffer));
    }

    TLookupRowsOptions lookupOptions;
    lookupOptions.ColumnFilter = columnFilter;
    lookupOptions.Timeout = timeout;
    lookupOptions.KeepMissingRows = true;
    auto resultOrError = WaitFor(client->LookupRows(
        GetOperationsArchiveOrderedByIdPath(),
        tableDescriptor.NameTable,
        MakeSharedRange(std::move(keys), std::move(rowBuffer)),
        lookupOptions));
    if (!resultOrError.IsOK()) {
        return TError(resultOrError);
    }
    return resultOrError.Value().Rowset;
}

////////////////////////////////////////////////////////////////////////////////

const int PoolNameMaxLength = 100;

TError CheckPoolName(const TString& poolName, const re2::RE2& regex)
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

void ValidatePoolName(const TString& poolName, const re2::RE2& regex)
{
    CheckPoolName(poolName, regex).ThrowOnError();
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
        allocationBriefInfoProto->set_operation_acl(aclYson.ToString());
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

    if (allocationInfoToRequest.ControllerAgentDescriptor) {
        allocationInfoToRequestProto->set_controller_agent_descriptor(true);
    }

    if (allocationInfoToRequest.NodeDescriptor) {
        allocationInfoToRequestProto->set_node_descriptor(true);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
