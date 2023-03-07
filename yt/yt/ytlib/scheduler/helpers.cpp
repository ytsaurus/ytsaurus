#include "helpers.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/client/security_client/acl.h>

#include <yt/client/api/transaction.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/core/misc/error.h>

#include <yt/core/ypath/token.h>

#include <yt/core/ytree/fluent.h>

#include <util/string/ascii.h>

namespace NYT::NScheduler {

using namespace NApi;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NFileClient;
using namespace NTransactionClient;
using namespace NSecurityClient;
using namespace NLogging;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TYPath GetOperationsPath()
{
    return "//sys/operations";
}

TYPath GetOperationPath(TOperationId operationId)
{
    int hashByte = operationId.Parts32[0] & 0xff;
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

std::optional<TString> GetControllerAgentAddressFromCypress(
    TOperationId operationId,
    const IChannelPtr& channel)
{
    using NYT::ToProto;

    static const std::vector<TString> attributes = {"controller_agent_address"};

    TObjectServiceProxy proxy(channel);

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

const TYPath& GetOperationsArchiveOrderedByIdPath()
{
    static TYPath path = "//sys/operations_archive/ordered_by_id";
    return path;
}

const TYPath& GetOperationsArchiveOrderedByStartTimePath()
{
    static TYPath path = "//sys/operations_archive/ordered_by_start_time";
    return path;
}

const TYPath& GetOperationsArchiveOperationAliasesPath()
{
    static TYPath path = "//sys/operations_archive/operation_aliases";
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
    static TYPath path = "//sys/operations_archive/job_specs";
    return path;
}

const TYPath& GetOperationsArchiveJobStderrsPath()
{
    static TYPath path = "//sys/operations_archive/stderrs";
    return path;
}

const TYPath& GetOperationsArchiveJobProfilesPath()
{
    static TYPath path = "//sys/operations_archive/job_profiles";
    return path;
}

const TYPath& GetOperationsArchiveJobFailContextsPath()
{
    static TYPath path = "//sys/operations_archive/fail_contexts";
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

bool IsOperationWithUserJobs(EOperationType operationType)
{
    return
        operationType == EOperationType::Map ||
        operationType == EOperationType::Reduce ||
        operationType == EOperationType::MapReduce ||
        operationType == EOperationType::JoinReduce ||
        operationType == EOperationType::Vanilla;
}

void ValidateEnvironmentVariableName(TStringBuf name)
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

void SaveJobFiles(
    const NNative::IClientPtr& client,
    TOperationId operationId,
    const std::vector<TJobFile>& files)
{
    using NYT::FromProto;
    using NYT::ToProto;

    if (files.empty()) {
        return;
    }

    struct TJobFileInfo
    {
        TTransactionId UploadTransactionId;
        TNodeId NodeId;
        TCellTag ExternalCellTag = InvalidCellTag;
        TChunkListId ChunkListId;
        NChunkClient::NProto::TDataStatistics Statistics;
    };
    THashMap<const TJobFile*, TJobFileInfo> fileToInfo;

    auto connection = client->GetNativeConnection();

    NApi::ITransactionPtr transaction;
    {
        NApi::TTransactionStartOptions options;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Saving job files of operation %v", operationId));
        options.Attributes = std::move(attributes);

        transaction = WaitFor(client->StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();
    }

    auto transactionId = transaction->GetId();

    {
        TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Leader));
        auto batchReq = proxy.ExecuteBatch();

        for (const auto& file : files) {
            auto req = TCypressYPathProxy::Create(file.Path);
            req->set_recursive(true);
            req->set_force(true);
            req->set_type(static_cast<int>(EObjectType::File));

            auto attributes = CreateEphemeralAttributes();
            attributes->Set("external", true);
            attributes->Set("external_cell_tag", CellTagFromId(file.ChunkId));
            attributes->Set("vital", false);
            attributes->Set("replication_factor", 1);
            attributes->Set(
                "description", BuildYsonStringFluently()
                    .BeginMap()
                        .Item("type").Value(file.DescriptionType)
                        .Item("job_id").Value(file.JobId)
                    .EndMap());
            ToProto(req->mutable_node_attributes(), *attributes);

            SetTransactionId(req, transactionId);
            GenerateMutationId(req);
            req->Tag() = &file;
            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
        const auto& batchRsp = batchRspOrError.Value();

        for (const auto& rspOrError : batchRsp->GetResponses<TCypressYPathProxy::TRspCreate>()) {
            const auto& rsp = rspOrError.Value();
            const auto* file = std::any_cast<const TJobFile*>(rsp->Tag());
            auto& info = fileToInfo[file];
            info.NodeId = FromProto<TNodeId>(rsp->node_id());
            info.ExternalCellTag = CellTagFromId(file->ChunkId);
        }
    }

    THashMap<TCellTag, std::vector<const TJobFile*>> nativeCellTagToFiles;
    for (const auto& file : files) {
        const auto& info = fileToInfo[&file];
        nativeCellTagToFiles[CellTagFromId(info.NodeId)].push_back(&file);
    }

    THashMap<TCellTag, std::vector<const TJobFile*>> externalCellTagToFiles;
    for (const auto& file : files) {
        externalCellTagToFiles[CellTagFromId(file.ChunkId)].push_back(&file);
    }

    for (const auto& [nativeCellTag, files] : nativeCellTagToFiles) {
        TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Leader, nativeCellTag));
        auto batchReq = proxy.ExecuteBatch();

        for (const auto* file : files) {
            const auto& info = fileToInfo[file];
            auto req = TFileYPathProxy::BeginUpload(FromObjectId(info.NodeId));
            req->set_update_mode(static_cast<int>(EUpdateMode::Overwrite));
            req->set_lock_mode(static_cast<int>(ELockMode::Exclusive));
            req->set_upload_transaction_title(Format("Saving files of job %v of operation %v",
                file->JobId,
                operationId));
            GenerateMutationId(req);
            SetTransactionId(req, transactionId);
            req->Tag() = file;
            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
        const auto& batchRsp = batchRspOrError.Value();

        for (const auto& rspOrError : batchRsp->GetResponses<TFileYPathProxy::TRspBeginUpload>()) {
            const auto& rsp = rspOrError.Value();
            const auto* file = std::any_cast<const TJobFile*>(rsp->Tag());
            auto& info = fileToInfo[file];
            info.UploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
        }
    }

    for (const auto& [externalCellTag, files] : externalCellTagToFiles) {
        TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Leader, externalCellTag));
        auto batchReq = proxy.ExecuteBatch();

        for (const auto* file : files) {
            const auto& info = fileToInfo[file];
            auto req = TFileYPathProxy::GetUploadParams(FromObjectId(info.NodeId));
            req->Tag() = file;
            SetTransactionId(req, info.UploadTransactionId);
            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
        const auto& batchRsp = batchRspOrError.Value();

        for (const auto& rspOrError : batchRsp->GetResponses<TFileYPathProxy::TRspGetUploadParams>()) {
            const auto& rsp = rspOrError.Value();
            const auto* file = std::any_cast<const TJobFile*>(rsp->Tag());
            auto& info = fileToInfo[file];
            info.ChunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
        }
    }

    for (const auto& [externalCellTag, files] : externalCellTagToFiles) {
        TChunkServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Leader, externalCellTag));
        auto batchReq = proxy.ExecuteBatch();
        batchReq->set_suppress_upstream_sync(true);
        GenerateMutationId(batchReq);

        for (const auto* file : files) {
            const auto& info = fileToInfo[file];
            auto* req = batchReq->add_attach_chunk_trees_subrequests();
            ToProto(req->mutable_parent_id(), info.ChunkListId);
            ToProto(req->add_child_ids(), file->ChunkId);
            req->set_request_statistics(true);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
        const auto& batchRsp = batchRspOrError.Value();

        for (int index = 0; index < batchRsp->attach_chunk_trees_subresponses_size(); ++index) {
            const auto& rsp = batchRsp->attach_chunk_trees_subresponses(index);
            const auto* file = files[index];
            auto& info = fileToInfo[file];
            info.Statistics = rsp.statistics();
        }
    }

    for (const auto& [nativeCellTag, files] : nativeCellTagToFiles) {
        TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Leader, nativeCellTag));
        auto batchReq = proxy.ExecuteBatch();

        for (const auto* file : files) {
            const auto& info = fileToInfo[file];
            auto req = TFileYPathProxy::EndUpload(FromObjectId(info.NodeId));
            *req->mutable_statistics() = info.Statistics;
            SetTransactionId(req, info.UploadTransactionId);
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
    }

    WaitFor(transaction->Commit())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void ValidateOperationAccess(
    const std::optional<TString>& user,
    TOperationId operationId,
    TJobId jobId,
    EPermissionSet permissionSet,
    const NSecurityClient::TSerializableAccessControlList& acl,
    const NNative::IClientPtr& client,
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

    auto results = WaitFor(Combine(futures))
        .ValueOrThrow();

    if (!results.empty() && !results.front().MissingSubjects.empty()) {
        YT_LOG_DEBUG("Operation has missing subjects in ACL (OperationId: %v, JobId: %v, MissingSubjects: %v)",
            operationId ? ToString(operationId) : "<unknown>",
            jobId ? ToString(jobId) : "<unknown>",
            results.front().MissingSubjects);
    }

    for (const auto& result : results) {
        if (result.Action != ESecurityAction::Allow) {
            auto error = TError(
                NSecurityClient::EErrorCode::AuthorizationError,
                "Operation access denied")
                << TErrorAttribute("user", user)
                << TErrorAttribute("required_permissions", permissionSet)
                << TErrorAttribute("acl", acl);
            if (operationId) {
                error = error << TErrorAttribute("operation_id", operationId);
            }
            if (jobId) {
                error = error << TErrorAttribute("job_id", jobId);
            }
            THROW_ERROR error;
        }
    }

    YT_LOG_DEBUG("Operation access successfully validated (OperationId: %v, JobId: %v, User: %v, Permissions: %v, Acl: %v)",
        operationId ? ToString(operationId) : "<unknown>",
        jobId ? ToString(jobId) : "<unknown>",
        user,
        permissionSet,
        ConvertToYsonString(acl, EYsonFormat::Text));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

