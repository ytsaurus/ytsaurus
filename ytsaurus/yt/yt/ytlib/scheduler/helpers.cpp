#include "helpers.h"

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/client/api/operation_archive_schema.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/library/re2/re2.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/fluent.h>

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
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NSecurityClient;
using namespace NLogging;
using namespace NRpc;
using namespace NJobTrackerClient::NProto;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void Serialize(const TCoreInfo& coreInfo, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
        .Item("process_id").Value(coreInfo.process_id())
        .Item("executable_name").Value(coreInfo.executable_name())
        .DoIf(coreInfo.has_size(), [&] (TFluentMap fluent) {
            fluent
                .Item("size").Value(coreInfo.size());
        })
        .DoIf(coreInfo.has_error(), [&] (TFluentMap fluent) {
            fluent
                .Item("error").Value(NYT::FromProto<TError>(coreInfo.error()));
        })
        .DoIf(coreInfo.has_thread_id(), [&] (TFluentMap fluent) {
            fluent
                .Item("thread_id").Value(coreInfo.thread_id());
        })
        .DoIf(coreInfo.has_signal(), [&] (TFluentMap fluent) {
            fluent
                .Item("signal").Value(coreInfo.signal());
        })
        .DoIf(coreInfo.has_container(), [&] (TFluentMap fluent) {
            fluent
                .Item("container").Value(coreInfo.container());
        })
        .DoIf(coreInfo.has_datetime(), [&] (TFluentMap fluent) {
            fluent
                .Item("datetime").Value(coreInfo.datetime());
        })
        .Item("cuda").Value(coreInfo.cuda())
        .EndMap();
}

} // namespace NProto

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

const TYPath& GetOperationsArchiveOrderedByIdPath()
{
    static const TYPath path = "//sys/operations_archive/ordered_by_id";
    return path;
}

const TYPath& GetOperationsArchiveOrderedByStartTimePath()
{
    static const TYPath path = "//sys/operations_archive/ordered_by_start_time";
    return path;
}

const TYPath& GetOperationsArchiveOperationAliasesPath()
{
    static const TYPath path = "//sys/operations_archive/operation_aliases";
    return path;
}

const TYPath& GetOperationsArchiveVersionPath()
{
    static const TYPath path = "//sys/operations_archive/@version";
    return path;
}

const TYPath& GetOperationsArchiveJobsPath()
{
    static const TYPath path = "//sys/operations_archive/jobs";
    return path;
}

const TYPath& GetOperationsArchiveJobSpecsPath()
{
    static const TYPath path = "//sys/operations_archive/job_specs";
    return path;
}

const TYPath& GetOperationsArchiveJobStderrsPath()
{
    static const TYPath path = "//sys/operations_archive/stderrs";
    return path;
}

const TYPath& GetOperationsArchiveJobProfilesPath()
{
    static const TYPath path = "//sys/operations_archive/job_profiles";
    return path;
}

const TYPath& GetOperationsArchiveJobFailContextsPath()
{
    static const TYPath path = "//sys/operations_archive/fail_contexts";
    return path;
}

const NYPath::TYPath& GetOperationsArchiveOperationIdsPath()
{
    static const TYPath path = "//sys/operations_archive/operation_ids";
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

bool IsJobAbsenceGuaranteed(EAbortReason reason)
{
    return IsSchedulingReason(reason) || reason == EAbortReason::GetSpecFailed;
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
        auto proxy = CreateObjectServiceWriteProxy(client);
        auto batchReq = proxy.ExecuteBatch();

        const auto nestingLevelLimit = client->GetNativeConnection()->GetConfig()->CypressWriteYsonNestingLevelLimit;
        for (const auto& file : files) {
            auto req = TCypressYPathProxy::Create(file.Path);
            req->set_recursive(true);
            req->set_force(true);
            req->set_type(static_cast<int>(EObjectType::File));

            auto attributes = CreateEphemeralAttributes(nestingLevelLimit);
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
        auto proxy = CreateObjectServiceWriteProxy(client, nativeCellTag);
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
        auto proxy = CreateObjectServiceWriteProxy(client, externalCellTag);
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
        SetSuppressUpstreamSync(&batchReq->Header(), true);
        // COMPAT(shakurov): prefer proto ext (above).
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
        auto proxy = CreateObjectServiceWriteProxy(client, nativeCellTag);
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

    auto results = WaitFor(AllSucceeded(futures))
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
                << TErrorAttribute("user", user.value_or(GetCurrentAuthenticationIdentity().User))
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

static TUnversionedRow CreateOperationKey(
    const TOperationId& operationId,
    const TOrderedByIdTableDescriptor::TIndex& index,
    const TRowBufferPtr& rowBuffer)
{
    auto key = rowBuffer->AllocateUnversioned(2);
    key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], index.IdHi);
    key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], index.IdLo);
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
    return WaitFor(client->LookupRows(
        GetOperationsArchiveOrderedByIdPath(),
        tableDescriptor.NameTable,
        MakeSharedRange(std::move(keys), std::move(rowBuffer)),
        lookupOptions));
}

////////////////////////////////////////////////////////////////////////////////

const int PoolNameMaxLength = 100;

TString StrictPoolNameRegexSymbols = "-_a-z0-9";
TString NonStrictPoolNameRegexSymbols = StrictPoolNameRegexSymbols + ":A-Z";

TEnumIndexedVector<EPoolNameValidationLevel, TString> RegexStrings = {
    "[" + NonStrictPoolNameRegexSymbols + "]+",
    "[" + StrictPoolNameRegexSymbols + "]+",
};

TEnumIndexedVector<EPoolNameValidationLevel, TIntrusivePtr<NRe2::TRe2>> Regexes = {
    New<NRe2::TRe2>(RegexStrings[EPoolNameValidationLevel::NonStrict]),
    New<NRe2::TRe2>(RegexStrings[EPoolNameValidationLevel::Strict]),
};

TError CheckPoolName(const TString& poolName, EPoolNameValidationLevel validationLevel)
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

    const auto& regex = Regexes[validationLevel];

    if (!NRe2::TRe2::FullMatch(NRe2::StringPiece(poolName), *regex)) {
        const auto& regexString = RegexStrings[validationLevel];
        return TError("Pool name %Qv must match regular expression %Qv", poolName, regexString);
    }

    return TError();
}

void ValidatePoolName(const TString& poolName, EPoolNameValidationLevel validationLevel)
{
    CheckPoolName(poolName, validationLevel).ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

