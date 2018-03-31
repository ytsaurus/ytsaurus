#include "helpers.h"

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/object_client/helpers.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/chunk_client/data_statistics.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/core/misc/error.h>
#include <yt/core/ypath/token.h>

#include <yt/core/ytree/fluent.h>

#include <util/string/ascii.h>

namespace NYT {
namespace NScheduler {

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
    const TString& resourceName)
{
    TString suffix;
    if (!resourceName.empty()) {
        suffix = "/" + resourceName;
    }

    // COMPAT(babenko)
    return {
        GetJobPath(operationId, jobId) + suffix,
        GetNewJobPath(operationId, jobId) + suffix
    };
}

std::vector<NYPath::TYPath> GetCompatibilityOperationPaths(
    const TOperationId& operationId,
    const TString& resourceName)
{
    TString suffix;
    if (!resourceName.empty()) {
        suffix = "/" + resourceName;
    }

    // COMPAT(babenko)
    return {
        GetOperationPath(operationId) + suffix,
        GetNewOperationPath(operationId) + suffix
    };
}

const TYPath& GetPoolTreesPath()
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
        state == EOperationState::Starting ||
        state == EOperationState::WaitingForAgent ||
        state == EOperationState::Orphaned ||
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

void SaveJobFiles(INativeClientPtr client, const TOperationId& operationId, const std::vector<TJobFile>& files)
{
    if (files.empty()) {
        return;
    }

    auto connection = client->GetNativeConnection();

    ITransactionPtr transaction;
    {
        NApi::TTransactionStartOptions options;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Saving job files of operation %v", operationId));
        options.Attributes = std::move(attributes);

        transaction = WaitFor(client->StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();
    }

    const auto& transactionId = transaction->GetId();

    THashMap<TCellTag, std::vector<TJobFile>> cellTagToFiles;
    for (const auto& file : files) {
        cellTagToFiles[CellTagFromId(file.ChunkId)].push_back(file);
    }

    for (const auto& pair : cellTagToFiles) {
        auto cellTag = pair.first;
        const auto& perCellFiles = pair.second;

        struct TJobFileInfo
        {
            TTransactionId UploadTransactionId;
            TNodeId NodeId;
            TChunkListId ChunkListId;
            NChunkClient::NProto::TDataStatistics Statistics;
        };

        std::vector<TJobFileInfo> infos;

        {
            TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Leader, PrimaryMasterCellTag));
            auto batchReq =  proxy.ExecuteBatch();

            for (const auto& file : perCellFiles) {
                {
                    auto req = TCypressYPathProxy::Create(file.Path);
                    req->set_recursive(true);
                    req->set_force(true);
                    req->set_type(static_cast<int>(EObjectType::File));

                    auto attributes = CreateEphemeralAttributes();
                    if (cellTag == connection->GetPrimaryMasterCellTag()) {
                        attributes->Set("external", false);
                    } else {
                        attributes->Set("external_cell_tag", cellTag);
                    }
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
                    batchReq->AddRequest(req, "create");
                }
                {
                    auto req = TFileYPathProxy::BeginUpload(file.Path);
                    req->set_update_mode(static_cast<int>(EUpdateMode::Overwrite));
                    req->set_lock_mode(static_cast<int>(ELockMode::Exclusive));
                    req->set_upload_transaction_title(Format("Saving files of job %v of operation %v",
                        file.JobId,
                        operationId));
                    GenerateMutationId(req);
                    SetTransactionId(req, transactionId);
                    batchReq->AddRequest(req, "begin_upload");
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
            const auto& batchRsp = batchRspOrError.Value();

            auto createRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspCreate>("create");
            auto beginUploadRsps = batchRsp->GetResponses<TFileYPathProxy::TRspBeginUpload>("begin_upload");
            for (int index = 0; index < perCellFiles.size(); ++index) {
                infos.push_back(TJobFileInfo());
                auto& info = infos.back();

                {
                    const auto& rsp = createRsps[index].Value();
                    info.NodeId = FromProto<TNodeId>(rsp->node_id());
                }
                {
                    const auto& rsp = beginUploadRsps[index].Value();
                    info.UploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
                }
            }
        }

        {
            TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Follower, cellTag));
            auto batchReq =  proxy.ExecuteBatch();

            for (const auto& info : infos) {
                auto req = TFileYPathProxy::GetUploadParams(FromObjectId(info.NodeId));
                SetTransactionId(req, info.UploadTransactionId);
                batchReq->AddRequest(req, "get_upload_params");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
            const auto& batchRsp = batchRspOrError.Value();

            auto getUploadParamsRsps = batchRsp->GetResponses<TFileYPathProxy::TRspGetUploadParams>("get_upload_params");
            for (int index = 0; index < getUploadParamsRsps.size(); ++index) {
                const auto& rsp = getUploadParamsRsps[index].Value();
                auto& info = infos[index];
                info.ChunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
            }
        }

        {
            TChunkServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag));
            auto batchReq = proxy.ExecuteBatch();

            GenerateMutationId(batchReq);
            batchReq->set_suppress_upstream_sync(true);

            for (int index = 0; index < perCellFiles.size(); ++index) {
                const auto& file = perCellFiles[index];
                const auto& info = infos[index];
                auto* req = batchReq->add_attach_chunk_trees_subrequests();
                ToProto(req->mutable_parent_id(), info.ChunkListId);
                ToProto(req->add_child_ids(), file.ChunkId);
                req->set_request_statistics(true);
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
            const auto& batchRsp = batchRspOrError.Value();

            for (int index = 0; index < perCellFiles.size(); ++index) {
                auto& info = infos[index];
                const auto& rsp = batchRsp->attach_chunk_trees_subresponses(index);
                info.Statistics = rsp.statistics();
            }
        }

        {
            TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Leader, PrimaryMasterCellTag));
            auto batchReq =  proxy.ExecuteBatch();

            for (int index = 0; index < perCellFiles.size(); ++index) {
                const auto& info = infos[index];
                auto req = TFileYPathProxy::EndUpload(FromObjectId(info.NodeId));
                *req->mutable_statistics() = info.Statistics;
                SetTransactionId(req, info.UploadTransactionId);
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
        }
    }

    WaitFor(transaction->Commit())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

