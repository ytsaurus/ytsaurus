#include "helpers.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/controller_agent_descriptor.pb.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NControllerAgent {

using namespace NApi;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NFileClient;

using NYT::FromProto;
using NYT::ToProto;

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
                .Item("error").Value(FromProto<TError>(coreInfo.error()));
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

void SaveJobFiles(
    const NNative::IClientPtr& client,
    TOperationId operationId,
    const std::vector<TJobFile>& files,
    TTransactionId transactionId)
{
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
}

////////////////////////////////////////////////////////////////////////////////

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

bool WasAbortedAfterStart(EAbortReason reason)
{
    return NScheduler::IsSchedulingReason(reason) || reason == EAbortReason::GetSpecFailed;
}

int GetJobSpecVersion()
{
    return 2;
}

bool IsFinishedState(EControllerState state)
{
    return state == NControllerAgent::EControllerState::Completed ||
        state == NControllerAgent::EControllerState::Failed ||
        state == NControllerAgent::EControllerState::Aborted;
}

TYsonString BuildBriefStatistics(const INodePtr& statistics)
{
    if (statistics->GetType() != ENodeType::Map) {
        return BuildYsonStringFluently()
            .BeginMap()
            .EndMap();
    }

    // See NControllerAgent::BuildBriefStatistics(std::unique_ptr<TJobSummary> jobSummary).
    auto rowCount = FindNodeByYPath(statistics, "/data/input/row_count/sum");
    auto uncompressedDataSize = FindNodeByYPath(statistics, "/data/input/uncompressed_data_size/sum");
    auto compressedDataSize = FindNodeByYPath(statistics, "/data/input/compressed_data_size/sum");
    auto dataWeight = FindNodeByYPath(statistics, "/data/input/data_weight/sum");
    auto inputPipeIdleTime = FindNodeByYPath(statistics, "/user_job/pipes/input/idle_time/sum");
    auto jobProxyCpuUsage = FindNodeByYPath(statistics, "/job_proxy/cpu/user/sum");

    return BuildYsonStringFluently()
        .BeginMap()
            .DoIf(static_cast<bool>(rowCount), [&] (TFluentMap fluent) {
                fluent.Item("processed_input_row_count").Value(rowCount->AsInt64()->GetValue());
            })
            .DoIf(static_cast<bool>(uncompressedDataSize), [&] (TFluentMap fluent) {
                fluent.Item("processed_input_uncompressed_data_size").Value(uncompressedDataSize->AsInt64()->GetValue());
            })
            .DoIf(static_cast<bool>(compressedDataSize), [&] (TFluentMap fluent) {
                fluent.Item("processed_input_compressed_data_size").Value(compressedDataSize->AsInt64()->GetValue());
            })
            .DoIf(static_cast<bool>(dataWeight), [&] (TFluentMap fluent) {
                fluent.Item("processed_input_data_weight").Value(dataWeight->AsInt64()->GetValue());
            })
            .DoIf(static_cast<bool>(inputPipeIdleTime), [&] (TFluentMap fluent) {
                fluent.Item("input_pipe_idle_time").Value(inputPipeIdleTime->AsInt64()->GetValue());
            })
            .DoIf(static_cast<bool>(jobProxyCpuUsage), [&] (TFluentMap fluent) {
                fluent.Item("job_proxy_cpu_usage").Value(jobProxyCpuUsage->AsInt64()->GetValue());
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void SanitizeJobSpec(NProto::TJobSpec* jobSpec)
{
    if (!jobSpec->HasExtension(NProto::TJobSpecExt::job_spec_ext)) {
        return;
    }

    auto* jobSpecExt = jobSpec->MutableExtension(NProto::TJobSpecExt::job_spec_ext);
    if (!jobSpecExt->has_user_job_spec()) {
        return;
    }

    auto* userJobSpec = jobSpecExt->mutable_user_job_spec();
    auto environment = FromProto<std::vector<TString>>(userJobSpec->environment());

    userJobSpec->clear_environment();
    for (const auto& variable : environment) {
        if (!variable.StartsWith(SecureVaultEnvPrefix)) {
            userJobSpec->add_environment(variable);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TControllerAgentDescriptor::operator bool() const
{
    return Addresses && !std::empty(*Addresses);
}

void FromProto(
    TControllerAgentDescriptor* controllerAgentDescriptor,
    const NProto::TControllerAgentDescriptor& controllerAgentDescriptorProto)
{
    if (controllerAgentDescriptorProto.has_addresses()) {
        controllerAgentDescriptor->Addresses = FromProto<NNodeTrackerClient::TAddressMap>(
            controllerAgentDescriptorProto.addresses());
    }
    controllerAgentDescriptor->IncarnationId = FromProto<TIncarnationId>(
        controllerAgentDescriptorProto.incarnation_id());
    controllerAgentDescriptor->AgentId = FromProto<TAgentId>(
        controllerAgentDescriptorProto.agent_id());
}

void ToProto(
    NProto::TControllerAgentDescriptor* controllerAgentDescriptorProto,
    const TControllerAgentDescriptor& controllerAgentDescriptor)
{
    if (controllerAgentDescriptor.Addresses) {
        ToProto(
            controllerAgentDescriptorProto->mutable_addresses(),
            *controllerAgentDescriptor.Addresses);
    }
    ToProto(
        controllerAgentDescriptorProto->mutable_incarnation_id(),
        controllerAgentDescriptor.IncarnationId);
    ToProto(
        controllerAgentDescriptorProto->mutable_agent_id(),
        controllerAgentDescriptor.AgentId);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TControllerAgentDescriptor& controllerAgentDescriptor,
    TStringBuf /*format*/)
{
    builder->AppendFormat(
        "{Addresses: %v, IncarnationId: %v, AgentId: %v}",
        controllerAgentDescriptor.Addresses,
        controllerAgentDescriptor.IncarnationId,
        controllerAgentDescriptor.AgentId);
}

////////////////////////////////////////////////////////////////////////////////

bool AreCompatible(ELayerAccessMethod accessMethod, ELayerFilesystem filesystem)
{
    if (accessMethod == ELayerAccessMethod::Nbd) {
        if (filesystem == ELayerFilesystem::Archive) {
            return false;
        }
    }

    if (accessMethod == ELayerAccessMethod::Local) {
        if (filesystem == ELayerFilesystem::Ext3 || filesystem == ELayerFilesystem::Ext4) {
            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void AdvanceEpoch(TControllerEpoch& epoch)
{
    ++epoch.Underlying();
}

////////////////////////////////////////////////////////////////////////////////

TIncarnationId IncarnationIdFromTransactionId(NObjectClient::TTransactionId transactionId)
{
    return TIncarnationId(transactionId);
}

NObjectClient::TTransactionId IncarnationIdToTransactionId(TIncarnationId incarnationId)
{
    return incarnationId.Underlying();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
