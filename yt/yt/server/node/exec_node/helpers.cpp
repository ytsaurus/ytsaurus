#include "bootstrap.h"
#include "private.h"
#include "helpers.h"

#include <yt/yt/server/node/exec_node/job_controller.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/permission.h>

#include <util/generic/vector.h>

namespace NYT::NExecNode {

using namespace NApi;
using namespace NClusterNode;
using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NFileClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYTree;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr int MaxChunksPerLocateRequest = 10000;

namespace {

void FetchContentRevision(
    IBootstrap const* bootstrap,
    TUserObject* userObject)
{
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;

    const auto& client = bootstrap->GetClient();
    auto rsp = WaitFor(client->GetNode(FromObjectId(userObject->ObjectId) + "/@content_revision"))
        .ValueOrThrow();
    userObject->ContentRevision = ConvertTo<NHydra::TRevision>(rsp);
}

} // namespace

TFetchedArtifactKey FetchLayerArtifactKeyIfRevisionChanged(
    const NYPath::TYPath& path,
    NHydra::TRevision contentRevision,
    IBootstrap const* bootstrap,
    const NLogging::TLogger& logger)
{
    const auto& Logger = logger;

    TUserObject userObject;
    userObject.Path = path;

    {
        YT_TLOG_INFO("Fetching layer basic attributes")
            .With("LayerPath", path)
            .WithFormat("OldContentRevision", "%x", contentRevision);

        TGetUserObjectBasicAttributesOptions options;
        options.SuppressAccessTracking = true;
        options.SuppressExpirationTimeoutRenewal = true;
        options.ReadFrom = EMasterChannelKind::Cache;
        GetUserObjectBasicAttributes(
            bootstrap->GetClient(),
            {&userObject},
            NullTransactionId,
            Logger,
            EPermission::Read,
            options);

        if (userObject.Type != EObjectType::File) {
            THROW_ERROR_EXCEPTION(
                "Invalid type of layer object %v: expected %Qlv, actual %Qlv",
                path,
                EObjectType::File,
                userObject.Type)
                .With("path", path)
                .With("expected_type", EObjectType::File)
                .With("actual_type", userObject.Type);
        }
    }

    auto objectId = userObject.ObjectId;
    auto objectIdPath = FromObjectId(objectId);

    // COMPAT(shakurov): remove this once YT-13605 is deployed everywhere.
    if (userObject.ContentRevision == NHydra::NullRevision) {
        YT_TLOG_INFO("Fetching layer revision")
            .With("LayerPath", path)
            .WithFormat("OldContentRevision", "%x", contentRevision);
        try {
            FetchContentRevision(bootstrap, &userObject);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Error fetching revision for layer %v", path)
                .With(ex);
        }
    }

    auto result = TFetchedArtifactKey {
        .ContentRevision = userObject.ContentRevision
    };

    if (contentRevision == userObject.ContentRevision) {
        YT_TLOG_INFO("Layer revision not changed, using cached")
            .With("LayerPath", path)
            .With("ObjectId", objectId);
        return result;
    }

    YT_TLOG_INFO("Fetching layer chunk specs")
        .With("LayerPath", path)
        .With("ObjectId", objectId)
        .WithFormat("ContentRevision", "%x", userObject.ContentRevision);

    const auto& client = bootstrap->GetClient();
    const auto& connection = client->GetNativeConnection();

    auto proxy = CreateObjectServiceReadProxy(
        client,
        EMasterChannelKind::Cache,
        userObject.ExternalCellTag);
    auto batchReq = proxy.ExecuteBatchWithRetries(client->GetNativeConnection()->GetConfig()->ChunkFetchRetries);

    {
        // Add file fetch request.
        auto req = TFileYPathProxy::Fetch(objectIdPath);
        ToProto(req->mutable_ranges(), std::vector<TLegacyReadRange>{{}});
        SetSuppressAccessTracking(req, true);
        SetSuppressExpirationTimeoutRenewal(req, true);

        TMasterReadOptions options;
        options.ReadFrom = EMasterChannelKind::Cache;
        SetCachingHeader(req, connection, options);
        req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);

        batchReq->AddRequest(req);
    }

    {
        // Add get_attributes request.
        auto req = TYPathProxy::Get(objectIdPath + "/@");
        SetSuppressAccessTracking(req, true);
        SetSuppressExpirationTimeoutRenewal(req, true);

        TMasterReadOptions options;
        options.ReadFrom = EMasterChannelKind::Cache;
        SetCachingHeader(req, connection, options);
        ToProto(req->mutable_attributes()->mutable_keys(), TVector<TString>{"filesystem", "access_method"});

        batchReq->AddRequest(req, "get_attributes");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        GetCumulativeError(batchRspOrError),
        "Error fetching chunks for layer %v",
        path);

    const auto& batchRsp = batchRspOrError.Value();
    const auto& rspOrError = batchRsp->GetResponse<TFileYPathProxy::TRspFetch>(0);
    const auto& fileFetchRsp = rspOrError.Value();

    // Process file fetch response.
    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;
    ProcessFetchResponse(
        client,
        fileFetchRsp,
        userObject.ExternalCellTag,
        bootstrap->GetNodeDirectory(),
        MaxChunksPerLocateRequest,
        std::nullopt,
        Logger,
        &chunkSpecs);

    // Process get_attributes response.
    auto getAttributesRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGetKey>("get_attributes");
    auto getAttributesRsp = getAttributesRspOrError.Value();
    auto attributeDictionaryPtr = ConvertToAttributes(NYson::TYsonString(getAttributesRsp->value()));
    const auto& attributes = *attributeDictionaryPtr;

    auto accessMethod = attributes.Find<ELayerAccessMethod>("access_method").value_or(ELayerAccessMethod::Local);
    auto filesystem = attributes.Find<ELayerFilesystem>("filesystem").value_or(ELayerFilesystem::Archive);

    ValidateCompatibility(accessMethod, filesystem);

    // Create artifact key.
    TArtifactKey layerKey;
    ToProto(layerKey.mutable_chunk_specs(), chunkSpecs);
    layerKey.mutable_data_source()->set_type(ToProto(EDataSourceType::File));
    layerKey.mutable_data_source()->set_path(path);
    layerKey.set_access_method(ToProto(accessMethod));
    layerKey.set_filesystem(ToProto(filesystem));
    result.ArtifactKey = std::move(layerKey);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TErrorOr<std::string> TryParseControllerAgentAddress(
    const NNodeTrackerClient::NProto::TAddressMap& proto,
    const NNodeTrackerClient::TNetworkPreferenceList& networks)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto addresses = FromProto<NNodeTrackerClient::TAddressMap>(proto);

    try {
        return GetAddressOrThrow(addresses, networks);
    } catch (const std::exception& ex) {
        return TError(
            "No suitable controller agent address exists from %v",
            GetValues(addresses))
            .With(ex);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TControllerAgentDescriptor::operator bool() const noexcept
{
    return *this != TControllerAgentDescriptor{};
}

void FormatValue(
    TStringBuilderBase* builder,
    const TControllerAgentDescriptor& controllerAgentDescriptor,
    TStringBuf /*format*/)
{
    builder->AppendFormat(
        "{Address: %v, IncarnationId: %v}",
        controllerAgentDescriptor.Address,
        controllerAgentDescriptor.IncarnationId);
}

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TControllerAgentDescriptor> TryParseControllerAgentDescriptor(
    const NControllerAgent::NProto::TControllerAgentDescriptor& proto,
    const NNodeTrackerClient::TNetworkPreferenceList& networks)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto incarnationId = FromProto<NScheduler::TIncarnationId>(proto.incarnation_id());

    if (!proto.has_addresses()) {
        return TError("Controller agent descriptor has no addresses")
            .With("incarnation_id", incarnationId);
    }

    auto addressOrError = TryParseControllerAgentAddress(proto.addresses(), networks);
    if (!addressOrError.IsOK()) {
        return TError{std::move(addressOrError)}
            .With("incarnation_id", incarnationId);
    }

    return TControllerAgentDescriptor{std::move(addressOrError.Value()), incarnationId};
}

////////////////////////////////////////////////////////////////////////////////

TControllerAgentAffiliationInfo::TControllerAgentAffiliationInfo()
    : DescriptorResetTime_(TInstant::Now())
{ }

TControllerAgentAffiliationInfo::TControllerAgentAffiliationInfo(TControllerAgentDescriptor descriptor)
    : Descriptor_(std::move(descriptor))
{ }

const TControllerAgentDescriptor& TControllerAgentAffiliationInfo::GetDescriptor() const noexcept
{
    return Descriptor_;
}

TInstant TControllerAgentAffiliationInfo::GetDescriptorResetTime() const noexcept
{
    return DescriptorResetTime_;
}

void TControllerAgentAffiliationInfo::SetDescriptor(TControllerAgentDescriptor descriptor)
{
    if (!descriptor) {
        ResetControllerAgent();
        return;
    }
    Descriptor_ = std::move(descriptor);
    DescriptorResetTime_ = {};
}

void TControllerAgentAffiliationInfo::ResetControllerAgent()
{
    Descriptor_ = {};
    DescriptorResetTime_ = TInstant::Now();
}

////////////////////////////////////////////////////////////////////////////////

const TAbsoluteNormalizedPath& GetVolumeMountPathByVolumeId(const std::string& volumeId, const std::vector<TVolumeMountPtr>& volumeMounts)
{
    auto volumeMountIt = std::find_if(volumeMounts.begin(), volumeMounts.end(), [&] (const auto& volumeMount) {
        return volumeMount->VolumeId == volumeId;
    });

    // COMPAT(krasovav): Now all tmpfs volumes must be in user job rootfs.
    // Therefore, there must not be a single tmpfs that is not listed in volumeMounts.
    YT_VERIFY(volumeMountIt != volumeMounts.end());
    return volumeMountIt->Get()->MountPath;
}

const TVolumeResultPtr& GetNonRootVolumeResultByVolumeId(const std::string& volumeId, const std::vector<TVolumeResultPtr>& volumes)
{
    auto volumeIt = std::find_if(volumes.begin(), volumes.end(), [&] (const auto& volume) {
        return volume->VolumeId == volumeId;
    });

    YT_VERIFY(volumeIt != volumes.end());
    return *volumeIt;
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TSandboxNbdRootVolumeSpec* nbd, const NScheduler::NProto::TNbdDiskRequest& protoNbd)
{
    nbd->DeviceSize = protoNbd.disk_request().storage_request_common_parameters().disk_space();

    switch (protoNbd.backend_case()) {
        case NScheduler::NProto::TNbdDiskRequest::kChunkNbd: {
            const auto& protoChunkDisk = protoNbd.chunk_nbd();
            nbd->BackendSpec = TChunkNbdVolumeSpec{
                .MediumIndex = static_cast<int>(protoNbd.disk_request().medium_index()),
                .DataNodeRpcTimeout = FromProto<TDuration>(protoChunkDisk.data_node_rpc_timeout()),
                .DataNodeAddress = YT_OPTIONAL_FROM_PROTO(protoChunkDisk, data_node_address),
                .DataNodeNbdServiceRpcTimeout = FromProto<TDuration>(protoChunkDisk.data_node_nbd_service_rpc_timeout()),
                .DataNodeNbdServiceMakeTimeout = FromProto<TDuration>(protoChunkDisk.data_node_nbd_service_make_timeout()),
                .MasterRpcTimeout = FromProto<TDuration>(protoChunkDisk.master_rpc_timeout()),
                .MinDataNodeCount = protoChunkDisk.min_data_node_count(),
                .MaxDataNodeCount = protoChunkDisk.max_data_node_count(),
                .MultiplexingParallelism = protoChunkDisk.multiplexing_parallelism(),
            };
            break;
        }

        case NScheduler::NProto::TNbdDiskRequest::BACKEND_NOT_SET:
            THROW_ERROR_EXCEPTION("NBD disk request specifies no backend");
    }
}

void FromProto(TTmpfsVolumeParams* tmpfs, const NScheduler::NProto::TTmpfsStorageRequest& protoTmpfs)
{
    tmpfs->Size = protoTmpfs.storage_request_common_parameters().disk_space();
    tmpfs->Index = protoTmpfs.tmpfs_index();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

size_t THash<NYT::NExecNode::TControllerAgentDescriptor>::operator()(
    const NYT::NExecNode::TControllerAgentDescriptor& descriptor) const
{
    return MultiHash(descriptor.Address, descriptor.IncarnationId);
}
