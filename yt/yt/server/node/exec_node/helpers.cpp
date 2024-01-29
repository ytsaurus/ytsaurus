#include "helpers.h"
#include "bootstrap.h"

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NExecNode {

using namespace NApi;
using namespace NClusterNode;
using namespace NChunkClient;
using namespace NDataNode;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NFileClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYTree;

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
        YT_LOG_INFO("Fetching layer basic attributes (LayerPath: %v, OldContentRevision: %x)",
            path,
            contentRevision);

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
            THROW_ERROR_EXCEPTION("Invalid type of layer object %v: expected %Qlv, actual %Qlv",
                path,
                EObjectType::File,
                userObject.Type)
                << TErrorAttribute("path", path)
                << TErrorAttribute("expected_type", EObjectType::File)
                << TErrorAttribute("actual_type", userObject.Type);
        }
    }

    auto objectId = userObject.ObjectId;
    auto objectIdPath = FromObjectId(objectId);

    // COMPAT(shakurov): remove this once YT-13605 is deployed everywhere.
    if (userObject.ContentRevision == NHydra::NullRevision) {
        YT_LOG_INFO("Fetching layer revision (LayerPath: %v, OldContentRevision: %x)", path, contentRevision);
        try {
            FetchContentRevision(bootstrap, &userObject);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error fetching revision for layer %v", path)
                << ex;
        }
    }

    auto result = TFetchedArtifactKey {
        .ContentRevision = userObject.ContentRevision
    };

    if (contentRevision == userObject.ContentRevision) {
        YT_LOG_INFO("Layer revision not changed, using cached (LayerPath: %v, ObjectId: %v)",
            path,
            objectId);
        return result;
    }

    YT_LOG_INFO("Fetching layer chunk specs (LayerPath: %v, ObjectId: %v, ContentRevision: %x)",
        path,
        objectId,
        userObject.ContentRevision);

    const auto& client = bootstrap->GetClient();
    const auto& connection = client->GetNativeConnection();

    auto proxy = CreateObjectServiceReadProxy(
        client,
        EMasterChannelKind::Cache,
        userObject.ExternalCellTag);
    auto batchReq = proxy.ExecuteBatchWithRetries(client->GetNativeConnection()->GetConfig()->ChunkFetchRetries);
    auto req = TFileYPathProxy::Fetch(objectIdPath);
    ToProto(req->mutable_ranges(), std::vector<TLegacyReadRange>{{}});
    SetSuppressAccessTracking(req, true);
    SetSuppressExpirationTimeoutRenewal(req, true);

    TMasterReadOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;
    SetCachingHeader(req, connection, options);
    req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);

    batchReq->AddRequest(req);
    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error fetching chunks for layer %v",
        path);

    const auto& batchRsp = batchRspOrError.Value();
    const auto& rspOrError = batchRsp->GetResponse<TFileYPathProxy::TRspFetch>(0);
    const auto& rsp = rspOrError.Value();

    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;
    ProcessFetchResponse(
        client,
        rsp,
        userObject.ExternalCellTag,
        bootstrap->GetNodeDirectory(),
        MaxChunksPerLocateRequest,
        std::nullopt,
        Logger,
        &chunkSpecs);

    TArtifactKey layerKey;
    ToProto(layerKey.mutable_chunk_specs(), chunkSpecs);
    layerKey.mutable_data_source()->set_type(static_cast<int>(EDataSourceType::File));
    layerKey.mutable_data_source()->set_path(path);

    result.ArtifactKey = std::move(layerKey);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TErrorOr<TString> TryParseControllerAgentAddress(
    const NNodeTrackerClient::NProto::TAddressMap& proto,
    const NNodeTrackerClient::TNetworkPreferenceList& localNetworks)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto addresses = FromProto<NNodeTrackerClient::TAddressMap>(proto);

    try {
        return GetAddressOrThrow(addresses, localNetworks);
    } catch (const std::exception& ex) {
        return TError(
            "No suitable controller agent address exists from %v",
            GetValues(addresses))
            << TError(ex);
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
    const NNodeTrackerClient::TNetworkPreferenceList& localNetworks)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto incarnationId = FromProto<NScheduler::TIncarnationId>(proto.incarnation_id());

    if (!proto.has_addresses()) {
        return TError("Controller agent descriptor has no addresses")
            << TErrorAttribute("incarnation_id", incarnationId);
    }

    auto addressOrError = TryParseControllerAgentAddress(proto.addresses(), localNetworks);
    if (!addressOrError.IsOK()) {
        return TError{std::move(addressOrError)}
            << TErrorAttribute("incarnation_id", incarnationId);
    }

    return TControllerAgentDescriptor{std::move(addressOrError.Value()), incarnationId};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

size_t THash<NYT::NExecNode::TControllerAgentDescriptor>::operator () (
    const NYT::NExecNode::TControllerAgentDescriptor& descriptor) const
{
    return MultiHash(descriptor.Address, descriptor.IncarnationId);
}
