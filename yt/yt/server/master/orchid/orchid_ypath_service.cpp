#include "orchid_ypath_service.h"

#include "manifest.h"

#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/ytlib/orchid/orchid_service_proxy.h>
#include <yt/yt/ytlib/orchid/private.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/balancing_channel.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NOrchid {

using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = OrchidLogger;

////////////////////////////////////////////////////////////////////////////////

class TOrchidYPathServiceBase
    : public IYPathService
{
public:
    TOrchidYPathServiceBase(INodeChannelFactoryPtr channelFactory)
        : ChannelFactory_(std::move(channelFactory))
    { }

    TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

    void Invoke(const IServiceContextPtr& context) override
    {
        if (IsRequestMutating(context->RequestHeader())) {
            THROW_ERROR_EXCEPTION("Orchid nodes are read-only");
        }

        auto manifest = LoadManifest();

        auto channel = CreateChannel(manifest);

        TOrchidServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(manifest->Timeout);

        auto path = GetRedirectPath(manifest, GetRequestTargetYPath(context->RequestHeader()));
        const auto& method = context->GetMethod();

        auto requestMessage = context->GetRequestMessage();
        NRpc::NProto::TRequestHeader requestHeader;
        if (!ParseRequestHeader(requestMessage, &requestHeader)) {
            context->Reply(TError("Error parsing request header"));
            return;
        }

        SetRequestTargetYPath(&requestHeader, path);

        auto innerRequestMessage = SetRequestHeader(requestMessage, requestHeader);

        auto outerRequest = proxy.Execute();
        outerRequest->SetMultiplexingBand(EMultiplexingBand::Heavy);
        outerRequest->Attachments() = innerRequestMessage.ToVector();

        YT_LOG_DEBUG("Sending request to remote Orchid (Path: %v, Method: %v, RequestId: %v)",
            path,
            method,
            outerRequest->GetRequestId());

        outerRequest->Invoke().Subscribe(BIND(
            &TOrchidYPathServiceBase::OnResponse,
            context,
            manifest,
            path,
            method));
    }

    void DoWriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const std::optional<std::vector<TString>>& /*attributeKeys*/,
        bool /*stable*/) override
    {
        YT_ABORT();
    }

    bool ShouldHideAttributes() override
    {
        YT_ABORT();
    }

private:
    const INodeChannelFactoryPtr ChannelFactory_;

    virtual TOrchidManifestPtr LoadManifest() const = 0;

    IChannelPtr CreateChannel(const TOrchidManifestPtr& manifest)
    {
        switch (manifest->RemoteAddresses->GetType()) {
            case ENodeType::Map:
                return CreateRetryingChannel(
                    manifest,
                    ChannelFactory_->CreateChannel(ConvertTo<TAddressMap>(manifest->RemoteAddresses)));

            case ENodeType::List: {
                auto channelConfig = New<TBalancingChannelConfig>();
                channelConfig->Addresses = ConvertTo<std::vector<TString>>(manifest->RemoteAddresses);
                auto endpointDescription = TString("Orchid@");
                auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
                    .BeginMap()
                        .Item("orchid").Value(true)
                    .EndMap());
                return CreateRetryingChannel(
                    manifest,
                    CreateBalancingChannel(
                        std::move(channelConfig),
                        ChannelFactory_,
                        std::move(endpointDescription),
                        std::move(endpointAttributes)));
            }

            default:
                YT_ABORT();
        }
    }

    static void OnResponse(
        const IServiceContextPtr& context,
        const TOrchidManifestPtr& manifest,
        const TYPath& path,
        const TString& method,
        const TOrchidServiceProxy::TErrorOrRspExecutePtr& rspOrError)
    {
        if (rspOrError.IsOK()) {
            YT_LOG_DEBUG("Orchid request succeeded");
            const auto& rsp = rspOrError.Value();
            auto innerResponseMessage = TSharedRefArray(rsp->Attachments(), TSharedRefArray::TMoveParts{});
            context->Reply(std::move(innerResponseMessage));
        } else {
            context->Reply(TError("Error executing Orchid request")
                << TErrorAttribute("path", path)
                << TErrorAttribute("method", method)
                << TErrorAttribute("remote_addresses", manifest->RemoteAddresses)
                << TErrorAttribute("remote_root", manifest->RemoteRoot)
                << rspOrError);
        }
    }

    static TString GetRedirectPath(const TOrchidManifestPtr& manifest, const TYPath& path)
    {
        return manifest->RemoteRoot + path;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNodeBasedOrchidYPathService
    : public TOrchidYPathServiceBase
{
public:
    TNodeBasedOrchidYPathService(INodeChannelFactoryPtr channelFactory, INodePtr owningProxy)
        : TOrchidYPathServiceBase(std::move(channelFactory))
        , OwningNode_(std::move(owningProxy))
    { }

private:
    const INodePtr OwningNode_;

    TOrchidManifestPtr LoadManifest() const override
    {
        auto manifest = New<TOrchidManifest>();
        auto manifestNode = ConvertToNode(OwningNode_->Attributes());
        try {
            manifest->Load(manifestNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing Orchid manifest")
                << ex;
        }
        return manifest;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TManifestBasedOrchidYPathService
    : public TOrchidYPathServiceBase
{
public:
    TManifestBasedOrchidYPathService(INodeChannelFactoryPtr channelFactory, TOrchidManifestPtr manifest)
        : TOrchidYPathServiceBase(std::move(channelFactory))
        , Manifest_(std::move(manifest))
    { }

private:
    TOrchidManifestPtr Manifest_;

    TOrchidManifestPtr LoadManifest() const override
    {
        return Manifest_;
    }
};

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateOrchidYPathService(
    NNodeTrackerClient::INodeChannelFactoryPtr nodeChannelFactory,
    NYTree::INodePtr owningNode)
{
    return New<TNodeBasedOrchidYPathService>(std::move(nodeChannelFactory), std::move(owningNode));
}

NYTree::IYPathServicePtr CreateOrchidYPathService(
    NNodeTrackerClient::INodeChannelFactoryPtr nodeChannelFactory,
    TOrchidManifestPtr manifest)
{
    return New<TManifestBasedOrchidYPathService>(std::move(nodeChannelFactory), std::move(manifest));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrchid
