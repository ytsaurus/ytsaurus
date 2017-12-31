#include "cypress_integration.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/cypress_server/node.h>
#include <yt/server/cypress_server/virtual.h>

#include <yt/server/cell_master/bootstrap.h>

#include <yt/ytlib/orchid/orchid_service_proxy.h>
#include <yt/ytlib/orchid/private.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/core/rpc/bus_channel.h>
#include <yt/core/rpc/caching_channel_factory.h>
#include <yt/core/rpc/retrying_channel.h>

namespace NYT {
namespace NOrchid {

using namespace NRpc;
using namespace NBus;
using namespace NYTree;
using namespace NYson;
using namespace NHydra;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NOrchid::NProto;
using namespace NNodeTrackerClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = OrchidLogger;

////////////////////////////////////////////////////////////////////////////////

class TOrchidYPathService
    : public IYPathService
{
public:
    TOrchidYPathService(INodeChannelFactoryPtr channelFactory, INodePtr owningProxy)
        : ChannelFactory_(std::move(channelFactory))
        , OwningNode_(std::move(owningProxy))
    { }

    virtual TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

    virtual void Invoke(const IServiceContextPtr& context) override
    {
        const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        if (ypathExt.mutating()) {
            THROW_ERROR_EXCEPTION("Orchid nodes are read-only");
        }

        auto manifest = LoadManifest();

        auto channel = CreateRetryingChannel(
            manifest,
            ChannelFactory_->CreateChannel(manifest->RemoteAddresses));

        TOrchidServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(manifest->Timeout);

        auto path = GetRedirectPath(manifest, GetRequestYPath(context->RequestHeader()));
        const auto& method = context->GetMethod();

        auto requestMessage = context->GetRequestMessage();
        NRpc::NProto::TRequestHeader requestHeader;
        if (!ParseRequestHeader(requestMessage, &requestHeader)) {
            context->Reply(TError("Error parsing request header"));
            return;
        }

        SetRequestYPath(&requestHeader, path);

        auto innerRequestMessage = SetRequestHeader(requestMessage, requestHeader);

        auto outerRequest = proxy.Execute();
        outerRequest->SetMultiplexingBand(EMultiplexingBand::Heavy);
        outerRequest->Attachments() = innerRequestMessage.ToVector();

        LOG_DEBUG("Sending request to remote Orchid (RemoteAddress: %v, Path: %v, Method: %v, RequestId: %v)",
            GetDefaultAddress(manifest->RemoteAddresses),
            path,
            method,
            outerRequest->GetRequestId());

        outerRequest->Invoke().Subscribe(BIND(
            &TOrchidYPathService::OnResponse,
            context,
            manifest,
            path,
            method));
    }

    virtual void DoWriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const TNullable<std::vector<TString>>& /*attributeKeys*/,
        bool /*stable*/) override
    {
        Y_UNREACHABLE();
    }

    virtual bool ShouldHideAttributes() override
    {
        Y_UNREACHABLE();
    }

private:
    const INodeChannelFactoryPtr ChannelFactory_;
    const INodePtr OwningNode_;


    TOrchidManifestPtr LoadManifest()
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

    static void OnResponse(
        const IServiceContextPtr& context,
        const TOrchidManifestPtr& manifest,
        const TYPath& path,
        const TString& method,
        const TOrchidServiceProxy::TErrorOrRspExecutePtr& rspOrError)
    {
        if (rspOrError.IsOK()) {
            LOG_DEBUG("Orchid request succeeded");
            const auto& rsp = rspOrError.Value();
            auto innerResponseMessage = TSharedRefArray(rsp->Attachments());
            context->Reply(innerResponseMessage);
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

INodeTypeHandlerPtr CreateOrchidTypeHandler(NCellMaster::TBootstrap* bootstrap)
{
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::Orchid,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TOrchidYPathService>(bootstrap->GetNodeChannelFactory(), owningNode);
        }),
        EVirtualNodeOptions::None);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
