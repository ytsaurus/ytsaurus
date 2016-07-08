#include "cypress_integration.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/cypress_server/node.h>
#include <yt/server/cypress_server/virtual.h>

#include <yt/server/hydra/hydra_manager.h>
#include <yt/server/hydra/mutation_context.h>

#include <yt/server/object_server/object_manager.h>
#include <yt/server/object_server/object_proxy.h>

#include <yt/ytlib/orchid/orchid_service_proxy.h>
#include <yt/ytlib/orchid/private.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/lazy_ptr.h>

#include <yt/core/rpc/bus_channel.h>
#include <yt/core/rpc/caching_channel_factory.h>
#include <yt/core/rpc/channel.h>
#include <yt/core/rpc/helpers.h>
#include <yt/core/rpc/message.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/ypath.pb.h>
#include <yt/core/ytree/ypath_detail.h>

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
using namespace NTransactionServer;
using namespace NOrchid::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = OrchidLogger;

static IChannelFactoryPtr ChannelFactory(CreateCachingChannelFactory(GetBusChannelFactory()));

////////////////////////////////////////////////////////////////////////////////

class TOrchidYPathService
    : public IYPathService
{
public:
    TOrchidYPathService(TBootstrap* bootstrap, INodePtr owningProxy)
        : Bootstrap_(bootstrap)
        , OwningNode_(owningProxy)
    { }

    TResolveResult Resolve(const TYPath& path, IServiceContextPtr /*context*/) override
    {
        return TResolveResult::Here(path);
    }

    void Invoke(IServiceContextPtr context) override
    {
        const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        if (ypathExt.mutating()) {
            THROW_ERROR_EXCEPTION("Orchid nodes are read-only");
        }

        auto manifest = LoadManifest();

        auto channel = ChannelFactory->CreateChannel(manifest->RemoteAddress);

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
        outerRequest->Attachments() = innerRequestMessage.ToVector();

        LOG_DEBUG("Sending request to remote Orchid (RemoteAddress: %v, Path: %v, Method: %v, RequestId: %v)",
            manifest->RemoteAddress,
            path,
            method,
            outerRequest->GetRequestId());

        outerRequest->Invoke().Subscribe(
            BIND(
                &TOrchidYPathService::OnResponse,
                context,
                manifest,
                path,
                method));
    }

    virtual void WriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const TNullable<std::vector<Stroka>>& /*attributeKeys*/,
        bool /*sortKeys*/) override
    {
        Y_UNREACHABLE();
    }

private:
    TBootstrap* const Bootstrap_;
    const INodePtr OwningNode_;


    TOrchidManifestPtr LoadManifest()
    {
        auto objectManager = Bootstrap_->GetObjectManager();
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
        IServiceContextPtr context,
        TOrchidManifestPtr manifest,
        const TYPath& path,
        const Stroka& method,
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
                << TErrorAttribute("remote_address", manifest->RemoteAddress)
                << TErrorAttribute("remote_root", manifest->RemoteRoot)
                << rspOrError);
        }
    }

    static Stroka GetRedirectPath(TOrchidManifestPtr manifest, const TYPath& path)
    {
        return manifest->RemoteRoot + path;
    }
};

INodeTypeHandlerPtr CreateOrchidTypeHandler(TBootstrap* bootstrap)
{
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::Orchid,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TOrchidYPathService>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::None);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
