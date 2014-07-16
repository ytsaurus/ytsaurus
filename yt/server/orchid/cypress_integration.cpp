#include "stdafx.h"
#include "cypress_integration.h"

#include <core/misc/lazy_ptr.h>

#include <core/concurrency/action_queue.h>

#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/ypath_detail.h>

#include <core/ytree/ypath.pb.h>

#include <ytlib/orchid/private.h>

#include <core/rpc/channel.h>
#include <core/rpc/message.h>
#include <core/rpc/caching_channel_factory.h>
#include <core/rpc/bus_channel.h>

#include <ytlib/orchid/orchid_service_proxy.h>

#include <ytlib/hydra/rpc_helpers.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>

#include <server/object_server/object_manager.h>
#include <server/object_server/object_proxy.h>

#include <server/cypress_server/node.h>
#include <server/cypress_server/virtual.h>

#include <server/hydra/mutation_context.h>
#include <server/hydra/hydra_manager.h>

namespace NYT {
namespace NOrchid {

using namespace NRpc;
using namespace NBus;
using namespace NYTree;
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
static TLazyIntrusivePtr<TActionQueue> OrchidQueue(TActionQueue::CreateFactory("Orchid"));

////////////////////////////////////////////////////////////////////////////////

class TOrchidYPathService
    : public IYPathService
{
public:
    TOrchidYPathService(
        TBootstrap* bootstrap,
        TCypressNodeBase* trunkNode,
        TTransaction* transaction)
        : Bootstrap(bootstrap)
        , TrunkNode(trunkNode)
        , Transaction(transaction)
    {
        YCHECK(trunkNode->IsTrunk());
    }

    TResolveResult Resolve(const TYPath& path, IServiceContextPtr /*context*/) override
    {
        return TResolveResult::Here(path);
    }

    void Invoke(IServiceContextPtr context) override
    {
        auto hydraManager = Bootstrap->GetHydraFacade()->GetHydraManager();
        
        // Prevent regarding the request as a mutating one.
        if (hydraManager->IsMutating()) {
            auto* mutationContext = hydraManager->GetMutationContext();
            mutationContext->SuppressMutation();
        }

        // Prevent doing anything during recovery and at followers.
        if (!hydraManager->IsLeader()) {
            return;
        }

        auto manifest = LoadManifest();

        auto channel = ChannelFactory->CreateChannel(manifest->RemoteAddress);

        TOrchidServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(manifest->Timeout);

        auto path = GetRedirectPath(manifest, GetRequestYPath(context));
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

        LOG_DEBUG("Sending request to the remote Orchid (RemoteAddress: %s, Path: %s, Method: %s, RequestId: %s)",
            ~manifest->RemoteAddress,
            ~path,
            ~method,
            ~ToString(outerRequest->GetRequestId()));

        outerRequest->Invoke().Subscribe(
            BIND(
                &TOrchidYPathService::OnResponse,
                MakeStrong(this),
                context,
                manifest,
                path,
                method)
            .Via(OrchidQueue->GetInvoker()));
    }

    virtual NLog::TLogger GetLogger() const override
    {
        return OrchidLogger;
    }

    // TODO(panin): remove this when getting rid of IAttributeProvider
    virtual void SerializeAttributes(
        NYson::IYsonConsumer* /*consumer*/,
        const TAttributeFilter& /*filter*/,
        bool /*sortKeys*/) override
    {
        YUNREACHABLE();
    }

private:
    TBootstrap* Bootstrap;
    TCypressNodeBase* TrunkNode;
    TTransaction* Transaction;

    TOrchidManifestPtr LoadManifest()
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto proxy = objectManager->GetProxy(TrunkNode, Transaction);
        auto manifest = New<TOrchidManifest>();
        auto manifestNode = ConvertToNode(proxy->Attributes());
        try {
            manifest->Load(manifestNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing Orchid manifest")
                << ex;
        }
        return manifest;
    }

    void OnResponse(
        IServiceContextPtr context,
        TOrchidManifestPtr manifest,
        const TYPath& path,
        const Stroka& method,
        TOrchidServiceProxy::TRspExecutePtr response)
    {
        if (response->IsOK()) {
            LOG_DEBUG("Orchid request succeded (RequestId: %s)",
                ~ToString(context->GetRequestId()));
            auto innerResponseMessage = TSharedRefArray(response->Attachments());
            context->Reply(innerResponseMessage);
        } else {
            LOG_DEBUG(*response, "Orchid request failed (RequestId: %s)",
                ~ToString(context->GetRequestId()));
            context->Reply(TError("Error executing an Orchid operation (Path: %s, Method: %s, RemoteAddress: %s, RemoteRoot: %s)",
                ~path,
                ~method,
                ~manifest->RemoteAddress,
                ~manifest->RemoteRoot)
                << response->GetError());
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
        BIND([=] (TCypressNodeBase* trunkNode, TTransaction* transaction) -> IYPathServicePtr {
            return New<TOrchidYPathService>(bootstrap, trunkNode, transaction);
        }),
        EVirtualNodeOptions::None);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
