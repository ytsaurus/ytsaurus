#include "stdafx.h"
#include "cypress_integration.h"

#include <ytlib/misc/lazy_ptr.h>

#include <ytlib/actions/action_queue.h>

#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/ypath_detail.h>

#include <server/cypress_server/virtual.h>

#include <ytlib/orchid/orchid_service_proxy.h>

#include <ytlib/rpc/channel.h>
#include <ytlib/rpc/message.h>
#include <ytlib/rpc/channel_cache.h>

#include <ytlib/bus/message.h>

#include <server/cell_master/bootstrap.h>

#include <server/object_server/object_manager.h>

#include <server/cypress_server/node.h>

namespace NYT {
namespace NOrchid {

using namespace NRpc;
using namespace NBus;
using namespace NYTree;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NOrchid::NProto;

////////////////////////////////////////////////////////////////////////////////

static TChannelCache ChannelCache;
static TLazyPtr<TActionQueue> OrchidQueue(TActionQueue::CreateFactory("Orchid"));
static NLog::TLogger& Logger = OrchidLogger;

////////////////////////////////////////////////////////////////////////////////

class TOrchidYPathService
    : public IYPathService
{
public:
    TOrchidYPathService(
        TBootstrap* bootstrap,
        ICypressNode* node,
        TTransaction* transaction)
        : Bootstrap(bootstrap)
        , Node(node)
        , Transaction(transaction)
    { }

    TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr context) override
    {
        UNUSED(context);

        return TResolveResult::Here(path);
    }

    void Invoke(IServiceContextPtr context) override
    {
        auto manifest = LoadManifest();

        auto channel = ChannelCache.GetChannel(manifest->RemoteAddress);

        TOrchidServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(manifest->Timeout);

        auto path = GetRedirectPath(manifest, context->GetPath());
        auto verb = context->GetVerb();

        auto requestMessage = context->GetRequestMessage();
        NRpc::NProto::TRequestHeader requestHeader;
        if (!ParseRequestHeader(requestMessage, &requestHeader)) {
            context->Reply(TError("Error parsing request header"));
            return;
        }

        requestHeader.set_path(path);
        auto innerRequestMessage = SetRequestHeader(requestMessage, requestHeader);

        auto outerRequest = proxy.Execute();
        outerRequest->Attachments() = innerRequestMessage->GetParts();

        LOG_INFO("Sending request to the remote Orchid (RemoteAddress: %s, Path: %s, Verb: %s, RequestId: %s)",
            ~manifest->RemoteAddress,
            ~path,
            ~verb,
            ~outerRequest->GetRequestId().ToString());

        outerRequest->Invoke().Subscribe(
            BIND(
                &TOrchidYPathService::OnResponse,
                MakeStrong(this),
                context,
                manifest,
                path,
                verb)
            .Via(OrchidQueue->GetInvoker()));
    }

    Stroka GetLoggingCategory() const override
    {
        return OrchidLogger.GetCategory();
    }

    bool IsWriteRequest(IServiceContextPtr context) const override
    {
        UNUSED(context);
        return false;
    }

private:
    TBootstrap* Bootstrap;
    ICypressNode* Node;
    TTransaction* Transaction;

    TOrchidManifest::TPtr LoadManifest()
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto proxy = objectManager->GetProxy(Node->GetId().ObjectId, Transaction);
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
        TOrchidManifest::TPtr manifest,
        const TYPath& path,
        const Stroka& verb,
        TOrchidServiceProxy::TRspExecutePtr response)
    {
        LOG_INFO(*response, "Reply from a remote Orchid received (RequestId: %s)",
            ~ToString(context->GetRequestId()));

        if (response->IsOK()) {
            auto innerResponseMessage = CreateMessageFromParts(response->Attachments());
            context->Reply(innerResponseMessage);
        } else {
            context->Reply(TError("Error executing an Orchid operation (Path: %s, Verb: %s, RemoteAddress: %s, RemoteRoot: %s)",
                ~path,
                ~verb,
                ~manifest->RemoteAddress,
                ~manifest->RemoteRoot)
                << response->GetError());
        }
    }

    static Stroka GetRedirectPath(TOrchidManifest::TPtr manifest, const TYPath& path)
    {
        return manifest->RemoteRoot + path;
    }
};

INodeTypeHandlerPtr CreateOrchidTypeHandler(TBootstrap* bootstrap)
{
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::Orchid,
        BIND([=] (ICypressNode* trunkNode, TTransaction* transaction) -> IYPathServicePtr {
            return New<TOrchidYPathService>(bootstrap, trunkNode, transaction);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
