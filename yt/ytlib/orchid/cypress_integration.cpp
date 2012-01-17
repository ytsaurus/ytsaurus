#include "stdafx.h"
#include "cypress_integration.h"

#include <ytlib/misc/lazy_ptr.h>

#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ypath_detail.h>

#include <ytlib/cypress/virtual.h>

#include <ytlib/orchid/orchid_service_proxy.h>

#include <ytlib/rpc/channel.h>
#include <ytlib/rpc/message.h>
#include <ytlib/rpc/channel_cache.h>

#include <ytlib/misc/new.h>

namespace NYT {
namespace NOrchid {

using namespace NRpc;
using namespace NYTree;
using namespace NCypress;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NRpc::TChannelCache ChannelCache;
static TLazyPtr<TActionQueue> OrchidQueue(TActionQueue::CreateFactory("Orchid"));
static NLog::TLogger& Logger = OrchidLogger;

class TOrchidYPathService
    : public IYPathService
{
public:
    typedef TIntrusivePtr<TOrchidYPathService> TPtr;

    TOrchidYPathService(const TYson& manifestYson)
    {
        auto manifestNode = DeserializeFromYson(manifestYson);
        try {
            Manifest = New<TOrchidManifest>();
            Manifest->LoadAndValidate(~manifestNode);
        } catch (...) {
            ythrow yexception() << Sprintf("Error parsing an Orchid manifest\n%s",
                ~CurrentExceptionMessage());
        }

        auto channel = ChannelCache.GetChannel(Manifest->RemoteAddress);
        Proxy = new TOrchidServiceProxy(~channel);
        Proxy->SetTimeout(Manifest->Timeout);
    }

    TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        UNUSED(verb);
        return TResolveResult::Here(path);
    }

    void Invoke(NRpc::IServiceContext* context)
    {
        TYPath path = GetRedirectPath(context->GetPath());
        Stroka verb = context->GetVerb();

        auto requestMessage = context->GetRequestMessage();
        auto requestHeader = GetRequestHeader(~requestMessage);
        requestHeader.set_path(path);
        auto innerRequestMessage = SetRequestHeader(~requestMessage, requestHeader);

        auto outerRequest = Proxy->Execute();
        WrapYPathRequest(~outerRequest, ~innerRequestMessage);

        LOG_INFO("Sending request to a remote Orchid (Address: %s, Path: %s, Verb: %s, RequestId: %s)",
            ~Manifest->RemoteAddress,
            ~path,
            ~verb,
            ~outerRequest->GetRequestId().ToString());

        outerRequest->Invoke()->Subscribe(
            ~FromMethod(
                &TOrchidYPathService::OnResponse,
                TPtr(this),
                IServiceContext::TPtr(context),
                path,
                verb)
            ->Via(OrchidQueue->GetInvoker()));
    }

    virtual Stroka GetLoggingCategory() const
    {
        return OrchidLogger.GetCategory();
    }

private:
    void OnResponse(
        TOrchidServiceProxy::TRspExecute::TPtr response,
        NRpc::IServiceContext::TPtr context,
        TYPath path,
        const Stroka& verb)
    {
        LOG_INFO("Reply from a remote Orchid received (RequestId: %s): %s",
            ~response->GetRequestId().ToString(),
            ~response->GetError().ToString());

        if (response->IsOK()) {
            auto innerResponseMessage = UnwrapYPathResponse(~response);
            ReplyYPathWithMessage(~context, ~innerResponseMessage);
        } else {
            context->Reply(TError(Sprintf("Error executing an Orchid operation (Path: %s, Verb: %s, RemoteAddress: %s, RemoteRoot: %s)\n%s",
                ~path,
                ~verb,
                ~Manifest->RemoteAddress,
                ~Manifest->RemoteRoot,
                ~response->GetError().ToString())));
        }
    }

    Stroka GetRedirectPath(const TYPath& path)
    {
        return CombineYPaths(Manifest->RemoteRoot, path);
    }

    TOrchidManifest::TPtr Manifest;
    TAutoPtr<TOrchidServiceProxy> Proxy;

};

INodeTypeHandler::TPtr CreateOrchidTypeHandler(
    TCypressManager* cypressManager)
{
    TCypressManager::TPtr cypressManager_ = cypressManager;
    return CreateVirtualTypeHandler(
        cypressManager,
        EObjectType::OrchidNode,
        ~FromFunctor([=] (const TVirtualYPathContext& context) -> IYPathService::TPtr
            {
                return New<TOrchidYPathService>(context.Manifest);
            }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
