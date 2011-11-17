#include "stdafx.h"
#include "cypress_integration.h"

#include "../misc/config.h"
#include "../misc/lazy_ptr.h"
#include "../ytree/yson_reader.h"
#include "../ytree/yson_writer.h"
#include "../ytree/ephemeral.h"
#include "../ytree/ypath_detail.h"
#include "../cypress/virtual.h"
#include "../orchid/orchid_service_rpc.h"
#include "../rpc/channel.h"

namespace NYT {
namespace NOrchid {

using namespace NRpc;
using namespace NYTree;
using namespace NCypress;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NRpc::TChannelCache ChannelCache;
static TLazyPtr<TActionQueue> OrchidQueue;

class TOrchidYPathService
    : public IYPathService
{
public:
    typedef TIntrusivePtr<TOrchidYPathService> TPtr;

    TOrchidYPathService(IYPathService* fallbackService, const TYson& manifestYson)
        : FallbackService(fallbackService)
    {
        auto manifestBuilder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        TYsonReader reader(~manifestBuilder);
        manifestBuilder->BeginTree();
        TStringInput manifestInput(manifestYson);
        reader.Read(&manifestInput);
        auto manifestRoot = manifestBuilder->EndTree();

        try {
            Manifest.Load(~manifestRoot);
        } catch (...) {
            ythrow yexception() << Sprintf("Error parsing an Orchid manifest\n%s",
                ~CurrentExceptionMessage());
        }

        auto channel = ChannelCache.GetChannel(Manifest.RemoteAddress);
        Proxy = new TOrchidServiceProxy(~channel);
    }

    IYPathService::TResolveResult Resolve(TYPath path, const Stroka& verb)
    {
        UNUSED(verb);

        if (IsEmptyYPath(path)) {
            return TResolveResult::There(~FallbackService, path);
        } else {
            return TResolveResult::Here(path);
        }
    }

    void Invoke(NRpc::IServiceContext* context)
    {
        TYPath path = context->GetPath();
        Stroka verb = context->GetVerb();

        // TODO: logging

        auto redirectPath = GetRedirectPath(path);

        auto outerRequestMesage = UpdateYPathRequestHeader(
            ~context->GetRequestMessage(),
            redirectPath,
            verb);

        auto innerRequest = Proxy->Execute();
        WrapYPathRequest(~innerRequest, ~outerRequestMesage);

        innerRequest->Invoke()->Subscribe(
            ~FromMethod(
                &TOrchidYPathService::OnResponse,
                TPtr(this),
                NRpc::IServiceContext::TPtr(context),
                redirectPath,
                verb)
            ->Via(OrchidQueue->GetInvoker()));
    }

private:
    void OnResponse(
        TOrchidServiceProxy::TRspExecute::TPtr response,
        NRpc::IServiceContext::TPtr context,
        TYPath path,
        const Stroka& verb)
    {
        if (response->IsOK()) {
            auto innerResponseMessage = UnwrapYPathResponse(~response);
            ReplyYPathWithMessage(~context, ~innerResponseMessage);
        } else {
            context->Reply(TError(
                EYPathErrorCode(EYPathErrorCode::GenericError),
                Sprintf("Error executing an Orchid operation (Path: %s, Verb: %s, RemoteAddress: %s, RemoteRoot: %s)\n%s",
                    ~path,
                    ~verb,
                    ~Manifest.RemoteAddress,
                    ~Manifest.RemoteRoot,
                    ~response->GetError().ToString())));
        }
    }

    Stroka GetRedirectPath(TYPath path)
    {
        // TODO: use CombineYPath
        return path == "/" ? Manifest.RemoteRoot : Manifest.RemoteRoot + path;
    }

    struct TManifest
        : TConfigBase
    {
        Stroka RemoteAddress;
        Stroka RemoteRoot;

        TManifest()
        {
            Register("remote_address", RemoteAddress);
            Register("remote_root", RemoteRoot).Default("/");
        }
    };

    IYPathService::TPtr FallbackService;
    TManifest Manifest;
    TAutoPtr<TOrchidServiceProxy> Proxy;

};

INodeTypeHandler::TPtr CreateOrchidTypeHandler(
    TCypressManager* cypressManager)
{
    TCypressManager::TPtr cypressManager_ = cypressManager;
    return CreateVirtualTypeHandler(
        cypressManager,
        ERuntimeNodeType::Orchid,
        // TODO: extract constant
        "orchid",
        ~FromFunctor([=] (const TVirtualYPathContext& context) -> IYPathService::TPtr
            {
                return New<TOrchidYPathService>(
                    ~IYPathService::FromNode(~context.Fallback),
                    context.Manifest);
            }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
