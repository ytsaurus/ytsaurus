#include "stdafx.h"
#include "cypress_integration.h"

#include "../misc/config.h"
#include "../misc/lazy_ptr.h"
#include "../ytree/ephemeral.h"
#include "../ytree/serialize.h"
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
static NLog::TLogger& Logger = OrchidLogger;

class TOrchidYPathService
    : public IYPathService
{
public:
    typedef TIntrusivePtr<TOrchidYPathService> TPtr;

    TOrchidYPathService(const TYson& manifestYson)
    {
        auto manifestRoot = DeserializeFromYson(manifestYson, GetEphemeralNodeFactory());

        try {
            Manifest.Load(~manifestRoot);
        } catch (...) {
            ythrow yexception() << Sprintf("Error parsing an Orchid manifest\n%s",
                ~CurrentExceptionMessage());
        }

        auto channel = ChannelCache.GetChannel(Manifest.RemoteAddress);
        Proxy = new TOrchidServiceProxy(~channel);
        Proxy->SetTimeout(Manifest.Timeout);
    }

    TResolveResult Resolve(TYPath path, const Stroka& verb)
    {
        UNUSED(verb);
        return TResolveResult::Here(path);
    }

    void Invoke(NRpc::IServiceContext* context)
    {
        TYPath path = GetRedirectPath(context->GetPath());
        Stroka verb = context->GetVerb();

        auto innerRequestMessage = UpdateYPathRequestHeader(
            ~context->GetRequestMessage(),
            path,
            verb);

        auto outerRequest = Proxy->Execute();
        WrapYPathRequest(~outerRequest, ~innerRequestMessage);

        LOG_INFO("Sending request to a remote Orchid (Address: %s, Path: %s, Verb: %s, RequestId: %s)",
            ~Manifest.RemoteAddress,
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
        TDuration Timeout;

        TManifest()
        {
            Register("remote_address", RemoteAddress);
            Register("remote_root", RemoteRoot).Default("/");
            Register("timeout", Timeout).Default(TDuration::MilliSeconds(3000));
        }
    };

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
                return New<TOrchidYPathService>(context.Manifest);
            }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
