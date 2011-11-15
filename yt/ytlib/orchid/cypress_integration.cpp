#include "stdafx.h"
#include "cypress_integration.h"

#include "../misc/config.h"
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

class TOrchidYPathService
    : public IYPathService
{
public:
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
            ythrow yexception() << Sprintf("Error parsing Orchid redirection manifest\n%s",
                ~CurrentExceptionMessage());
        }

        auto channel = ChannelCache.GetChannel(Manifest.RemoteAddress);
        Proxy = new TOrchidServiceProxy(~channel);
    }

    IYPathService::TNavigateResult Navigate(TYPath path, bool mustExist)
    {
        UNUSED(mustExist);

        if (path.empty()) {
            return TNavigateResult::There(~FallbackService, path);
        } else {
            return TNavigateResult::Here(path);
        }
    }

    void Invoke(NRpc::IServiceContext* context)
    {
        TYPath path = context->GetPath();
        if (!ShouldForward(path)) {
            FallbackService->Invoke(context);
            return;
        }

        auto requestMesage = context->GetRequestMessage();

        auto request = Proxy->Execute();
        WrapYPathRequest(~request, ~requestMesage);

        // TODO: logging
        NRpc::IServiceContext::TPtr context_ = context;
        request->Invoke()->Subscribe(~FromFunctor([=] (TOrchidServiceProxy::TRspExecute::TPtr response)
            {
                if (response->IsOK()) {
                    WrapYPathResponse(~context_, ~response->GetResponseMessage());
                    context_->Reply(TError(EErrorCode::OK));
                } else {
                    context_->Reply(TError(
                        EYPathErrorCode(EYPathErrorCode::GenericError),
                        Sprintf("Error executing Orchid operation\n%s",
                            ~response->GetError().ToString())));
                }
            }));
    }

private:
    static bool ShouldForward(TYPath path)
    {
        return !path.empty();
    }

    Stroka GetForwardPath(TYPath path)
    {
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
            Register("remote_root", RemoteRoot);
        }
    };

    IYPathService::TPtr FallbackService;
    TManifest Manifest;
    TAutoPtr<TOrchidServiceProxy> Proxy;

};

INodeTypeHandler::TPtr CreateOrchidTypeHandler(
    TCypressManager* cypressManager)
{
    TCypressManager::TPtr cypressManagerPtr = cypressManager;
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
