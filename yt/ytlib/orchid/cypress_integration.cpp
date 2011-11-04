#include "stdafx.h"
#include "cypress_integration.h"

#include "../ytree/ypath.h"
#include "../ytree/yson_reader.h"
#include "../ytree/yson_writer.h"
#include "../ytree/ephemeral.h"
#include "../cypress/virtual.h"
#include "../orchid/orchid_service_rpc.h"
#include "../rpc/channel.h"

namespace NYT {
namespace NOrchid {

using namespace NYTree;
using namespace NCypress;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NRpc::TChannelCache ChannelCache;

class TOrchidYPathService
    : public IYPathService
{
public:
    TOrchidYPathService(IYPathService* fallbackService, const Stroka& manifest)
        : FallbackService(fallbackService)
    {
        auto manifestBuilder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        TYsonReader reader(~manifestBuilder);
        manifestBuilder->BeginTree();
        TStringInput manifestInput(manifest);
        reader.Read(&manifestInput);
        auto manifestRoot = manifestBuilder->EndTree()->AsMap();

        // TODO: refactor using new API
        auto remoteAddressNode = manifestRoot->FindChild("remote_address");
        if (~remoteAddressNode == NULL) {
            ythrow TYTreeException() << "Missing remote_address";
        }
        RemoteAddress = remoteAddressNode->GetValue<Stroka>();

        auto remoteRootNode = manifestRoot->FindChild("remote_root");
        if (~remoteRootNode == NULL) {
            ythrow TYTreeException() << "Missing remote_root";
        }
        RemoteRoot = remoteRootNode->GetValue<Stroka>();

        auto channel = ChannelCache.GetChannel(RemoteAddress);
        Proxy = new TOrchidServiceProxy(~channel);
    }

    virtual TGetResult Get(TYPath path, IYsonConsumer* consumer)
    {
        if (!ShouldForward(path)) {
            return FallbackService->Get(path, consumer);
        }

        auto request = Proxy->Get();
        request->SetPath(GetForwardPath(path));
        
        auto response = request->Invoke()->Get();
        if (!response->IsOK()) {
            ythrow TYTreeException() << Sprintf("Error getting an Orchid path (RemoteAddress: %s, RemoteRoot: %s, Path: %s, Error: %s)",
                ~RemoteAddress,
                ~RemoteRoot,
                ~path,
                ~response->GetErrorCode().ToString());
        }

        TStringInput input(response->GetValue());
        TYsonReader reader(consumer);
        reader.Read(&input);

        return TRemoveResult::CreateDone();
    }

    virtual TSetResult Set(TYPath path, TYsonProducer::TPtr producer) 
    {
        if (!ShouldForward(path)) {
            return FallbackService->Set(path, producer);
        }

        TStringStream stream;
        TYsonWriter writer(&stream, TYsonWriter::EFormat::Binary);
        producer->Do(&writer);

        auto request = Proxy->Set();
        request->SetPath(GetForwardPath(path));
        request->SetValue(stream.Str());

        auto response = request->Invoke()->Get();
        if (!response->IsOK()) {
            ythrow TYTreeException() << Sprintf("Error setting an Orchid path (RemoteAddress: %s, RemoteRoot: %s, Path: %s, Error: %s)",
                ~RemoteAddress,
                ~RemoteRoot,
                ~path,
                ~response->GetErrorCode().ToString());
        }

        return TSetResult::CreateDone();
    }

    virtual TRemoveResult Remove(TYPath path)
    {
        if (!ShouldForward(path)) {
            return FallbackService->Remove(path);
        }

        auto request = Proxy->Remove();
        request->SetPath(GetForwardPath(path));

        auto response = request->Invoke()->Get();
        if (!response->IsOK()) {
            ythrow TYTreeException() << Sprintf("Error removing an Orchid path (RemoteAddress: %s, RemoteRoot: %s, Path: %s, Error: %s)",
                ~RemoteAddress,
                ~RemoteRoot,
                ~path,
                ~response->GetErrorCode().ToString());
        }

        return TRemoveResult::CreateDone();
    }

    virtual TNavigateResult Navigate(TYPath path)
    {
        UNUSED(path);
        ythrow TYTreeException() << "Navigation is not supported for an Orchid path";
    }

    virtual TLockResult Lock(TYPath path)
    {
        UNUSED(path);
        ythrow TYTreeException() << "Locking is not supported for an Orchid path";
    }

private:
    static bool ShouldForward(TYPath path)
    {
        return !path.empty();
    }

    Stroka GetForwardPath(TYPath path)
    {
        return path == "/" ? RemoteRoot : RemoteRoot + path;
    }

    IYPathService::TPtr FallbackService;
    Stroka RemoteAddress;
    Stroka RemoteRoot;
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
