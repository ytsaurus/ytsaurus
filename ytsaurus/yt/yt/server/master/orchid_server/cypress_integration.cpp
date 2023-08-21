#include "cypress_integration.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/node.h>
#include <yt/yt/server/master/cypress_server/virtual.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/orchid/orchid_ypath_service.h>

#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/balancing_channel.h>

namespace NYT::NOrchidServer {

using namespace NCypressServer;
using namespace NObjectClient;
using namespace NYTree;
using namespace NRpc;
using namespace NNodeTrackerClient;
using namespace NObjectServer;
using namespace NOrchid;

////////////////////////////////////////////////////////////////////////////////

struct TOrchidManifest
    : public TRetryingChannelConfig
{
    NYTree::INodePtr RemoteAddresses;
    TString RemoteRoot;
    TDuration Timeout;

    REGISTER_YSON_STRUCT(TOrchidManifest);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("remote_addresses", &TThis::RemoteAddresses);
        registrar.Parameter("remote_root", &TThis::RemoteRoot)
            .Default("/");
        registrar.Parameter("timeout", &TThis::Timeout)
            .Default(TDuration::Seconds(60));

        registrar.Postprocessor([] (TThis* config) {
            config->RetryAttempts = 1;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TOrchidManifest)
DECLARE_REFCOUNTED_STRUCT(TOrchidManifest)

////////////////////////////////////////////////////////////////////////////////

//! Create orchid service obtaining remote endpoint from the manifest stored in #owningNode's custom attributes.
IYPathServicePtr CreateService(
    const INodeChannelFactoryPtr& nodeChannelFactory,
    const INodePtr& owningNode)
{
    try {

        auto manifest = New<TOrchidManifest>();

        // NB: an attempt to obtain orchid manifest from #owningNode->Attributes() would lead
        // to an infinite recursion because attribute building would lead to execution of this method.
        // Actually it is true that owningNode is always of type TVirtualNodeProxy.
        auto objectNode = DynamicPointerCast<TObjectProxyBase>(owningNode);
        YT_VERIFY(objectNode);

        auto manifestNode = ConvertToNode(objectNode->GetCustomAttributes());

        try {
            manifest->Load(manifestNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing Orchid manifest")
                << ex;
        }

        IChannelPtr channel;

        switch (manifest->RemoteAddresses->GetType()) {
            case ENodeType::Map:
                channel = CreateRetryingChannel(
                    manifest,
                    nodeChannelFactory->CreateChannel(ConvertTo<TAddressMap>(manifest->RemoteAddresses)));
                break;

            case ENodeType::List: {
                auto channelConfig = New<TBalancingChannelConfig>();
                channelConfig->Addresses = ConvertTo<std::vector<TString>>(manifest->RemoteAddresses);
                auto endpointDescription = TString("Orchid@");
                auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
                    .BeginMap()
                        .Item("orchid").Value(true)
                    .EndMap());
                channel = CreateRetryingChannel(
                    manifest,
                    CreateBalancingChannel(
                        std::move(channelConfig),
                        nodeChannelFactory,
                        std::move(endpointDescription),
                        std::move(endpointAttributes)));
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Invalid remote addresses type %lv", manifest->RemoteAddresses->GetType());
        }

        return CreateOrchidYPathService(TOrchidOptions{
            .Channel = std::move(channel),
            .RemoteRoot = std::move(manifest->RemoteRoot),
            .Timeout = manifest->Timeout,
        });
    } catch (const std::exception& ex) {
        // Throwing errors out of this function is bad since it would break get-with-attributes
        // applied to an orchid node parent. Pass it to orchid YPath service instead.
        return CreateOrchidYPathService(TOrchidOptions{
            .Error = TError(ex),
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateOrchidTypeHandler(NCellMaster::TBootstrap* bootstrap)
{
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::Orchid,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return CreateService(bootstrap->GetNodeChannelFactory(), std::move(owningNode));
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrchidServer
