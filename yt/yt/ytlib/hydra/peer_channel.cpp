#include "peer_channel.h"
#include "peer_discovery.h"
#include "config.h"

#include <yt/yt/ytlib/hydra/proto/hydra_service.pb.h>

#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NHydra {

using namespace NRpc;
using namespace NRpc::NProto;
using namespace NHydra::NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreatePeerChannel(
    TPeerConnectionConfigPtr config,
    IChannelFactoryPtr channelFactory,
    EPeerKind kind)
{
    auto realmChannelFactory = CreateRealmChannelFactory(
        channelFactory,
        config->CellId);
    auto endpointDescription = Format("%v", kind);
    auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
        .BeginMap()
            .Item("kind").Value(kind)
        .EndMap());
    auto checkPeerState = !config->IgnorePeerState;
    return CreateBalancingChannel(
        std::move(config),
        std::move(realmChannelFactory),
        std::move(endpointDescription),
        std::move(endpointAttributes),
        CreateDefaultPeerDiscovery(checkPeerState
            ? CreateHydraDiscoverRequestHook(kind)
            : nullptr));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
