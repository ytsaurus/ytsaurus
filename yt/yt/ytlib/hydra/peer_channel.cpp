#include "peer_channel.h"
#include "config.h"
#include "hydra_service_proxy.h"

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
    auto balancingChannel = CreateBalancingChannel(
        std::move(config),
        std::move(realmChannelFactory),
        std::move(endpointDescription),
        std::move(endpointAttributes),
        BIND([=] (TReqDiscover* request) {
            auto* ext = request->MutableExtension(TPeerKindExt::peer_kind_ext);
            ext->set_peer_kind(static_cast<int>(kind));
        }));
    return balancingChannel;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
