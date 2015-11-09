#include "stdafx.h"
#include "peer_channel.h"
#include "config.h"
#include "hydra_service_proxy.h"

#include <core/rpc/balancing_channel.h>
#include <core/rpc/helpers.h>
#include <core/rpc/rpc.pb.h>

#include <core/ytree/fluent.h>

#include <ytlib/hydra/hydra_service.pb.h>

namespace NYT {
namespace NHydra {

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
        config,
        realmChannelFactory,
        endpointDescription,
        *endpointAttributes,
        BIND([=] (TReqDiscover* request) {
            auto* ext = request->MutableExtension(TPeerKindExt::peer_kind_ext);
            ext->set_peer_kind(static_cast<int>(kind));
        }));
    return balancingChannel;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
