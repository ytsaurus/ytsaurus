#include "peer_channel.h"
#include "peer_discovery.h"
#include "config.h"

#include <yt/yt/ytlib/hydra/proto/hydra_service.pb.h>

#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/channel_detail.h>
#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NHydra {

using namespace NRpc;
using namespace NRpc::NProto;
using namespace NHydra::NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TPeerKindInjectingChannel
    : public TChannelWrapper
{
public:
    TPeerKindInjectingChannel(
        IChannelPtr underlying,
        EPeerKind kind)
        : TChannelWrapper(std::move(underlying))
        , Kind_(kind)
    { }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        auto* ext = request->Header().MutableExtension(NProto::TPeerKindExt::request_peer_kind_ext);
        ext->set_peer_kind(ToProto(Kind_));

        return TChannelWrapper::Send(
            std::move(request),
            std::move(responseHandler),
            options);
    }

private:
    const EPeerKind Kind_;
};

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreatePeerChannel(
    TPeerConnectionConfigPtr config,
    IChannelFactoryPtr channelFactory,
    EPeerKind kind)
{
    // NB: If there's just a single Hydra peer configured then there will be no actual followers
    // and all read requests must be routed to the leader.
    if (kind == EPeerKind::Follower && config->Addresses && config->Addresses->size() == 1) {
        kind = EPeerKind::Leader;
    }

    auto endpointDescription = Format("%v", kind);
    auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
        .BeginMap()
            .Item("kind").Value(kind)
        .EndMap());

    auto realmChannelFactory = CreateRealmChannelFactory(
        channelFactory,
        config->CellId);

    auto checkPeerState = !config->IgnorePeerState;

    auto channel = CreateBalancingChannel(
        std::move(config),
        std::move(realmChannelFactory),
        endpointDescription,
        std::move(endpointAttributes),
        CreateDefaultPeerDiscovery(checkPeerState
            ? CreateHydraDiscoverRequestHook(kind)
            : nullptr));

    if (checkPeerState) {
        channel = New<TPeerKindInjectingChannel>(std::move(channel), kind);
    }

    return channel;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
