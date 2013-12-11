#include "stdafx.h"
#include "peer_channel.h"
#include "config.h"
#include "peer_discovery.h"
#include "private.h"

#include <core/rpc/roaming_channel.h>
#include <core/rpc/bus_channel.h>
#include <core/rpc/retrying_channel.h>
#include <core/rpc/helpers.h>

#include <core/bus/config.h>
#include <core/bus/tcp_client.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

Stroka FormatRole(EPeerRole role)
{
    switch (role) {
        case EPeerRole::Any:      return "peer";
        case EPeerRole::Leader:   return "leader";
        case EPeerRole::Follower: return "follower";
        default:                  YUNREACHABLE();
    }
}

TErrorOr<IChannelPtr> OnPeerFound(
    TPeerDiscoveryConfigPtr config,
    IChannelFactoryPtr channelFactory,
    EPeerRole role,
    TErrorOr<TPeerDiscoveryResult> resultOrError)
{
    auto formattedRole = FormatRole(role);

    if (!resultOrError.IsOK()) {
        return TError(
            NRpc::EErrorCode::Unavailable,
            "No %s found",
            ~formattedRole);
    }

    const auto result = resultOrError.GetValue();

    LOG_INFO("Found %s at %s",
        ~formattedRole,
        ~result.Address);

    auto busChannel = channelFactory->CreateChannel(result.Address);
    auto realmChannel = CreateRealmChannel(busChannel, config->CellGuid);
    return realmChannel;
}

} // namespace

IChannelPtr CreatePeerChannel(
    TPeerDiscoveryConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    EPeerRole role)
{
    auto roamingChannel = CreateRoamingChannel(
        BIND([=] () -> TFuture< TErrorOr<IChannelPtr> > {
            return
                DiscoverPeer(
                    config,
                    channelFactory,
                    role)
                .Apply(BIND(
                    &OnPeerFound,
                    config,
                    channelFactory,
                    role));
        }));
    roamingChannel->SetDefaultTimeout(config->RpcTimeout);
    return CreateRetryingChannel(config, roamingChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
