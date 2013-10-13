#include "stdafx.h"
#include "peer_channel.h"
#include "config.h"
#include "peer_discovery.h"
#include "private.h"

#include <core/rpc/roaming_channel.h>
#include <core/rpc/bus_channel.h>
#include <core/rpc/retrying_channel.h>
#include <core/rpc/channel_cache.h>
#include <core/rpc/helpers.h>

#include <core/bus/config.h>
#include <core/bus/tcp_client.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HydraLogger;
static TChannelCache ChannelCache; // TODO(babenko): unite with peer_discovery

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

    auto busChannel = ChannelCache.GetChannel(result.Address);
    auto realmChannel = CreateRealmChannel(busChannel, config->CellGuid);
    return realmChannel;
}

} // namespace

IChannelPtr CreatePeerChannel(
    TPeerDiscoveryConfigPtr config,
    EPeerRole role)
{
    auto roamingChannel = CreateRoamingChannel(
        config->RpcTimeout,
        BIND([=] () -> TFuture< TErrorOr<IChannelPtr> > {
            return DiscoverPeer(config, role).Apply(BIND(
                &OnPeerFound,
                config,
                role));
        }));
    return CreateRetryingChannel(config, roamingChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
