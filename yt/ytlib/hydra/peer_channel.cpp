#include "stdafx.h"
#include "peer_channel.h"
#include "config.h"

#include <core/rpc/balancing_channel.h>
#include <core/rpc/helpers.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateLeaderChannel(
    TPeerConnectionConfigPtr config,
    IChannelFactoryPtr channelFactory)
{
    auto realmChannelFactory = CreateRealmChannelFactory(
    	channelFactory,
    	config->CellId);
    auto balancingChannel = CreateBalancingChannel(
    	config,
    	realmChannelFactory);
    return balancingChannel;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
