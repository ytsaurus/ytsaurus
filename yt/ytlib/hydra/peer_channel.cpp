#include "stdafx.h"
#include "peer_channel.h"
#include "config.h"

#include <core/rpc/retrying_channel.h>
#include <core/rpc/balancing_channel.h>
#include <core/rpc/helpers.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreatePeerChannel(
    TPeerDiscoveryConfigPtr config,
    IChannelFactoryPtr channelFactory,
    EPeerRole /*role*/)
{
    auto realmChannelFactory = CreateRealmChannelFactory(channelFactory, config->CellGuid);
    auto balancingChannel = CreateBalancingChannel(config, realmChannelFactory);
    auto retryingChannel = CreateRetryingChannel(config, balancingChannel);
    retryingChannel->SetDefaultTimeout(config->RpcTimeout);
    return retryingChannel;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
