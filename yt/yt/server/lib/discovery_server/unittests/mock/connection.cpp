#include "connection.h"

#include <yt/yt/ytlib/discovery_client/discovery_client.h>
#include <yt/yt/ytlib/discovery_client/member_client.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

using namespace NDiscoveryClient;
using namespace NDiscoveryServer;

TMockDistributedThrottlerConnection::TMockDistributedThrottlerConnection(
    NDiscoveryClient::TDiscoveryConnectionConfigPtr config)
    : Config_(std::move(config))
{ }
NDiscoveryClient::IDiscoveryClientPtr TMockDistributedThrottlerConnection::CreateDiscoveryClient(
    NDiscoveryClient::TDiscoveryClientConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory)
{
    return NDiscoveryClient::CreateDiscoveryClient(
        Config_,
        std::move(config),
        std::move(channelFactory));
}

NDiscoveryClient::IMemberClientPtr TMockDistributedThrottlerConnection::CreateMemberClient(
    NDiscoveryClient::TMemberClientConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    TString id,
    TString groupId)
{
    return NDiscoveryClient::CreateMemberClient(
        Config_,
        std::move(config),
        std::move(channelFactory),
        std::move(invoker),
        std::move(id),
        std::move(groupId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
