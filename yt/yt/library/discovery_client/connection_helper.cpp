#include "connection_helper.h"

#include "discovery_client.h"

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

TDiscoveryConnectionConfigPtr FetchDiscoveryConnectionConfig(IClientPtr client)
{
    TGetOptions options;
    options.ReadFrom(EMasterReadKind::Cache);

    auto discoveryConnectionNode = client->Get(
        "//sys/@cluster_connection/discovery_connection", options);
    auto yson = NodeToYsonString(discoveryConnectionNode);
    return NYTree::ConvertTo<TDiscoveryConnectionConfigPtr>(
        NYson::TYsonStringBuf(yson));
}

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelFactoryPtr CreateDefaultDiscoveryChannelFactory()
{
    return NRpc::CreateCachingChannelFactory(
        NRpc::NBus::CreateTcpBusChannelFactory(New<NBus::NTcp::TBusConfig>()));
}

////////////////////////////////////////////////////////////////////////////////

IDiscoveryClientPtr CreateDiscoveryClientFromYtProxy(
    IClientPtr client,
    TDiscoveryClientConfigPtr clientConfig,
    NRpc::IChannelFactoryPtr channelFactory)
{
    auto discoveryConnection = FetchDiscoveryConnectionConfig(client);
    return CreateDiscoveryClient(
        std::move(discoveryConnection),
        std::move(clientConfig),
        std::move(channelFactory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
