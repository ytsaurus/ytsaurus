#pragma once

#include "channel_factory.h"
#include "config.h"
#include "connection.h"

#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/channel.h>
#include <yt/yt/core/rpc/grpc/channel.h>
#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/random/random.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public IConnection
{
public:
    TConnection(
        TConnectionConfigPtr config,
        NLogging::TLogger logger,
        IOrmPeerDiscoveryPtr peerDiscovery,
        NRpc::TBalancingChannelConfigPtr balancingChannelConfig,
        NRpc::IChannelFactoryPtr channelFactory)
        : Config_(std::move(config))
        , Logger(std::move(logger))
        , PeerDiscovery_(peerDiscovery)
        , ChannelFactory_(NRpc::CreateCachingChannelFactory(std::move(channelFactory), Config_->ChannelTtl))
        , Channel_(NRpc::CreateBalancingChannel(
            std::move(balancingChannelConfig),
            ChannelFactory_,
            /*endpointDescription*/ "",
            /*endpointAttributes*/ NYTree::CreateEphemeralAttributes(),
            peerDiscovery))
    { }

    NRpc::IChannelPtr GetChannel(bool retrying) override
    {
        if (retrying) {
            return NRpc::CreateRetryingChannel(Config_, Channel_);
        }

        return Channel_;
    }

    NRpc::IChannelPtr GetChannel(TMasterInstanceTag instanceTag, bool retrying) override
    {
        if (auto address = PeerDiscovery_->GetAddress(instanceTag)) {
            auto channel = ChannelFactory_->CreateChannel(*address);
            if (retrying) {
                return NRpc::CreateRetryingChannel(Config_, std::move(channel));
            }
            return channel;
        }

        return {};
    }

    TDuration GetRequestTimeout() const override
    {
        return Config_->RequestTimeout;
    }

    IOrmPeerDiscoveryPtr GetPeerDiscovery() const override
    {
        return PeerDiscovery_;
    }

private:
    const TConnectionConfigPtr Config_;
    const NLogging::TLogger Logger;
    const IOrmPeerDiscoveryPtr PeerDiscovery_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const NRpc::IChannelPtr Channel_;
};

////////////////////////////////////////////////////////////////////////////////

NRpc::TBalancingChannelConfigPtr MakeBalancingChannelConfig(TConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TObjectServiceProxy, class TDiscoveryServiceProxy>
IConnectionPtr CreateConnection(
    TConnectionConfigPtr config,
    NLogging::TLogger logger)
{
    auto peerDiscovery = CreateOrmPeerDiscovery<TDiscoveryServiceProxy>(
        logger,
        config->DiscoveryAddress,
        config->DiscoveryIP6Address);

    auto channelFactory = CreateChannelFactory(
        config->GrpcChannelTemplate,
        peerDiscovery,
        /*discoveryAddress*/ config->DiscoveryAddress,
        /*useIP6Addresses*/ config->DiscoveryIP6Address.has_value());

    return New<NDetail::TConnection>(
        std::move(config),
        std::move(logger),
        std::move(peerDiscovery),
        NDetail::MakeBalancingChannelConfig(config),
        std::move(channelFactory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
