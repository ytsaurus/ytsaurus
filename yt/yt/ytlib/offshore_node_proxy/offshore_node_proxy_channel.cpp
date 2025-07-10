#include "offshore_node_proxy_channel.h"
#include "config.h"

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/roaming_channel.h>
#include <yt/yt/core/rpc/balancing_channel.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NOffshoreNodeProxy {

// TODO(achulkov2): Only leave necessary declarations.
using namespace NBus;
using namespace NConcurrency;
using namespace NRpc;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

class TOffshoreNodeProxyChannelProvider
    : public IRoamingChannelProvider
{
public:
    TOffshoreNodeProxyChannelProvider(
        TOffshoreNodeProxyChannelConfigPtr config,
        IChannelFactoryPtr channelFactory,
        IChannelPtr masterChannel)
        : Config_(std::move(config))
        , ChannelFactory_(std::move(channelFactory))
        , MasterChannel_(std::move(masterChannel))
        , EndpointDescription_(Format("OffshoreNodeProxy@%v",
            MasterChannel_->GetEndpointDescription()))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("offshore_node_proxy").Value(true)
                .Items(MasterChannel_->GetEndpointAttributes())
            .EndMap()))
        , RefreshChannelExecutor_(New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TOffshoreNodeProxyChannelProvider::RefreshChannel, MakeWeak(this)),
            TDuration::Seconds(1)))
    {
        RefreshChannelExecutor_->Start();
    }

    const std::string& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    TFuture<IChannelPtr> GetChannel() override
    {
        {
            auto guard = Guard(SpinLock_);
            if (CachedChannel_) {
                return MakeFuture(CachedChannel_);
            }
        }

        return DoCreateChannel();
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel(std::string /*serviceName*/) override
    {
        return GetChannel();
    }

    void Terminate(const TError& error) override
    {
        auto guard = Guard(SpinLock_);
        if (CachedChannel_) {
            CachedChannel_->Terminate(error);
        }
    }

private:
    const TOffshoreNodeProxyChannelConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const IChannelPtr MasterChannel_;

    const std::string EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;

    NConcurrency::TPeriodicExecutorPtr RefreshChannelExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    IChannelPtr CachedChannel_;

    TFuture<IChannelPtr> DoCreateChannel()
    {
        auto proxy = TObjectServiceProxy::FromDirectMasterChannel(MasterChannel_);

        auto req = TYPathProxy::List("//sys/offshore_node_proxies/instances");
        return proxy.Execute(req)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TYPathProxy::TRspListPtr& rsp) -> IChannelPtr {
                auto addresses = ConvertTo<std::vector<std::string>>(TYsonString(rsp->value()));

                auto balancingChannelConfig = New<TBalancingChannelConfig>();
                balancingChannelConfig->Addresses.emplace(std::move(addresses));

                // This channel already detects failures internally.
                auto channel = CreateBalancingChannel(
                    balancingChannelConfig,
                    ChannelFactory_,
                    EndpointDescription_,
                    EndpointAttributes_);

                {
                    auto guard = Guard(SpinLock_);
                    CachedChannel_ = channel;
                }

                return channel;
            }));
    }

    void RefreshChannel()
    {
        YT_UNUSED_FUTURE(DoCreateChannel());
    }
};

IChannelPtr CreateOffshoreNodeProxyChannel(
    const TOffshoreNodeProxyChannelConfigPtr& config,
    IChannelFactoryPtr channelFactory,
    IChannelPtr masterChannel)
{
    auto channelProvider = New<TOffshoreNodeProxyChannelProvider>(
        config,
        std::move(channelFactory),
        std::move(masterChannel));
    auto channel = CreateRoamingChannel(channelProvider);

    // TODO(achulkov2): Think about this properly. For now, I think we need these retries.
    channel = CreateRetryingChannel(config, channel);
    channel = CreateDefaultTimeoutChannel(channel, config->RpcTimeout);

    return channel;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
