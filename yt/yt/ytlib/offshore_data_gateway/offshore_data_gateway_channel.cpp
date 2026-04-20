#include "offshore_data_gateway_channel.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/rpc_helpers.h>

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

namespace NYT::NOffshoreDataGateway {

using namespace NConcurrency;
using namespace NRpc;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TOffshoreDataGatewayChannelProvider
    : public IRoamingChannelProvider
{
public:
    TOffshoreDataGatewayChannelProvider(
        TOffshoreDataGatewayChannelConfigPtr config,
        IChannelFactoryPtr channelFactory,
        NApi::NNative::IConnectionPtr connection)
        : Config_(std::move(config))
        , ChannelFactory_(std::move(channelFactory))
        , Connection_(connection)
        , EndpointDescription_(Format("OffshoreDataGateway@%v", connection->GetClusterName()))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("offshore_data_gateway").Value(true)
                .Item("cluster").Value(connection->GetClusterName())
            .EndMap()))
        , RefreshChannelExecutor_(New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TOffshoreDataGatewayChannelProvider::RefreshChannel, MakeWeak(this)),
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
    const TOffshoreDataGatewayChannelConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const TWeakPtr<NApi::NNative::IConnection> Connection_;

    const std::string EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;

    NConcurrency::TPeriodicExecutorPtr RefreshChannelExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    IChannelPtr CachedChannel_;

    TFuture<IChannelPtr> DoCreateChannel()
    {
        auto connection = Connection_.Lock();
        if (!connection) {
            return MakeFuture<IChannelPtr>(TError(NYT::EErrorCode::Canceled, "Connection destroyed"));
        }

        // TODO(pavel-bash): Maybe later it'll be a good idea to include those into Config.
        NApi::TMasterReadOptions masterReadOptions;
        masterReadOptions.ReadFrom = NApi::EMasterChannelKind::MasterSideCache;

        auto req = TYPathProxy::List("//sys/offshore_data_gateways/instances");
        SetCachingHeader(req, connection, masterReadOptions);

        TObjectServiceProxy proxy(
            connection,
            masterReadOptions.ReadFrom,
            PrimaryMasterCellTagSentinel,
            connection->GetStickyGroupSizeCache());
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

IChannelPtr CreateOffshoreDataGatewayChannel(
    const TOffshoreDataGatewayChannelConfigPtr& config,
    IChannelFactoryPtr channelFactory,
    NApi::NNative::IConnectionPtr connection)
{
    auto channelProvider = New<TOffshoreDataGatewayChannelProvider>(
        config,
        std::move(channelFactory),
        std::move(connection));
    auto channel = CreateRoamingChannel(channelProvider);

    // TODO(achulkov2): Think about this properly. For now, I think we need these retries.
    channel = CreateRetryingChannel(config, channel);
    channel = CreateDefaultTimeoutChannel(channel, config->RpcTimeout);

    return channel;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
