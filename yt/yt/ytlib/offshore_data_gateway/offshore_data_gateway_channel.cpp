#include "offshore_data_gateway_channel.h"

#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/rpc/dynamic_channel_pool.h>
#include <yt/yt/core/rpc/dynamic_channel_pool_provider.h>
#include <yt/yt/core/rpc/peer_discovery.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NOffshoreDataGateway {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TOffshoreDataGatewayChannelManager
    : public IOffshoreDataGatewayChannelManager
{
public:
    TOffshoreDataGatewayChannelManager(
        TOffshoreDataGatewayChannelConfigPtr config,
        IChannelFactoryPtr channelFactory,
        NApi::NNative::IConnectionPtr connection)
        : Logger(OffshoreDataGatewayClientLogger())
        , Config_(std::move(config))
        , Connection_(connection)
        , EndpointDescription_(Format("OffshoreDataGateway@%v", connection->GetClusterName()))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("offshore_data_gateway").Value(true)
                .Item("cluster").Value(connection->GetClusterName())
            .EndMap()))
        , ChannelPool_(New<TDynamicChannelPool>(
            New<TDynamicChannelPoolConfig>(),
            std::move(channelFactory),
            EndpointDescription_,
            EndpointAttributes_,
            // NB: Offshore data gateway registers only DataNodeService for now,
            // therefore dynamic channel pool's discovery should go there.
            "DataNodeService",
            CreateDefaultPeerDiscovery()))
        , CachedNonStickyChannel_(CreateChannel(/*sticky*/ false))
    {
        if (Config_->DataGatewayUpdatePeriod) {
            YT_TLOG_DEBUG("Start periodic offshore data gateway list updater")
                .With("UpdatePeriod", *Config_->DataGatewayUpdatePeriod);

            RefreshExecutor_ = New<TPeriodicExecutor>(
                // TODO(ponasenko-rs): Use better specified invoker for refresh. See YT-29080.
                GetCurrentInvoker(),
                BIND(&TOffshoreDataGatewayChannelManager::Refresh, MakeWeak(this)),
                *Config_->DataGatewayUpdatePeriod);
        }
    }

    void InitializeRefCounted()
    {
        if (RefreshExecutor_) {
            RefreshExecutor_->Start();
        }
    }

    IChannelPtr GetStickyChannel() override
    {
        return CreateChannel(/*sticky*/ true);
    }

    const IChannelPtr& GetNonStickyChannel() override
    {
        return CachedNonStickyChannel_;
    }

private:
    const NLogging::TLogger Logger;

    const TOffshoreDataGatewayChannelConfigPtr Config_;

    const TWeakPtr<NApi::NNative::IConnection> Connection_;

    const std::string EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;

    const TDynamicChannelPoolPtr ChannelPool_;

    const IChannelPtr CachedNonStickyChannel_;

    TPeriodicExecutorPtr RefreshExecutor_;

    IChannelPtr CreateChannel(bool sticky)
    {
        auto provider = sticky
            ? CreateStickyDynamicChannelPoolProvider(
                ChannelPool_,
                EndpointDescription_,
                EndpointAttributes_)
            : CreateDynamicChannelPoolProvider(
                ChannelPool_,
                EndpointDescription_,
                EndpointAttributes_);

        return WrapChannel(CreateRoamingChannel(std::move(provider)));
    }

    IChannelPtr WrapChannel(IChannelPtr channel)
    {
        // TODO(achulkov2): Think about this properly. For now, I think we need these retries.
        channel = CreateRetryingChannel(Config_, std::move(channel));
        channel = CreateDefaultTimeoutChannel(std::move(channel), Config_->RpcTimeout);

        return channel;
    }

    TFuture<TYPathProxy::TRspListPtr> ListOffshoreDataGatewayInstances(const NApi::NNative::IConnectionPtr& connection)
    {
        NApi::TMasterReadOptions masterReadOptions{
            .ReadFrom = NApi::EMasterChannelKind::MasterSideCache,
        };

        if (Config_->Testing->BypassCache) {
            masterReadOptions.ReadFrom = NApi::EMasterChannelKind::Follower;
        }

        auto req = TYPathProxy::List("//sys/offshore_data_gateways/instances");
        SetCachingHeader(req, connection, masterReadOptions);

        TObjectServiceProxy proxy(
            connection,
            masterReadOptions.ReadFrom,
            PrimaryMasterCellTagSentinel,
            connection->GetStickyGroupSizeCache());

        return proxy.Execute(req);
    }

    void Refresh()
    {
        auto connection = Connection_.Lock();
        if (!connection) {
            return;
        }

        ListOffshoreDataGatewayInstances(connection)
            .Subscribe(BIND([this, this_ = MakeStrong(this)] (
                const TErrorOr<TYPathProxy::TRspListPtr>& rsp)
            {
                if (!rsp.IsOK()) {
                    YT_TLOG_WARNING("Failed to refresh offshore data gateways list")
                        .With(rsp);
                    return;
                }

                auto addresses = ConvertTo<std::vector<std::string>>(
                    TYsonString(rsp.ValueOrCrash()->value()));

                YT_TLOG_DEBUG("Offshore data gateways list refreshed")
                    .With("Addresses", addresses);
                ChannelPool_->SetPeers(addresses);
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

IOffshoreDataGatewayChannelManagerPtr CreateOffshoreDataGatewayChannelManager(
    const TOffshoreDataGatewayChannelConfigPtr& config,
    IChannelFactoryPtr channelFactory,
    NApi::NNative::IConnectionPtr connection)
{
    return New<TOffshoreDataGatewayChannelManager>(
        config,
        std::move(channelFactory),
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
