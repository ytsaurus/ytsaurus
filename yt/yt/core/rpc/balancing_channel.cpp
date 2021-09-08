#include "balancing_channel.h"
#include "channel.h"
#include "caching_channel_factory.h"
#include "config.h"
#include "roaming_channel.h"
#include "dynamic_channel_pool.h"
#include "dispatcher.h"
#include "hedging_channel.h"

#include <yt/yt/core/service_discovery/service_discovery.h>

#include <yt/yt/core/concurrency/spinlock.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/net/address.h>

namespace NYT::NRpc {

using namespace NYTree;
using namespace NConcurrency;
using namespace NServiceDiscovery;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBalancingChannelSubprovider)
DECLARE_REFCOUNTED_CLASS(TBalancingChannelProvider)

class TBalancingChannelSubprovider
    : public TRefCounted
{
public:
    TBalancingChannelSubprovider(
        TBalancingChannelConfigPtr config,
        IChannelFactoryPtr channelFactory,
        TString endpointDescription,
        IAttributeDictionaryPtr endpointAttributes,
        TString serviceName,
        TDiscoverRequestHook discoverRequestHook)
        : Config_(std::move(config))
        , EndpointDescription_(endpointDescription)
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Items(*endpointAttributes)
                .Item("service").Value(serviceName)
            .EndMap()))
        , ServiceName_(std::move(serviceName))
        , DiscoverRequestHook_(std::move(discoverRequestHook))
        , Pool_(New<TDynamicChannelPool>(
            Config_,
            std::move(channelFactory),
            EndpointDescription_,
            EndpointAttributes_,
            ServiceName_,
            DiscoverRequestHook_))
    {
        if (Config_->Addresses) {
            ConfigureFromAddresses();
        } else if (Config_->Endpoints) {
            ConfigureFromEndpoints();
        } else {
            // Must not happen for a properly validated configuration.
            Pool_->SetPeerDiscoveryError(TError("No endpoints configured"));
        }
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& request)
    {
        auto hedgingOptions = Config_->HedgingDelay
            ? std::make_optional(
                THedgingChannelOptions{
                    .Delay = *Config_->HedgingDelay,
                    .CancelPrimary = Config_->CancelPrimaryRequestOnHedging
                })
            : std::nullopt;

        return Pool_->GetChannel(request, hedgingOptions);
    }

    void Terminate(const TError& error)
    {
        Pool_->Terminate(error);
    }

private:
    const TBalancingChannelConfigPtr Config_;
    const TString EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;
    const TString ServiceName_;
    const TDiscoverRequestHook DiscoverRequestHook_;

    const TDynamicChannelPoolPtr Pool_;

    IServiceDiscoveryPtr ServiceDiscovery_;
    TPeriodicExecutorPtr EndpointsUpdateExecutor_;

    void ConfigureFromAddresses()
    {
        Pool_->SetPeers(*Config_->Addresses);
    }

    void ConfigureFromEndpoints()
    {
        ServiceDiscovery_ = TDispatcher::Get()->GetServiceDiscovery();
        if (!ServiceDiscovery_) {
            Pool_->SetPeerDiscoveryError(TError("No Service Discovery is configured"));
            return;
        }

        EndpointsUpdateExecutor_ = New<TPeriodicExecutor>(
            TDispatcher::Get()->GetHeavyInvoker(),
            BIND(&TBalancingChannelSubprovider::UpdateEndpoints, MakeWeak(this)),
            Config_->Endpoints->UpdatePeriod);
        EndpointsUpdateExecutor_->Start();
    }

    void UpdateEndpoints()
    {
        ServiceDiscovery_->ResolveEndpoints(
            Config_->Endpoints->Cluster,
            Config_->Endpoints->EndpointSetId)
            .Subscribe(
                BIND(&TBalancingChannelSubprovider::OnEndpointsResolved, MakeWeak(this)));
    }

    void OnEndpointsResolved(const TErrorOr<TEndpointSet>& endpointSetOrError)
    {
        if (!endpointSetOrError.IsOK()) {
            Pool_->SetPeerDiscoveryError(endpointSetOrError);
            return;
        }

        const auto& endpointSet = endpointSetOrError.Value();
        Pool_->SetPeers(AddressesFromEndpointSet(endpointSet));
    }
};

DEFINE_REFCOUNTED_TYPE(TBalancingChannelSubprovider)

////////////////////////////////////////////////////////////////////////////////

class TBalancingChannelProvider
    : public IRoamingChannelProvider
{
public:
    TBalancingChannelProvider(
        TBalancingChannelConfigPtr config,
        IChannelFactoryPtr channelFactory,
        TString endpointDescription,
        IAttributeDictionaryPtr endpointAttributes,
        TDiscoverRequestHook discoverRequestHook)
        : Config_(std::move(config))
        , ChannelFactory_(CreateCachingChannelFactory(channelFactory))
        , DiscoverRequestHook_(std::move(discoverRequestHook))
        , EndpointDescription_(Format("%v%v",
            endpointDescription,
            Config_->Addresses))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("addresses").Value(Config_->Addresses)
                .Items(*endpointAttributes)
            .EndMap()))
    { }

    const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    TNetworkId GetNetworkId() const override
    {
        // NB(psushin): Assume that balanced channels always use default network.
        // This is important for setting ToS level.
        return DefaultNetworkId;
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& request) override
    {
        if (Config_->Addresses && Config_->Addresses->size() == 1) {
            // Disable discovery and balancing when just one address is given.
            // This is vital for jobs since node's redirector is incapable of handling
            // Discover requests properly.
            return MakeFuture(ChannelFactory_->CreateChannel((*Config_->Addresses)[0]));
        } else {
            return GetSubprovider(request->GetService())->GetChannel(request);
        }
    }

    void Terminate(const TError& error) override
    {
        std::vector<TBalancingChannelSubproviderPtr> subproviders;
        {
            auto guard = ReaderGuard(SpinLock_);
            for (const auto& [_, subprovider] : SubproviderMap_) {
                subproviders.push_back(subprovider);
            }
        }

        for (const auto& subprovider : subproviders) {
            subprovider->Terminate(error);
        }
    }

private:
    const TBalancingChannelConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const TDiscoverRequestHook DiscoverRequestHook_;

    const TString EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, SpinLock_);
    THashMap<TString, TBalancingChannelSubproviderPtr> SubproviderMap_;


    TBalancingChannelSubproviderPtr GetSubprovider(const TString& serviceName)
    {
        {
            auto guard = ReaderGuard(SpinLock_);
            auto it = SubproviderMap_.find(serviceName);
            if (it != SubproviderMap_.end()) {
                return it->second;
            }
        }

        {
            auto guard = WriterGuard(SpinLock_);
            auto it = SubproviderMap_.find(serviceName);
            if (it != SubproviderMap_.end()) {
                return it->second;
            }

            auto subprovider = New<TBalancingChannelSubprovider>(
                Config_,
                ChannelFactory_,
                EndpointDescription_,
                EndpointAttributes_,
                serviceName,
                DiscoverRequestHook_);
            YT_VERIFY(SubproviderMap_.emplace(serviceName, subprovider).second);
            return subprovider;
        }
    }

};

DEFINE_REFCOUNTED_TYPE(TBalancingChannelProvider)

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateBalancingChannel(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory,
    TString endpointDescription,
    IAttributeDictionaryPtr endpointAttributes,
    TDiscoverRequestHook discoverRequestHook)
{
    YT_VERIFY(config);
    YT_VERIFY(channelFactory);

    auto channelProvider = New<TBalancingChannelProvider>(
        std::move(config),
        std::move(channelFactory),
        std::move(endpointDescription),
        std::move(endpointAttributes),
        std::move(discoverRequestHook));
    return CreateRoamingChannel(channelProvider);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
