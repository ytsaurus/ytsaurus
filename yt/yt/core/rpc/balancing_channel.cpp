#include "balancing_channel.h"
#include "channel.h"
#include "caching_channel_factory.h"
#include "config.h"
#include "roaming_channel.h"
#include "dynamic_channel_pool.h"

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NRpc {

// using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

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
        Pool_->SetPeers(Config_->Addresses);
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& request)
    {
        return Pool_->GetChannel(request);
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

    virtual const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    virtual TNetworkId GetNetworkId() const override
    {
        // NB(psushin): Assume that balanced channels always use default network.
        // This is important for setting ToS level.
        return DefaultNetworkId;
    }

    virtual TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& request) override
    {
        if (Config_->Addresses.size() == 1) {
            // Disable discovery and balancing when just one address is given.
            // This is vital for jobs since node's redirector is incapable of handling
            // Discover requests properly.
            return MakeFuture(ChannelFactory_->CreateChannel(Config_->Addresses[0]));
        } else {
            return GetSubprovider(request->GetService())->GetChannel(request);
        }
    }

    virtual void Terminate(const TError& error)
    {
        std::vector<TBalancingChannelSubproviderPtr> subproviders;
        {
            TReaderGuard guard(SpinLock_);
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

    mutable TReaderWriterSpinLock SpinLock_;
    THashMap<TString, TBalancingChannelSubproviderPtr> SubproviderMap_;


    TBalancingChannelSubproviderPtr GetSubprovider(const TString& serviceName)
    {
        {
            TReaderGuard guard(SpinLock_);
            auto it = SubproviderMap_.find(serviceName);
            if (it != SubproviderMap_.end()) {
                return it->second;
            }
        }

        {
            TWriterGuard guard(SpinLock_);
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
