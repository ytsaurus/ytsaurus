#include "dynamic_channel_pool.h"
#include "private.h"

#include <yt/core/rpc/client.h>
#include <yt/core/rpc/channel.h>
#include <yt/core/rpc/roaming_channel.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NRpcProxy {

using namespace NApi;
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;

static const auto& Logger = RpcProxyClientLogger;

DEFINE_REFCOUNTED_TYPE(TDynamicChannelPool)
DEFINE_REFCOUNTED_TYPE(TExpiringChannel)

////////////////////////////////////////////////////////////////////////////////

TFuture<NRpc::IChannelPtr> TExpiringChannel::GetChannel()
{
    return Channel_;
}

bool TExpiringChannel::IsExpired()
{
    return IsExpired_;
}

////////////////////////////////////////////////////////////////////////////////

class TProxyChannelProvider
    : public IRoamingChannelProvider
{
public:
    TProxyChannelProvider(
        TDynamicChannelPoolPtr pool)
        : Pool_(std::move(pool))
        , EndpointDescription_("RpcProxy")
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("rpc_proxy").Value(true)
            .EndMap()))
    { }

    ~TProxyChannelProvider()
    {
        if (ChannelCookie_) {
            Pool_->EvictChannel(ChannelCookie_);
        }
    }

    virtual const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    virtual TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        auto guard = Guard(SpinLock_);
        if (Terminated_) {
            return MakeFuture<IChannelPtr>(TError("Channel is terminated"));
        }
        if (!ChannelCookie_ || ChannelCookie_->IsExpired()) {
            ChannelCookie_ = Pool_->CreateChannel();
        }
        
        return ChannelCookie_->GetChannel();
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        TFuture<IChannelPtr> channel;
        {
            auto guard = Guard(SpinLock_);
            Terminated_ = true;
            if (ChannelCookie_) {
                channel = ChannelCookie_->GetChannel();
            }
        }

        if (channel) {
            return channel.Apply(BIND([error] (const TErrorOr<IChannelPtr>& channel) {
                return channel.ValueOrThrow()->Terminate(error);
            }));
        } else {
            return VoidFuture;
        }
    }

private:
    const TDynamicChannelPoolPtr Pool_;

    const TString EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;

    TSpinLock SpinLock_;
    TExpiringChannelPtr ChannelCookie_;
    bool Terminated_ = false;
};

////////////////////////////////////////////////////////////////////////////////

TDynamicChannelPool::TDynamicChannelPool(
    IChannelFactoryPtr channelFactory)
    : ChannelFactory_(std::move(channelFactory))
{ }


TExpiringChannelPtr TDynamicChannelPool::CreateChannel()
{
    auto channelCookie = New<TExpiringChannel>();
    TFuture<std::vector<TString>> addresses;
    {
        auto guard = Guard(SpinLock_);
        addresses = Addresses_;
    }
    
    channelCookie->Channel_ = addresses
        .Apply(BIND([this, this_ = MakeStrong(this), channelCookie] (const TErrorOr<std::vector<TString>>& addressesOrError) {
            if (!addressesOrError.IsOK()) {
                EvictChannel(channelCookie);
            }
        
            const auto& addresses = addressesOrError.ValueOrThrow();
            YCHECK(!addresses.empty());
            auto address = addresses[RandomNumber(addresses.size())];

            auto channel = ChannelFactory_->CreateChannel(address);
            channel = CreateFailureDetectingChannel(
                std::move(channel),
                BIND([pool = MakeWeak(this), cookie = MakeWeak(channelCookie)] (IChannelPtr channel) {
                    auto strongPool = pool.Lock();
                    auto strongCookie = cookie.Lock();
                    if (strongPool && strongCookie && !strongCookie->IsExpired()) {
                        strongPool->EvictChannel(strongCookie);
                    }
                }));
            
            auto guard = Guard(SpinLock_);
            if (Terminated_) {
                channel->Terminate(TError("Channel pool is terminated"));
                THROW_ERROR_EXCEPTION("Channel pool is terminated");
            }

            if (channelCookie->IsExpired_) {
                THROW_ERROR_EXCEPTION("Channel is expired");
            }

            channelCookie->Address_ = address;
            channelCookie->IsActive_ = true;
            ActiveChannels_[address].insert(channelCookie);
            return channel;
        }));
    
    return channelCookie;
}

void TDynamicChannelPool::EvictChannel(TExpiringChannelPtr channelCookie)
{
    auto guard = Guard(SpinLock_);
    if (channelCookie->IsActive_) {
        channelCookie->IsExpired_ = true;

        auto it = ActiveChannels_.find(channelCookie->Address_);
        if (it == ActiveChannels_.end()) {
            return;
        }
        it->second.erase(channelCookie);
    }
}

TErrorOr<IChannelPtr> TDynamicChannelPool::TryCreateChannel()
{
    auto guard = Guard(SpinLock_);
    if (Addresses_ && Addresses_.IsSet() && Addresses_.Get().IsOK()) {
        if (Terminated_) {
            return TError("Channel pool is terminated");
        }

        const auto& addresses = Addresses_.Get().Value();
        YCHECK(!addresses.empty());
        auto address = addresses[RandomNumber(addresses.size())];
        return ChannelFactory_->CreateChannel(address);
    } else {
        return TError("Address list is invalid");
    }
}

void TDynamicChannelPool::Terminate()
{
    THashMap<TString, THashSet<TExpiringChannelPtr>> activeChannels;
    {
        auto guard = Guard(SpinLock_);
        Terminated_ = true;
        activeChannels = std::move(ActiveChannels_);
    }

    auto error = TError("Channel pool is terminated");
    for (auto& address : activeChannels) {
        for (auto& channel : address.second) {
            YCHECK(channel->Channel_.IsSet() && channel->Channel_.Get().IsOK());
            auto terminateError = WaitFor(channel->Channel_.Get().Value()->Terminate(error));
            if (!terminateError.IsOK()) {
                LOG_ERROR(terminateError, "Error while terminating channel pool");
            }
        }
    }
}

void TDynamicChannelPool::SetAddressList(TFuture<std::vector<TString>> addresses)
{
    {
        auto guard = Guard(SpinLock_);
        if (!Addresses_ || (Addresses_.IsSet() && !Addresses_.Get().IsOK())) {
            Addresses_ = addresses;
        }
    }

    addresses.Apply(BIND(&TDynamicChannelPool::OnAddressListChange, MakeStrong(this)));
}

void TDynamicChannelPool::OnAddressListChange(const TErrorOr<std::vector<TString>>& addressesOrError)
{
    if (!addressesOrError.IsOK()) {
        LOG_ERROR(addressesOrError, "Address list update failed");
        return;
    }

    auto addresses = addressesOrError.Value();
    if (addresses.empty()) {
        LOG_ERROR("Address list is empty");
        return;
    }

    std::sort(addresses.begin(), addresses.end());

    std::vector<TString> expiredAddresses;
    std::vector<TExpiringChannelPtr> expiredChannels;
    {
        auto guard = Guard(SpinLock_);
        for (auto&& address : ActiveChannels_) {
            if (std::binary_search(addresses.begin(), addresses.end(), address.first)) {
                continue;
            }

            expiredAddresses.push_back(address.first);
            expiredChannels.insert(expiredChannels.end(), address.second.begin(), address.second.end());
        }

        for (auto&& address : expiredAddresses) {
            ActiveChannels_.erase(address);
        }

        Addresses_ = MakeFuture(addresses);
    }

    for (auto&& address : expiredAddresses) {
        LOG_ERROR("Proxy is anavailable (Address: %s)", address);
    }
}

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateDynamicChannel(
    TDynamicChannelPoolPtr pool)
{
    auto provider = New<TProxyChannelProvider>(std::move(pool));
    return CreateRoamingChannel(std::move(provider));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
