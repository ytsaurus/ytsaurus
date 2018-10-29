#include "dynamic_channel_pool.h"
#include "private.h"
#include "config.h"

#include <yt/core/rpc/client.h>
#include <yt/core/rpc/channel.h>
#include <yt/core/rpc/roaming_channel.h>

#include <yt/core/utilex/random.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <util/random/shuffle.h>

namespace NYT {
namespace NApi {
namespace NRpcProxy {

using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;

static const auto& Logger = RpcProxyClientLogger;

DEFINE_REFCOUNTED_TYPE(TDynamicChannelPool)

////////////////////////////////////////////////////////////////////////////////

class TProxyChannelProvider
    : public IRoamingChannelProvider
{
public:
    TProxyChannelProvider(
        TDynamicChannelPoolPtr pool,
        bool sticky)
        : Pool_(std::move(pool))
        , Sticky_(sticky)
        , EndpointDescription_("RpcProxy")
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("rpc_proxy").Value(true)
            .EndMap()))
    { }

    ~TProxyChannelProvider()
    { }

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
        if (!Sticky_) {
            return Pool_->GetRandomChannel();
        } else {
            auto guard = Guard(SpinLock_);
            if (!Channel_) {
                Channel_ = Pool_->GetRandomChannel();
            }
            return Channel_;
        }
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        return VoidFuture;
    }

private:
    const TDynamicChannelPoolPtr Pool_;
    const bool Sticky_;

    const TString EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;

    TSpinLock SpinLock_;
    TFuture<IChannelPtr> Channel_;
};

////////////////////////////////////////////////////////////////////////////////

bool TDynamicChannelPool::TChannelSlot::IsWarm(TInstant now)
{
    auto channelWarmupTime = TDuration::Seconds(1);
    return CreationTime + channelWarmupTime < now;
}

TDynamicChannelPool::TDynamicChannelPool(
    IChannelFactoryPtr channelFactory,
    TConnectionConfigPtr config)
    : ChannelFactory_(std::move(channelFactory))
    , Config_(std::move(config))
{
    for (int i = 0; i < Config_->ChannelPoolSize; ++i) {
        Slots_.push_back(New<TChannelSlot>());
    }
}

TFuture<IChannelPtr> TDynamicChannelPool::GetRandomChannel()
{
    constexpr int TryCount = 3;
    auto now = TInstant::Now();

    TReaderGuard guard(SpinLock_);
    if (Terminated_) {
        return MakeFuture<IChannelPtr>(TError("Channel pool is terminated"));
    }

    YCHECK(!Slots_.empty());

    int index = RandomNumber(Slots_.size());
    for (int i = 0; i < TryCount; ++i) {
        if (Slots_[index]->SeemsBroken || !Slots_[index]->IsWarm(now)) {
            index = RandomNumber(Slots_.size());
            continue;
        }
    }

    return Slots_[index]->Channel;
}

void TDynamicChannelPool::Terminate()
{
    std::vector<IChannelPtr> aliveChannels;
    auto error = TError("Channel pool is terminated");

    {
        TWriterGuard guard(SpinLock_);
        if (Terminated_) {
            return;
        }
        Terminated_ = true;
        for (auto& slot : Slots_) {
            slot->Channel.TrySet(error);

            if (slot->Channel.IsSet()) {
                auto channelOrError = slot->Channel.Get();
                if (channelOrError.IsOK()) {
                    aliveChannels.push_back(channelOrError.Value());
                }
            }
        }
        Slots_.clear();
    }

    for (auto channel : aliveChannels) {
        auto terminateError = WaitFor(channel->Terminate(error));
        if (!terminateError.IsOK()) {
            LOG_ERROR(terminateError, "Error while terminating channel pool");
        }
    }

    auto guard = Guard(OpenChannelsLock_);
    for (auto channel : OpenChannels_) {
        auto terminateError = WaitFor(channel.second->Terminate(error));
        if (!terminateError.IsOK()) {
            LOG_ERROR(terminateError, "Error while terminating channel pool");
        }
    }
    OpenChannels_.clear();
}

void TDynamicChannelPool::SetAddressList(const TErrorOr<std::vector<TString>>& addressesOrError)
{
    auto broadcastError = [this] (const TError& error)
    {
        TWriterGuard guard(SpinLock_);
        for (const auto& slot : Slots_) {
            slot->Channel.TrySet(error);
        }
    };

    if (!addressesOrError.IsOK()) {
        broadcastError(addressesOrError);
        return;
    }

    auto addresses = addressesOrError.Value();
    if (addresses.empty()) {
        broadcastError(TError("Address list is empty"));
        return;
    }

    auto rebalanceInterval = RandomDuration(Config_->ChannelPoolRebalanceInterval) +
        Config_->ChannelPoolRebalanceInterval;
    auto now = TInstant::Now();

    std::vector<TChannelSlotPtr> replaced;
    {
        TWriterGuard guard(SpinLock_);
        if (Terminated_) {
            return;
        }
        for (int i = 0; i < Slots_.size(); ++i) {
            if (Slots_[i]->SeemsBroken) {
                Slots_[i] = New<TChannelSlot>();
                replaced.push_back(Slots_[i]);
                continue;
            }

            if (!Slots_[i]->Channel.IsSet()) {
                replaced.push_back(Slots_[i]);
                continue;
            }

            if (Slots_[i]->Channel.IsSet() && !Slots_[i]->Channel.Get().IsOK()) {
                Slots_[i] = New<TChannelSlot>();
                replaced.push_back(Slots_[i]);
                continue;
            }
        }

        int randomVictim = -1;
        if (replaced.empty() && LastRebalance_ + rebalanceInterval < now) {
            LastRebalance_ = now;
            randomVictim = RandomNumber(Slots_.size());
        }

        if (replaced.empty()) {
            Slots_[randomVictim] = New<TChannelSlot>();
            replaced.push_back(Slots_[randomVictim]);
        }
    }

    ShuffleRange(addresses);
    for (int i = 0; i < replaced.size(); i++) {
        auto address = addresses[i % addresses.size()];
        auto channel = CreateChannel(address);
        channel = CreateFailureDetectingChannel(
            std::move(channel),
            BIND([slot = MakeWeak(replaced[i])] (IChannelPtr channel) {
                auto strongSlot = slot.Lock();
                if (strongSlot) {
                    strongSlot->SeemsBroken = true;
                }
            }));

        replaced[i]->Channel.TrySet(channel);
    }

    TerminateIdleChannels();
}

void TDynamicChannelPool::TerminateIdleChannels()
{
    auto guard = Guard(OpenChannelsLock_);
    std::vector<TString> idle;
    for (auto& channel : OpenChannels_) {
        if (channel.second->GetRefCount() == 1) {
            idle.push_back(channel.first);
        }
    }

    for (auto idleAddress : idle) {
        auto channel = OpenChannels_[idleAddress];
        OpenChannels_.erase(idleAddress);
        channel->Terminate(TError("Channel is idle"));
    }
}

IChannelPtr TDynamicChannelPool::CreateChannel(const TString& address)
{
    auto guard = Guard(OpenChannelsLock_);
    auto& channel = OpenChannels_[address];
    if (!channel) {
        channel = ChannelFactory_->CreateChannel(address);
    }
    return channel;
}

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateDynamicChannel(
    TDynamicChannelPoolPtr pool)
{
    auto provider = New<TProxyChannelProvider>(std::move(pool), false);
    return CreateRoamingChannel(std::move(provider));
}

NRpc::IChannelPtr CreateStickyChannel(
    TDynamicChannelPoolPtr pool)
{
    auto provider = New<TProxyChannelProvider>(std::move(pool), true);
    return CreateRoamingChannel(std::move(provider));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT
