#include "caching_channel_factory.h"
#include "channel.h"
#include "channel_detail.h"
#include "client.h"
#include "dispatcher.h"
#include "private.h"

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/lock_free.h>

namespace NYT::NRpc {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYT::NBus;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ExpirationCheckInterval = TDuration::Seconds(15);
static const auto& Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TCachedChannel
    : public TChannelWrapper
{
public:
    TCachedChannel(
        TCachingChannelFactory* factory,
        IChannelPtr underlyingChannel,
        const TString& address)
        : TChannelWrapper(std::move(underlyingChannel))
        , Factory_(factory)
        , Address_(address)
        , LastActivityTime_(TInstant::Now())
    {
        UnderlyingChannel_->SubscribeTerminated(BIND(&TCachedChannel::OnTerminated, MakeWeak(this)));
    }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        Touch();
        return TChannelWrapper::Send(
            std::move(request),
            std::move(responseHandler),
            options);
    }

    void Touch()
    {
        LastActivityTime_.store(TInstant::Now());
    }

    TInstant GetLastActivityTime() const
    {
        return LastActivityTime_.load();
    }

private:
    const TWeakPtr<TCachingChannelFactory> Factory_;
    const TString Address_;

    std::atomic<TInstant> LastActivityTime_;

    void OnTerminated(const TError& error);
};

DECLARE_REFCOUNTED_CLASS(TCachedChannel)
DEFINE_REFCOUNTED_TYPE(TCachedChannel)

////////////////////////////////////////////////////////////////////////////////

class TCachingChannelFactory
    : public IChannelFactory
{
public:
    TCachingChannelFactory(
        IChannelFactoryPtr underlyingFactory,
        TDuration expirationTime)
        : UnderlyingFactory_(std::move(underlyingFactory))
        , IdleChannelTtl_(expirationTime)
        , ExpirationExecutor_(New<TPeriodicExecutor>(
            TDispatcher::Get()->GetHeavyInvoker(),
            BIND(&TCachingChannelFactory::CheckExpiredChannels, MakeWeak(this)),
            std::min(ExpirationCheckInterval, IdleChannelTtl_)))
    {
        if (IdleChannelTtl_ != TDuration::Max()) {
            ExpirationExecutor_->Start();
        }
    }

    virtual IChannelPtr CreateChannel(const TAddressWithNetwork& addressWithNetwork) override
    {
        return DoCreateChannel(
            addressWithNetwork.Address,
            [&] { return UnderlyingFactory_->CreateChannel(addressWithNetwork); });
    }

    virtual IChannelPtr CreateChannel(const TString& address) override
    {
        return DoCreateChannel(
            address,
            [&] { return UnderlyingFactory_->CreateChannel(address); });
    }

    void EvictChannel(const TString& address, IChannel* evictableChannel)
    {
        TWriterGuard guard(SpinLock_);

        YT_LOG_DEBUG("Cached channel evicted (Endpoint: %v)",
            evictableChannel->GetEndpointDescription());

        if (auto it = WeakChannelMap_.find(address); it != WeakChannelMap_.end()) {
            if (auto existingChannel = it->second.Lock(); existingChannel.Get() == evictableChannel) {
                WeakChannelMap_.erase(it);
            }
        }

        if (auto it = StrongChannelMap_.find(address); it != StrongChannelMap_.end()) {
            if (const auto& existingChannel = it->second; existingChannel.Get() == evictableChannel) {
                StrongChannelMap_.erase(it);
            }
        }
    }

private:
    const IChannelFactoryPtr UnderlyingFactory_;
    const TDuration IdleChannelTtl_;

    TPeriodicExecutorPtr ExpirationExecutor_;

    TReaderWriterSpinLock SpinLock_;
    THashMap<TString, TCachedChannelPtr> StrongChannelMap_;
    THashMap<TString, TWeakPtr<TCachedChannel>> WeakChannelMap_;

    using TTtlItem = std::pair<TString, TWeakPtr<TCachedChannel>>;
    TMultipleProducerSingleConsumerLockFreeStack<TTtlItem> TtlRegisterQueue_;
    std::vector<TTtlItem> TtlCheckQueue_;

    template <class TFactory>
    IChannelPtr DoCreateChannel(const TString& address, const TFactory& factory)
    {
        {
            TReaderGuard readerGuard(SpinLock_);

            if (auto it = StrongChannelMap_.find(address)) {
                auto channel = it->second;
                channel->Touch();
                return channel;
            }

            if (auto it = WeakChannelMap_.find(address)) {
                const auto& weakChannel = it->second;
                if (auto channel = weakChannel.Lock()) {
                    readerGuard.Release();

                    {
                        TWriterGuard writerGuard(SpinLock_);
                        // Check if the weak map still contains the same channel.
                        if (auto jt = WeakChannelMap_.find(address); jt != WeakChannelMap_.end() && jt->second == weakChannel) {
                            StrongChannelMap_.emplace(address, channel);
                            RegisterChannelForTtlChecks(address, channel);
                        }
                    }

                    channel->Touch();
                    return channel;
                }
            }
        }

        auto underlyingChannel = factory();
        auto wrappedChannel = New<TCachedChannel>(this, underlyingChannel, address);

        {
            TWriterGuard writerGuard(SpinLock_);
            // Check if another channel has been inserted while the lock was released.
            if (auto it = WeakChannelMap_.find(address)) {
                const auto& weakChannel = it->second;
                if (auto channel = weakChannel.Lock()) {
                    StrongChannelMap_.emplace(address, channel);
                    channel->Touch();
                    return channel;
                }
            }
            
            WeakChannelMap_.emplace(address, wrappedChannel);
            StrongChannelMap_.emplace(address, wrappedChannel);
            RegisterChannelForTtlChecks(address, wrappedChannel);

            YT_LOG_DEBUG("Cached channel registered (Endpoint: %v)",
                wrappedChannel->GetEndpointDescription());

            return wrappedChannel;
        }
    }

    void RegisterChannelForTtlChecks(const TString& address, const TCachedChannelPtr& channel)
    {
        if (IdleChannelTtl_ < TDuration::Max()) {
            TtlRegisterQueue_.Enqueue({address, channel});
        }
    }

    void CheckExpiredChannels()
    {
        TtlRegisterQueue_.DequeueAll(false, [&] (auto&& item) {
            TtlCheckQueue_.push_back(std::move(item));
        });

        auto deadline = TInstant::Now() - IdleChannelTtl_;

        std::vector<std::pair<TString, TCachedChannelPtr>> expiredItems;
        auto it = TtlCheckQueue_.begin();
        while (it != TtlCheckQueue_.end()) {
            auto channel = it->second.Lock();
            if (!channel || channel->GetLastActivityTime() < deadline) {
                YT_LOG_DEBUG("Cached channel expired (Address: %v, Endpoint: %v, Ttl: %v)",
                    it->first,
                    channel ? channel->GetEndpointDescription() : TStringBuf("<unknown>"),
                    IdleChannelTtl_);
                expiredItems.emplace_back(std::move(it->first), std::move(channel));
                *it = std::move(TtlCheckQueue_.back());
                TtlCheckQueue_.pop_back();
            } else {
                ++it;
            }
        }

        if (!expiredItems.empty()) {
            TWriterGuard guard(SpinLock_);
            for (auto& item : expiredItems) {
                if (auto it = StrongChannelMap_.find(item.first)) {
                    if (it->second == item.second) {
                        StrongChannelMap_.erase(it);
                    }
                }
                item.second.Reset();
                if (auto it = WeakChannelMap_.find(item.first)) {
                    if (it->second.IsExpired()) {
                        WeakChannelMap_.erase(it);
                    }
                }
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TCachingChannelFactory)

////////////////////////////////////////////////////////////////////////////////

void TCachedChannel::OnTerminated(const TError& error)
{
    if (auto factory = Factory_.Lock()) {
        factory->EvictChannel(Address_, this);
    }
}

////////////////////////////////////////////////////////////////////////////////

IChannelFactoryPtr CreateCachingChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    TDuration idleChannelTtl)
{
    YT_VERIFY(underlyingFactory);

    return New<TCachingChannelFactory>(
        std::move(underlyingFactory),
        idleChannelTtl);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
