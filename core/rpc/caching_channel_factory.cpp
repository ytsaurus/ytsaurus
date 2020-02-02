#include "caching_channel_factory.h"
#include "channel.h"
#include "channel_detail.h"
#include "client.h"

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT::NRpc {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYT::NBus;

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
        , LastAccessTime_(TInstant::Now())
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
        LastAccessTime_.store(TInstant::Now());
    }

    bool IsIdleSince(TInstant time)
    {
        return LastAccessTime_.load() < time;
    }

private:
    const TWeakPtr<TCachingChannelFactory> Factory_;
    const TString Address_;

    std::atomic<TInstant> LastAccessTime_;

    void OnTerminated(const TError& error);
};

DECLARE_REFCOUNTED_CLASS(TCachedChannel)
DEFINE_REFCOUNTED_TYPE(TCachedChannel)

////////////////////////////////////////////////////////////////////////////////

class TCachingChannelFactory
    : public ICachingChannelFactory
{
public:
    explicit TCachingChannelFactory(IChannelFactoryPtr underlyingFactory)
        : UnderlyingFactory_(std::move(underlyingFactory))
    { }

    virtual void TerminateIdleChannels(TDuration ttl) override
    {
        auto lastActiveTime = TInstant::Now() - ttl;

        TWriterGuard guard(SpinLock_);

        for (auto it = StrongChannelMap_.begin(); it != StrongChannelMap_.end();) {
            auto channel = it->second;
            auto jt = it++;
            if (channel->IsIdleSince(lastActiveTime)) {
                StrongChannelMap_.erase(jt);
            }
        }

        for (auto it = WeakChannelMap_.begin(); it != WeakChannelMap_.end();) {
            auto weakChannel = it->second;
            auto jt = it++;
            if (weakChannel.IsExpired()) {
                WeakChannelMap_.erase(jt);
            }
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

    TReaderWriterSpinLock SpinLock_;
    THashMap<TString, TCachedChannelPtr> StrongChannelMap_;
    THashMap<TString, TWeakPtr<TCachedChannel>> WeakChannelMap_;


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

                    TWriterGuard writerGuard(SpinLock_);
                    // Check if the weak map still contains the same channel.
                    if (auto jt = WeakChannelMap_.find(address); jt != WeakChannelMap_.end() && jt->second == weakChannel) {
                        StrongChannelMap_.emplace(address, channel);
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
            return wrappedChannel;
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

ICachingChannelFactoryPtr CreateCachingChannelFactory(IChannelFactoryPtr underlyingFactory)
{
    YT_VERIFY(underlyingFactory);

    return New<TCachingChannelFactory>(std::move(underlyingFactory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
