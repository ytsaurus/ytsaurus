#include "caching_channel_factory.h"
#include "channel.h"
#include "channel_detail.h"
#include "client.h"

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {
namespace NRpc {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TCachedChannel
    : public IChannel
{
public:
    TCachedChannel(
        TCachingChannelFactory* factory,
        IChannelPtr underlyingChannel,
        const TString& address)
        : Factory_(factory)
        , UnderlyingChannel_(std::move(underlyingChannel))
        , Address_(address)
        , LastAccess_(TInstant::Now())
    { }

    virtual const TString& GetEndpointDescription() const override
    {
        return UnderlyingChannel_->GetEndpointDescription();
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return UnderlyingChannel_->GetEndpointAttributes();
    }

    void Touch()
    {
        LastAccess_.store(TInstant::Now());
    }

    bool IsIdleSince(TInstant time)
    {
        return GetRefCount() == 1 && LastAccess_.load() < time;
    }

    TInstant GetLastAccess()
    {
        return LastAccess_.load();
    }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        return UnderlyingChannel_->Send(
            std::move(request),
            std::move(responseHandler),
            options);
    }

    virtual TFuture<void> Terminate(const TError& error) override;

private:
    const TWeakPtr<TCachingChannelFactory> Factory_;
    const IChannelPtr UnderlyingChannel_;
    const TString Address_;

    std::atomic<TInstant> LastAccess_;
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
        std::vector<std::pair<TString, TCachedChannelPtr>> idleChannels;
        auto lastActiveTime = TInstant::Now() - ttl;

        {
            TWriterGuard guard(SpinLock_);
            for (const auto& channel : ChannelMap_) {
                if (channel.second->IsIdleSince(lastActiveTime)) {
                    idleChannels.push_back(channel);
                }
            }

            for (const auto& idle : idleChannels) {
                ChannelMap_.erase(idle.first);
            }
        }

        auto error = TError("Channel is idle")
            << TErrorAttribute("ttl", ttl);
        for (const auto& idle : idleChannels) {
            idle.second->Terminate(error);
        }
    }

    virtual IChannelPtr CreateChannel(const TString& address) override
    {
        {
            TReaderGuard guard(SpinLock_);
            auto it = ChannelMap_.find(address);
            if (it != ChannelMap_.end()) {
                it->second->Touch();
                return it->second;
            }
        }

        auto underlyingChannel = UnderlyingFactory_->CreateChannel(address);
        auto wrappedChannel = New<TCachedChannel>(this, underlyingChannel, address);

        {
            TWriterGuard guard(SpinLock_);
            auto it = ChannelMap_.find(address);
            if (it == ChannelMap_.end()) {
                YCHECK(ChannelMap_.insert(std::make_pair(address, wrappedChannel)).second);
                return wrappedChannel;
            } else {
                return it->second;
            }
        }
    }

    void EvictChannel(const TString& address, IChannel* channel)
    {
        TWriterGuard guard(SpinLock_);
        auto it = ChannelMap_.find(address);
        if (it != ChannelMap_.end() && it->second.Get() == channel) {
            ChannelMap_.erase(it);
        }
    }

private:
    const IChannelFactoryPtr UnderlyingFactory_;

    TReaderWriterSpinLock SpinLock_;
    THashMap<TString, TCachedChannelPtr> ChannelMap_;
};

DEFINE_REFCOUNTED_TYPE(TCachingChannelFactory)

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TCachedChannel::Terminate(const TError& error)
{
    auto factory = Factory_.Lock();
    if (factory) {
        factory->EvictChannel(Address_, this);
    }
    return UnderlyingChannel_->Terminate(error);
}

////////////////////////////////////////////////////////////////////////////////

ICachingChannelFactoryPtr CreateCachingChannelFactory(IChannelFactoryPtr underlyingFactory)
{
    YCHECK(underlyingFactory);

    return New<TCachingChannelFactory>(std::move(underlyingFactory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
