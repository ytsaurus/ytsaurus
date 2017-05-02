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

class TCachingChannelFactory
    : public IChannelFactory
{
public:
    explicit TCachingChannelFactory(IChannelFactoryPtr underlyingFactory)
        : UnderlyingFactory_(std::move(underlyingFactory))
    { }

    virtual IChannelPtr CreateChannel(const Stroka& address) override
    {
        {
            TReaderGuard guard(SpinLock_);
            auto it = ChannelMap_.find(address);
            if (it != ChannelMap_.end()) {
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

    void EvictChannel(const Stroka& address, IChannel* channel)
    {
        TWriterGuard guard(SpinLock_);
        auto it = ChannelMap_.find(address);
        if (it != ChannelMap_.end() && it->second == channel) {
            ChannelMap_.erase(it);
        }
    }

private:
    const IChannelFactoryPtr UnderlyingFactory_;

    TReaderWriterSpinLock SpinLock_;
    yhash<Stroka, IChannelPtr> ChannelMap_;

    class TCachedChannel
        : public IChannel
    {
    public:
        TCachedChannel(
            TCachingChannelFactory* factory,
            IChannelPtr underlyingChannel,
            const Stroka& address)
            : Factory_(factory)
            , UnderlyingChannel_(std::move(underlyingChannel))
            , Address_(address)
        { }

        virtual const Stroka& GetEndpointDescription() const override
        {
            return UnderlyingChannel_->GetEndpointDescription();
        }

        virtual const IAttributeDictionary& GetEndpointAttributes() const override
        {
            return UnderlyingChannel_->GetEndpointAttributes();
        }

        virtual IClientRequestControlPtr Send(
            IClientRequestPtr request,
            IClientResponseHandlerPtr responseHandler,
            TNullable<TDuration> timeout,
            bool requestAck) override
        {
            return UnderlyingChannel_->Send(
                std::move(request),
                std::move(responseHandler),
                timeout,
                requestAck);
        }

        virtual TFuture<void> Terminate(const TError& error) override
        {
            auto factory = Factory_.Lock();
            if (factory) {
                factory->EvictChannel(Address_, this);
            }
            return UnderlyingChannel_->Terminate(error);
        }

    private:
        const TWeakPtr<TCachingChannelFactory> Factory_;
        const IChannelPtr UnderlyingChannel_;
        const Stroka Address_;
    };
};

IChannelFactoryPtr CreateCachingChannelFactory(IChannelFactoryPtr underlyingFactory)
{
    YCHECK(underlyingFactory);

    return New<TCachingChannelFactory>(std::move(underlyingFactory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
