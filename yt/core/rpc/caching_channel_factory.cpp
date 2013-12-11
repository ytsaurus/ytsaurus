#include "stdafx.h"
#include "caching_channel_factory.h"
#include "channel.h"
#include "client.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TCachedChannel
    : public IChannel
{
public:
    explicit TCachedChannel(IChannelPtr underlyingChannel)
        : UnderlyingChannel_(std::move(underlyingChannel))
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return DefaultTimeout_;
    }

    virtual void SetDefaultTimeout(const TNullable<TDuration>& timeout) override
    {
        DefaultTimeout_ = timeout;
    }

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout) override
    {
        UnderlyingChannel_->Send(
            std::move(request),
            std::move(responseHandler),
            timeout ? timeout : DefaultTimeout_);
    }

    virtual TFuture<void> Terminate(const TError& /*error*/) override
    {
        YUNREACHABLE();
    }

private:
    IChannelPtr UnderlyingChannel_;
    TNullable<TDuration> DefaultTimeout_;

};

class TCachingChannelFactory
    : public IChannelFactory
{
public:
    explicit TCachingChannelFactory(IChannelFactoryPtr underlyingFactory)
        : UnderlyingFactory_(std::move(underlyingFactory))
    { }

    virtual IChannelPtr CreateChannel(const Stroka& address) override
    {
        TGuard<TSpinLock> firstAttemptGuard(SpinLock);

        auto it = ChannelMap.find(address);
        if (it == ChannelMap.end()) {
            firstAttemptGuard.Release();

            auto channel = UnderlyingFactory_->CreateChannel(address);

            TGuard<TSpinLock> secondAttemptGuard(SpinLock);
            it = ChannelMap.find(address);
            if (it == ChannelMap.end()) {
                it = ChannelMap.insert(std::make_pair(address, channel)).first;
            } else {
                channel->Terminate(TError(
                    NRpc::EErrorCode::TransportError,
                    "Channel terminated"));
            }
        }

        return New<TCachedChannel>(it->second);
    }

private:
    IChannelFactoryPtr UnderlyingFactory_;

    TSpinLock SpinLock;
    yhash_map<Stroka, IChannelPtr> ChannelMap;

};

IChannelFactoryPtr CreateCachingChannelFactory(IChannelFactoryPtr underlyingFactory)
{
    return New<TCachingChannelFactory>(underlyingFactory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
