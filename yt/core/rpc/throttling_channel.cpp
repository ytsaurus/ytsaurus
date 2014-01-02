#include "stdafx.h"
#include "config.h"
#include "public.h"
#include "throttling_channel.h"

#include <core/concurrency/public.h>
#include <core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TThrottlingChannel
    : public IChannel
{
public:
    TThrottlingChannel(
        TThrottlingChannelConfigPtr config,
        IChannelPtr underlyingChannel)
        : Config(config)
        , UnderlyingChannel(underlyingChannel)
    {
        auto throttlerConfig = New<NConcurrency::TThroughputThrottlerConfig>();
        throttlerConfig->Period = TDuration::Seconds(1);
        throttlerConfig->Limit = Config->RateLimit;
        Throttler = CreateLimitedThrottler(throttlerConfig);
    }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return UnderlyingChannel->GetDefaultTimeout();
    }

    virtual void SetDefaultTimeout(const TNullable<TDuration>& timeout) override
    {
        UnderlyingChannel->SetDefaultTimeout(timeout);
    }
    
    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        Throttler->Throttle(1).Subscribe(BIND(
            &IChannel::Send,
            UnderlyingChannel,
            std::move(request),
            std::move(responseHandler),
            timeout,
            requestAck));
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        return UnderlyingChannel->Terminate(error);
    }

private:
    TThrottlingChannelConfigPtr Config;
    IChannelPtr UnderlyingChannel;

    NConcurrency::IThroughputThrottlerPtr Throttler;

};

IChannelPtr CreateThrottlingChannel(
    TThrottlingChannelConfigPtr config,
    IChannelPtr underlyingChannel)
{
    YCHECK(config);
    YCHECK(underlyingChannel);

    return New<TThrottlingChannel>(config, underlyingChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
