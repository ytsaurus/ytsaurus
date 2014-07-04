#include "stdafx.h"
#include "throttling_channel.h"
#include "channel_detail.h"
#include "config.h"

#include <core/concurrency/public.h>
#include <core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TThrottlingChannel
    : public TChannelWrapper
{
public:
    TThrottlingChannel(TThrottlingChannelConfigPtr config, IChannelPtr underlyingChannel)
        : TChannelWrapper(std::move(underlyingChannel))
        , Config_(config)
    {
        auto throttlerConfig = New<NConcurrency::TThroughputThrottlerConfig>();
        throttlerConfig->Period = TDuration::Seconds(1);
        throttlerConfig->Limit = Config_->RateLimit;
        Throttler_ = CreateLimitedThrottler(throttlerConfig);
    }

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        Throttler_->Throttle(1).Subscribe(BIND(
            &IChannel::Send,
            UnderlyingChannel_,
            std::move(request),
            std::move(responseHandler),
            timeout,
            requestAck));
    }

private:
    TThrottlingChannelConfigPtr Config_;

    NConcurrency::IThroughputThrottlerPtr Throttler_;

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
