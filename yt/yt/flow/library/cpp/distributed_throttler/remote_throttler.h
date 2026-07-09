#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

//! Forwards Throttle() to a remote TDistributedThrottlerService over RPC;
//! the returned future resolves when the server grants the quota.
//! Synchronous methods can't be answered without a round-trip, so they
//! return pessimistic defaults. Wrap with TPrefetchingThrottler.
class TRemoteThrottler
    : public NConcurrency::IThroughputThrottler
{
public:
    //! |channelProvider| is called per RPC; a new leader is picked up on
    //! the next call.
    TRemoteThrottler(
        std::function<NRpc::IChannelPtr()> channelProvider,
        TThrottlerId throttlerName,
        std::string clientId,
        TDuration rpcTimeout,
        std::function<TPriority()> priorityProvider,
        IStatusErrorStatePtr errorState,
        NLogging::TLogger logger);

    TFuture<void> Throttle(i64 amount) override;

    bool TryAcquire(i64 amount) override;
    i64 TryAcquireAvailable(i64 amount) override;
    void Acquire(i64 amount) override;
    void Release(i64 amount) override;
    bool IsOverdraft() override;

    i64 GetQueueTotalAmount() const override;
    TDuration GetEstimatedOverdraftDuration() const override;
    i64 GetAvailable() const override;

private:
    const std::function<NRpc::IChannelPtr()> ChannelProvider_;
    const TThrottlerId ThrottlerName_;
    const std::string ClientId_;
    const TDuration RpcTimeout_;
    const std::function<TPriority()> PriorityProvider_;
    const IStatusErrorStatePtr ErrorState_;
    const NLogging::TLogger Logger_;
};

DEFINE_REFCOUNTED_TYPE(TRemoteThrottler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
