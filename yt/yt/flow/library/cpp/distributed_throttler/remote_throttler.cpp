#include "remote_throttler.h"

#include "service_proxy.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

TRemoteThrottler::TRemoteThrottler(
    std::function<NRpc::IChannelPtr()> channelProvider,
    TThrottlerId throttlerName,
    std::string clientId,
    TDuration rpcTimeout,
    std::function<TPriority()> priorityProvider,
    IStatusErrorStatePtr errorState,
    NLogging::TLogger logger)
    : ChannelProvider_(std::move(channelProvider))
    , ThrottlerName_(std::move(throttlerName))
    , ClientId_(std::move(clientId))
    , RpcTimeout_(rpcTimeout)
    , PriorityProvider_(std::move(priorityProvider))
    , ErrorState_(std::move(errorState))
    , Logger_(std::move(logger))
{ }

TFuture<void> TRemoteThrottler::Throttle(i64 amount)
{
    auto channel = ChannelProvider_();
    if (!channel) {
        return MakeFuture<void>(TError("Controller channel is not available"));
    }
    TDistributedThrottlerServiceProxy proxy(std::move(channel));
    auto req = proxy.RequestQuota();
    req->SetTimeout(RpcTimeout_);
    NRpc::GenerateMutationId(req);
    req->set_throttler_id(ThrottlerName_);
    req->set_client_id(ClientId_);
    req->set_amount(amount);
    req->set_timestamp(PriorityProvider_ ? PriorityProvider_() : 0);

    auto future = req->Invoke().AsVoid();

    if (ErrorState_) {
        auto errorState = ErrorState_;
        future.Subscribe(BIND([errorState] (const TError& error) {
            if (error.IsOK()) {
                errorState->ClearError();
            } else {
                errorState->SetError(error);
            }
        }));
    }

    return future;
}

bool TRemoteThrottler::TryAcquire(i64 /*amount*/)
{
    return false;
}

i64 TRemoteThrottler::TryAcquireAvailable(i64 /*amount*/)
{
    return 0;
}

void TRemoteThrottler::Acquire(i64 amount)
{
    Y_UNUSED(Throttle(amount));
}

void TRemoteThrottler::Release(i64 /*amount*/)
{ }

bool TRemoteThrottler::IsOverdraft()
{
    return false;
}

i64 TRemoteThrottler::GetQueueTotalAmount() const
{
    return 0;
}

TDuration TRemoteThrottler::GetEstimatedOverdraftDuration() const
{
    return TDuration::Zero();
}

i64 TRemoteThrottler::GetAvailable() const
{
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
