#include "remote_throttler.h"

#include <yt/yt/flow/library/cpp/common/controller/channel_provider.h>

#include <yt/yt/flow/library/cpp/distributed_throttler/service_proxy.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

TRemoteThrottler::TRemoteThrottler(
    NRpc::IChannelPtr channel,
    TThrottlerId throttlerName,
    std::string clientId,
    TDuration rpcTimeout,
    std::function<TPriority()> priorityProvider,
    std::function<TQuotaClassId()> quotaClassProvider,
    IStatusErrorStatePtr errorState,
    NLogging::TLogger logger)
    : Channel_(std::move(channel))
    , ThrottlerName_(std::move(throttlerName))
    , ClientId_(std::move(clientId))
    , RpcTimeout_(rpcTimeout)
    , PriorityProvider_(std::move(priorityProvider))
    , QuotaClassProvider_(std::move(quotaClassProvider))
    , ErrorState_(std::move(errorState))
    , Logger_(std::move(logger))
{ }

TRemoteThrottler::TRemoteThrottler(
    std::function<NRpc::IChannelPtr()> channelProvider,
    TThrottlerId throttlerName,
    std::string clientId,
    TDuration rpcTimeout,
    std::function<TPriority()> priorityProvider,
    std::function<TQuotaClassId()> quotaClassProvider,
    IStatusErrorStatePtr errorState,
    NLogging::TLogger logger)
    : TRemoteThrottler(
        NRpc::CreateRoamingChannel(New<TControllerChannelProvider>(std::move(channelProvider))),
        std::move(throttlerName),
        std::move(clientId),
        rpcTimeout,
        std::move(priorityProvider),
        std::move(quotaClassProvider),
        std::move(errorState),
        std::move(logger))
{ }

TFuture<void> TRemoteThrottler::GetFailureFuture() const
{
    return FailurePromise_.ToFuture();
}

TFuture<void> TRemoteThrottler::Throttle(i64 amount)
{
    if (FailurePromise_.IsSet()) {
        return GetFailureFuture();
    }

    TDistributedThrottlerServiceProxy proxy(Channel_);
    auto req = proxy.RequestQuota();
    req->SetTimeout(RpcTimeout_);
    NRpc::GenerateMutationId(req);
    req->set_throttler_id(ThrottlerName_);
    req->set_client_id(ClientId_);
    req->set_amount(amount);
    req->set_timestamp(PriorityProvider_ ? PriorityProvider_() : 0);
    if (QuotaClassProvider_) {
        auto quotaClassId = QuotaClassProvider_();
        if (!quotaClassId.empty()) {
            req->set_quota_class_id(std::move(quotaClassId));
        }
    }

    return req->Invoke().AsVoid().Apply(BIND_NO_PROPAGATE(&TRemoteThrottler::OnQuotaResponse, MakeStrong(this)));
}

void TRemoteThrottler::OnQuotaResponse(const TError& error)
{
    if (!error.IsOK()) {
        FailurePromise_.TrySet(error);
    }
    if (ErrorState_) {
        auto guard = Guard(StatusLock_);
        ErrorState_->SetError(FailurePromise_.IsSet() ? FailurePromise_.GetOrCrash() : error);
    }
    if (FailurePromise_.IsSet()) {
        FailurePromise_.GetOrCrash()
            .ThrowOnError();
    }
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
