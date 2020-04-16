#include "channel_detail.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

TChannelWrapper::TChannelWrapper(IChannelPtr underlyingChannel)
    : UnderlyingChannel_(std::move(underlyingChannel))
{
    YT_ASSERT(UnderlyingChannel_);
}

const TString& TChannelWrapper::GetEndpointDescription() const
{
    return UnderlyingChannel_->GetEndpointDescription();
}

const NYTree::IAttributeDictionary& TChannelWrapper::GetEndpointAttributes() const
{
    return UnderlyingChannel_->GetEndpointAttributes();
}

TNetworkId TChannelWrapper::GetNetworkId() const
{
    return UnderlyingChannel_->GetNetworkId();
}

IClientRequestControlPtr TChannelWrapper::Send(
    IClientRequestPtr request,
    IClientResponseHandlerPtr responseHandler,
    const TSendOptions& options)
{
    return UnderlyingChannel_->Send(
        std::move(request),
        std::move(responseHandler),
        options);
}

void TChannelWrapper::Terminate(const TError& error)
{
    UnderlyingChannel_->Terminate(error);
}

void TChannelWrapper::SubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    UnderlyingChannel_->SubscribeTerminated(callback);
}

void TChannelWrapper::UnsubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    UnderlyingChannel_->UnsubscribeTerminated(callback);
}

////////////////////////////////////////////////////////////////////////////////

void TClientRequestControlThunk::SetUnderlying(IClientRequestControlPtr underlying)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!underlying) {
        return;
    }

    TGuard<TSpinLock> guard(SpinLock_);

    // NB: SetUnderlying can only be invoked once.
    // This protects from races on unguarded reads since once Underlying_ is non-null, it never changes.
    YT_VERIFY(!Underlying_);
    Underlying_ = std::move(underlying);

    auto canceled = UnderlyingCanceled_ = Canceled_;
    auto streamingPayloads = std::move(PendingStreamingPayloads_);
    auto streamingFeedback = PendingStreamingFeedback_;

    guard.Release();

    if (canceled) {
        Underlying_->Cancel();
    }

    for (auto& payload : streamingPayloads) {
        payload.Promise.SetFrom(Underlying_->SendStreamingPayload(payload.Payload));
    }

    if (streamingFeedback.Feedback.ReadPosition >= 0) {
        streamingFeedback.Promise.SetFrom(Underlying_->SendStreamingFeedback(streamingFeedback.Feedback));
    }
}

void TClientRequestControlThunk::Cancel()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);

    if (Canceled_) {
        return;
    }

    Canceled_ = true;

    if (Underlying_ && !UnderlyingCanceled_) {
        UnderlyingCanceled_ = true;
        guard.Release();
        Underlying_->Cancel();
    }
}

TFuture<void> TClientRequestControlThunk::SendStreamingPayload(const TStreamingPayload& payload)
{
    TGuard<TSpinLock> guard(SpinLock_);

    if (Underlying_) {
        guard.Release();
        return Underlying_->SendStreamingPayload(payload);
    }

    auto promise = NewPromise<void>();
    PendingStreamingPayloads_.push_back({
        payload,
        promise
    });
    return promise.ToFuture();
}

TFuture<void> TClientRequestControlThunk::SendStreamingFeedback(const TStreamingFeedback& feedback)
{
    TGuard<TSpinLock> guard(SpinLock_);

    if (Underlying_) {
        guard.Release();
        return Underlying_->SendStreamingFeedback(feedback);
    }

    if (!PendingStreamingFeedback_.Promise) {
        PendingStreamingFeedback_.Promise = NewPromise<void>();
    }
    auto promise = PendingStreamingFeedback_.Promise;

    PendingStreamingFeedback_.Feedback = TStreamingFeedback{
        std::max(PendingStreamingFeedback_.Feedback.ReadPosition, feedback.ReadPosition)
    };

    return promise;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
