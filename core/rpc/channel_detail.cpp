#include "channel_detail.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

TChannelWrapper::TChannelWrapper(IChannelPtr underlyingChannel)
    : UnderlyingChannel_(std::move(underlyingChannel))
{
    Y_ASSERT(UnderlyingChannel_);
}

const TString& TChannelWrapper::GetEndpointDescription() const
{
    return UnderlyingChannel_->GetEndpointDescription();
}

const NYTree::IAttributeDictionary& TChannelWrapper::GetEndpointAttributes() const
{
    return UnderlyingChannel_->GetEndpointAttributes();
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

TFuture<void> TChannelWrapper::Terminate(const TError& error)
{
    return UnderlyingChannel_->Terminate(error);
}

////////////////////////////////////////////////////////////////////////////////

void TClientRequestControlThunk::SetUnderlying(IClientRequestControlPtr underlyingControl)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!underlyingControl)
        return;

    TGuard<TSpinLock> guard(SpinLock_);
    UnderlyingCanceled_ = false;
    Underlying_ = std::move(underlyingControl);
    if (Canceled_) {
        PropagateCancel(guard);
    }
}

void TClientRequestControlThunk::Cancel()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);
    Canceled_ = true;
    if (Underlying_) {
        PropagateCancel(guard);
    }
}

void TClientRequestControlThunk::PropagateCancel(TGuard<TSpinLock>& guard)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (!UnderlyingCanceled_) {
        UnderlyingCanceled_ = true;
        auto underlying = Underlying_;
        guard.Release();
        underlying->Cancel();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
