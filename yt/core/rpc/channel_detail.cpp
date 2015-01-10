#include "stdafx.h"
#include "channel_detail.h"

#include <core/concurrency/thread_affinity.h>

#include <core/actions/future.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

TChannelWrapper::TChannelWrapper(IChannelPtr underlyingChannel)
    : UnderlyingChannel_(std::move(underlyingChannel))
{
    YASSERT(UnderlyingChannel_);
}

TNullable<TDuration> TChannelWrapper::GetDefaultTimeout() const
{
    return DefaultTimeout_;
}

void TChannelWrapper::SetDefaultTimeout(const TNullable<TDuration>& timeout)
{
    DefaultTimeout_ = timeout;
}

NYTree::TYsonString TChannelWrapper::GetEndpointDescription() const
{
    return UnderlyingChannel_->GetEndpointDescription();
}

IClientRequestControlPtr TChannelWrapper::Send(
    IClientRequestPtr request,
    IClientResponseHandlerPtr responseHandler,
    TNullable<TDuration> timeout,
    bool requestAck)
{
    return UnderlyingChannel_->Send(
        std::move(request),
        std::move(responseHandler),
        timeout ? timeout : DefaultTimeout_,
        requestAck);
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
        guard.Release();
        Underlying_->Cancel();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
