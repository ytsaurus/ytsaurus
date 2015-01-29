#include "stdafx.h"
#include "cancelable_context.h"
#include "callback.h"
#include "invoker_util.h"
#include "invoker_detail.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCancelableContext::TCancelableInvoker
    : public TInvokerWrapper
{
public:
    TCancelableInvoker(
        TCancelableContextPtr context,
        IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
        , Context_(std::move(context))
    {
        YCHECK(Context_);
    }

    virtual void Invoke(const TClosure& callback) override
    {
        YASSERT(callback);

        if (Context_->Canceled_)
            return;

        auto this_ = MakeStrong(this);
        return UnderlyingInvoker_->Invoke(BIND([this, this_, callback] {
            if (!Context_->Canceled_) {
                TCurrentInvokerGuard guard(this_);
                callback.Run();
            }
        }));
    }

private:
    TCancelableContextPtr Context_;

};

////////////////////////////////////////////////////////////////////////////////

bool TCancelableContext::IsCanceled() const
{
    return Canceled_;
}

void TCancelableContext::Cancel()
{
    yhash_set<TWeakPtr<TCancelableContext>> propagateToContexts;
    yhash_set<TFuture<void>> propagateToFutures;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (Canceled_)
            return;
        Canceled_ = true;
        PropagateToContexts_.swap(propagateToContexts);
        PropagateToFutures_.swap(propagateToFutures);
    }

    Handlers_.FireAndClear();

    for (auto weakContext : propagateToContexts) {
        auto context = weakContext.Lock();
        if (context) {
            context->Cancel();
        }
    }

    for (auto future : propagateToFutures) {
        future.Cancel();
    }
}

IInvokerPtr TCancelableContext::CreateInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TCancelableInvoker>(this, std::move(underlyingInvoker));
}

void TCancelableContext::SubscribeCanceled(const TClosure& callback)
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (Canceled_) {
        guard.Release();
        callback.Run();
        return;
    }
    Handlers_.Subscribe(callback);
}

void TCancelableContext::UnsubscribeCanceled(const TClosure& /*callback*/)
{
    YUNREACHABLE();
}

void TCancelableContext::PropagateTo(TCancelableContextPtr context)
{
    auto weakContext = MakeWeak(context);

    {
        TGuard<TSpinLock> guard(SpinLock_);
        PropagateToContexts_.insert(context);
    }

    auto weakThis = MakeWeak(this);
    context->SubscribeCanceled(BIND([=] () {
        auto this_ = weakThis.Lock();
        if (this_) {
            TGuard<TSpinLock> guard(this_->SpinLock_);
            this_->PropagateToContexts_.erase(context);
        }
    }));
}

void TCancelableContext::PropagateTo(TFuture<void> future)
{
    {
        TGuard<TSpinLock> guard(SpinLock_);
        PropagateToFutures_.insert(future);
    }

    auto weakThis = MakeWeak(this);
    future.Subscribe(BIND([=] (const TError&) {
        auto this_ = weakThis.Lock();
        if (this_) {
            TGuard<TSpinLock> guard(this_->SpinLock_);
            this_->PropagateToFutures_.erase(future);
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
