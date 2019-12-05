#include "cancelable_context.h"
#include "callback.h"
#include "invoker_detail.h"
#include "invoker_util.h"

#include <yt/core/concurrency/scheduler.h>

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
        YT_VERIFY(Context_);
    }

    virtual void Invoke(TClosure callback) override
    {
        YT_ASSERT(callback);

        if (Context_->Canceled_) {
            return;
        }

        return UnderlyingInvoker_->Invoke(BIND([=, this_ = MakeStrong(this), callback = std::move(callback)] {
            if (Context_->Canceled_) {
                return;
            }

            NConcurrency::TCurrentInvokerGuard guard(this_);
            callback.Run();
        }));
    }

private:
    const TCancelableContextPtr Context_;

};

////////////////////////////////////////////////////////////////////////////////

bool TCancelableContext::IsCanceled() const
{
    return Canceled_;
}

void TCancelableContext::Cancel()
{
    THashSet<TWeakPtr<TCancelableContext>> propagateToContexts;
    THashSet<TAwaitable> propagateToAwaitables;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (Canceled_) {
            return;
        }
        Canceled_ = true;
        PropagateToContexts_.swap(propagateToContexts);
        PropagateToAwaitables_.swap(propagateToAwaitables);
    }

    Handlers_.FireAndClear();

    for (const auto& weakContext : propagateToContexts) {
        auto context = weakContext.Lock();
        if (context) {
            context->Cancel();
        }
    }

    for (const auto& awaitable : propagateToAwaitables) {
        awaitable.Cancel();
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
    YT_ABORT();
}

void TCancelableContext::PropagateTo(const TCancelableContextPtr& context)
{
    auto weakContext = MakeWeak(context);

    bool canceled = [&] {
        TGuard<TSpinLock> guard(SpinLock_);
        if (Canceled_) {
            return true;
        }
        PropagateToContexts_.insert(context);
        return false;
    } ();

    if (canceled) {
        context->Cancel();
        return;
    }

    context->SubscribeCanceled(BIND([=, weakThis = MakeWeak(this)] {
        if (auto this_ = weakThis.Lock()) {
            TGuard<TSpinLock> guard(SpinLock_);
            PropagateToContexts_.erase(context);
        }
    }));
}

void TCancelableContext::PropagateTo(const TAwaitable& awaitable)
{
    bool canceled = [&] {
        TGuard<TSpinLock> guard(SpinLock_);
        if (Canceled_) {
            return true;
        }
        PropagateToAwaitables_.insert(awaitable);

        return false;
    } ();

    if (canceled) {
        awaitable.Cancel();
        return;
    }

    awaitable.Subscribe(BIND([=, weakThis = MakeWeak(this)] () {
        if (auto this_ = weakThis.Lock()) {
            TGuard<TSpinLock> guard(SpinLock_);
            PropagateToAwaitables_.erase(awaitable);
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
