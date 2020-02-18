#pragma once
#ifndef ASYNC_BATCHER_INL_H_
#error "Direct inclusion of this file is not allowed, include async_batcher.h"
// For the sake of sane code completion.
#include "async_batcher.h"
#endif
#undef ASYNC_BATCHER_INL_H_

#include "delayed_executor.h"
#include "thread_affinity.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TAsyncBatcher<T>::TAsyncBatcher(TCallback<TFuture<T>()> provider, TDuration batchingDelay)
    : Provider_(std::move(provider))
    , BatchingDelay_(batchingDelay)
{ }

template <class T>
TFuture<T> TAsyncBatcher<T>::Run()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(Lock_);
    if (!PendingPromise_) {
        PendingPromise_ = NewPromise<void>();
        if (BatchingDelay_) {
            TDelayedExecutor::Submit(
                BIND(&TAsyncBatcher::OnDeadlineReached, MakeWeak(this)),
                BatchingDelay_);
        } else {
            DeadlineReached_ = true;

            if (!ActivePromise_) {
                DoRun(guard);
                return ActivePromise_;
            }
        }
    }
    return PendingPromise_
        .ToFuture()
        .ToUncancelable();
}

template <class T>
void TAsyncBatcher<T>::Cancel(const TError& error)
{
    auto guard = Guard(Lock_);
    std::array<TPromise<T>, 2> promises{
        std::move(PendingPromise_),
        std::move(ActivePromise_)
    };
    guard.Release();
    for (auto& promise : promises) {
        if (promise) {
            promise.TrySet(error);
        }
    }
}

template <class T>
void TAsyncBatcher<T>::OnDeadlineReached()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(Lock_);

    DeadlineReached_ = true;

    if (!ActivePromise_) {
        DoRun(guard);
    }
}

template <class T>
void TAsyncBatcher<T>::DoRun(TGuard<TSpinLock>& guard)
{
    VERIFY_SPINLOCK_AFFINITY(Lock_);

    DeadlineReached_ = false;

    YT_VERIFY(!ActivePromise_);
    swap(ActivePromise_, PendingPromise_);

    guard.Release();

    Provider_
        .Run()
        .Subscribe(BIND(&TAsyncBatcher::OnResult, MakeWeak(this)));
}

template <class T>
void TAsyncBatcher<T>::OnResult(const TErrorOr<T>& result)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(Lock_);
    auto activePromise = std::move(ActivePromise_);

    if (DeadlineReached_) {
        DoRun(guard);
    }

    guard.Release();

    if (activePromise) {
        activePromise.TrySet(result);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
