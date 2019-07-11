#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Batches requests to the underlying provider.
/*!
 *  This is, in essence, forward batching; i.e. TAsyncBatcher::Run returns
 *  the result obtained by calling the provider _after_ the original call to TAsyncBatcher::Run.
 */
template <class T>
class TAsyncBatcher
    : public TRefCounted
{
public:
    TAsyncBatcher(TCallback<TFuture<T>()> provider, TDuration batchingDelay);

    TFuture<T> Run();

    void Cancel(const TError& error);

private:
    const TCallback<TFuture<T>()> Provider_;
    const TDuration BatchingDelay_;

    TSpinLock Lock_;
    TPromise<T> ActivePromise_;
    TPromise<T> PendingPromise_;
    bool DeadlineReached_ = false;

    void OnDeadlineReached();
    void DoRun(TGuard<TSpinLock>& guard);
    void OnResult(const TErrorOr<T>& result);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define ASYNC_BATCHER_INL_H_
#include "async_batcher-inl.h"
#undef ASYNC_BATCHER_INL_H_
