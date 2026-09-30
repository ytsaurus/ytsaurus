#pragma once

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/config.h>

namespace NYT::NFlow::NSortedDynamicTable::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TObject, class TAttempt>
TFuture<void> RunSerializedRetries(
    TWeakPtr<TObject> weakObject,
    NConcurrency::TAsyncSemaphorePtr semaphore,
    IInvokerPtr invoker,
    TExponentialBackoffOptions backoffOptions,
    TAttempt attempt)
{
    return semaphore->AsyncAcquire().AsUnique().Apply(
        BIND([
            weakObject = std::move(weakObject),
            backoffOptions,
            attempt = std::move(attempt)] (NConcurrency::TAsyncSemaphoreGuard&& /*guard*/) mutable {
            TBackoffStrategy backoffStrategy(backoffOptions);
            while (true) {
                {
                    auto lockedObject = weakObject.Lock();
                    if (!lockedObject || attempt(lockedObject.Get())) {
                        return;
                    }
                }

                backoffStrategy.Next();
                NConcurrency::TDelayedExecutor::WaitForDuration(backoffStrategy.GetBackoff());
            }
        })
            .AsyncVia(std::move(invoker)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NSortedDynamicTable::NDetail
