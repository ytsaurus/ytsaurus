#ifndef DYNAMIC_STATE_INL_H_
#error "Direct inclusion of this file is not allowed, include dynamic_state.h"
// For the sake of sane code completion.
#include "dynamic_state.h"
#endif

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/backoff_strategy.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

template <typename TRow, typename TRecordDescriptor>
template <typename R>
TFuture<R> TTableBase<TRow, TRecordDescriptor>::RetryCallback(
    TCallback<TFuture<R>()> callback,
    IInvokerPtr invoker) const
{
    return BIND([callback = std::move(callback), retryBackoffOptions = RetryBackoffOptions_.Load(), path = Path_] () -> R {
        const auto& Logger = QueueClientLogger();

        auto resultOrError = NConcurrency::WaitFor(callback());
        if (resultOrError.IsOK()) {
            return std::move(resultOrError).Value();
        }

        TBackoffStrategy retryBackoffStrategy(retryBackoffOptions);

        while (retryBackoffStrategy.Next()) {
            YT_TLOG_DEBUG("Dynamic state request attempt failed, backing off")
                .With("Path", path)
                .With("Retry", retryBackoffStrategy.GetInvocationIndex())
                .With(resultOrError);

            NConcurrency::TDelayedExecutor::WaitForDuration(retryBackoffStrategy.GetBackoff());

            resultOrError = NConcurrency::WaitFor(callback());
            if (resultOrError.IsOK()) {
                return std::move(resultOrError).Value();
            }
        }

        THROW_ERROR_EXCEPTION("Dynamic state request to %v failed after %v retries",
            path,
            retryBackoffStrategy.GetInvocationCount())
            .With(TError(std::move(resultOrError)));
    })
        .AsyncVia(std::move(invoker))
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
