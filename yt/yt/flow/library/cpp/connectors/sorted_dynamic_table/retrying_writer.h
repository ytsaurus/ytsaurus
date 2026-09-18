#pragma once

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NFlow::NSortedDynamicTable::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TObject, class TAttempt>
TFuture<void> RunSerializedRetries(
    TWeakPtr<TObject> weakObject,
    NConcurrency::TAsyncSemaphorePtr semaphore,
    IInvokerPtr invoker,
    TExponentialBackoffOptions backoffOptions,
    TAttempt attempt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NSortedDynamicTable::NDetail

#include "retrying_writer-inl.h"
