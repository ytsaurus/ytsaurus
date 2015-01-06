#pragma once

#include <core/misc/public.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TActionQueue)
DECLARE_REFCOUNTED_CLASS(TFairShareActionQueue)
DECLARE_REFCOUNTED_CLASS(TThreadPool)
DECLARE_REFCOUNTED_CLASS(TParallelAwaiter)
DECLARE_REFCOUNTED_CLASS(TPeriodicExecutor)
DECLARE_REFCOUNTED_CLASS(TFiber)

DECLARE_REFCOUNTED_STRUCT(TDelayedExecutorEntry)
typedef TDelayedExecutorEntryPtr TDelayedExecutorCookie;

DECLARE_REFCOUNTED_CLASS(TThroughputThrottlerConfig)
DECLARE_REFCOUNTED_STRUCT(IThroughputThrottler)

DECLARE_REFCOUNTED_STRUCT(IAsyncInputStream)
DECLARE_REFCOUNTED_STRUCT(IAsyncOutputStream)

DECLARE_REFCOUNTED_STRUCT(IAsyncZeroCopyInputStream)
DECLARE_REFCOUNTED_STRUCT(IAsyncZeroCopyOutputStream)

class TAsyncSemaphore;

class TFiber;

template <class TSignature>
class TCoroutine;

typedef size_t TThreadId;
const size_t InvalidThreadId = 0;

typedef size_t TFiberId;
const size_t InvalidFiberId = 0;

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
