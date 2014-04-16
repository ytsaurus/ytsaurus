#pragma once

#include <core/misc/common.h>

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

DECLARE_REFCOUNTED_CLASS(TThroughputThrottlerConfig)
DECLARE_REFCOUNTED_STRUCT(IThroughputThrottler)

DECLARE_REFCOUNTED_STRUCT(IAsyncInputStream)
DECLARE_REFCOUNTED_STRUCT(IAsyncOutputStream)

class TAsyncSemaphore;

template <class TSignature>
class TCoroutine;

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
