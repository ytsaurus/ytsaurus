#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TEnqueuedAction;

class TMpmcQueueImpl;
class TMpscQueueImpl;

template <class TQueueImpl>
class TInvokerQueue;

template <class TQueueImpl>
using TInvokerQueuePtr = TIntrusivePtr<TInvokerQueue<TQueueImpl>>;

using TMpmcInvokerQueue = TInvokerQueue<TMpmcQueueImpl>;
using TMpmcInvokerQueuePtr = TIntrusivePtr<TMpmcInvokerQueue>;

using TMpscInvokerQueue = TInvokerQueue<TMpscQueueImpl>;
using TMpscInvokerQueuePtr = TIntrusivePtr<TMpscInvokerQueue>;

template <class TQueueImpl>
class TSingleQueueSchedulerThread;

template <class TQueueImpl>
using TSingleQueueSchedulerThreadPtr = TIntrusivePtr<TSingleQueueSchedulerThread<TQueueImpl>>;

using TMpmcSingleQueueSchedulerThread = TSingleQueueSchedulerThread<TMpmcQueueImpl>;
using TMpmcSingleQueueSchedulerThreadPtr = TIntrusivePtr<TMpmcSingleQueueSchedulerThread>;

using TMpscSingleQueueSchedulerThread = TSingleQueueSchedulerThread<TMpscQueueImpl>;
using TMpscSingleQueueSchedulerThreadPtr = TIntrusivePtr<TMpscSingleQueueSchedulerThread>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TFairShareInvokerQueue)
DECLARE_REFCOUNTED_CLASS(TFairShareQueueSchedulerThread)
DECLARE_REFCOUNTED_STRUCT(IFairShareCallbackQueue)

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ConcurrencyLogger;
extern const NProfiling::TProfiler ConcurrencyProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
