#pragma once

#include "callback.h"

#include <yt/core/concurrency/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IInvoker
    : public virtual TRefCounted
{
    //! Schedules invocation of a given callback.
    virtual void Invoke(TClosure callback) = 0;

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    //! Returns the thread id this invoker is bound to.
    //! For invokers not bound to any particular thread,
    //! returns |InvalidThreadId|.
    virtual NConcurrency::TThreadId GetThreadId() const = 0;

    //! Returns |true| if this invoker is either equal to #invoker or wraps it,
    //! in some sense.
    virtual bool CheckAffinity(const IInvokerPtr& invoker) const = 0;
#endif
};

DEFINE_REFCOUNTED_TYPE(IInvoker)

////////////////////////////////////////////////////////////////////////////////

struct IPrioritizedInvoker
    : public virtual IInvoker
{
    using IInvoker::Invoke;

    //! Schedules invocation of a given callback with a given priority.
    /*
     *  Larger priority values dominate over smaller ones.
     *  
     *  While a typical invoker executes callbacks in the order they were
     *  enqueued via IInvoker::Invoke (holds for most but not all invoker types),
     *  callbacks enqueued via IPrioritizedInvoker::Invoke are subject to reordering.
     */
    virtual void Invoke(TClosure callback, i64 priority) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPrioritizedInvoker)

////////////////////////////////////////////////////////////////////////////////

struct ISuspendableInvoker
    : public virtual IInvoker
{
    using IInvoker::Invoke;

    //! Puts invoker into suspended mode.
    /*
     *  Warning: This function is not thread-safe.
     *  When all currently executing callbacks will be finished, returned future will be set.
     *  All incoming callbacks will be queued until Resume is called.
     */
    virtual TFuture<void> Suspend() = 0;

    //! Puts invoker out of suspended mode.
    /*
     *  Warning: This function is not thread-safe.
     *  All queued callbacks will be at once submitted to the underlying invoker.
     *  All incoming callbacks will be at once propagated to underlying invoker.
     */
    virtual void Resume() = 0;

    //! Returns true when invoker is suspended (i.e. no callbacks are submitted
    virtual bool IsSuspended() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISuspendableInvoker)

///////////////////////////////////////////n/////////////////////////////////////

} // namespace NYT

#define INVOKER_INL_H_
#include "invoker-inl.h"
#undef INVOKER_INL_H_

