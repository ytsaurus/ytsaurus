#pragma once

#include "callback.h"

#include <core/concurrency/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IInvoker
    : public virtual TRefCounted
{
    //! Schedules invocation of a given callback.
    virtual void Invoke(const TClosure& callback) = 0;

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    //! Returns the thread id this invoker is bound to.
    //! For invokers not bound to any particular thread,
    //! returns |InvalidThreadId|.
    virtual NConcurrency::TThreadId GetThreadId() const = 0;

    //! Returns |true| if this invoker is either equal to #invoker or wraps it,
    //! in some sense.
    virtual bool CheckAffinity(IInvokerPtr invoker) const = 0;
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
    virtual void Invoke(const TClosure& callback, i64 priority) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPrioritizedInvoker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define INVOKER_INL_H_
#include "invoker-inl.h"
#undef INVOKER_INL_H_

