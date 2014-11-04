#pragma once

#include "public.h"

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

    //! Verifies that the caller is executing under the invoker.
    //! If not, then invokes a trap.
    virtual void VerifyAffinity() const = 0;
#endif
};

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
