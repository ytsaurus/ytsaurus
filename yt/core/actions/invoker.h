#pragma once

#include "common.h"
#include "callback_forward.h"

#include <core/concurrency/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move to public.h
struct IInvoker;
typedef TIntrusivePtr<IInvoker> IInvokerPtr;

struct IPrioritizedInvoker;
typedef TIntrusivePtr<IPrioritizedInvoker> IPrioritizedInvokerPtr;

struct IInvoker
    : public virtual TRefCounted
{
    //! Schedules invocation of a given callback.
    virtual bool Invoke(const TClosure& action) = 0;

    //! Returns the thread id this invoker is bound to.
    //! For invokers not bound to any particular thread,
    //! returns |InvalidThreadId|.
    virtual NConcurrency::TThreadId GetThreadId() const = 0;
};

struct IPrioritizedInvoker
    : public virtual IInvoker
{
    using IInvoker::Invoke;

    virtual bool Invoke(const TClosure& action, i64 priority) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
