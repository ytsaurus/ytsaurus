#pragma once

#include "common.h"
#include "callback_forward.h"

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
    virtual bool Invoke(const TClosure& action) = 0;
};

struct IPrioritizedInvoker
    : public virtual IInvoker
{
    using IInvoker::Invoke;

    virtual bool Invoke(const TClosure& action, i64 priority) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
