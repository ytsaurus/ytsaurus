#pragma once

#include "common.h"
#include "callback_forward.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IInvoker
    : public virtual TRefCounted
{
    virtual bool Invoke(const TClosure& action) = 0;
};

typedef TIntrusivePtr<IInvoker> IInvokerPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
