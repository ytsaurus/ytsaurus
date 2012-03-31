#pragma once

#include "common.h"
#include "callback_forward.h"

#include <ytlib/misc/ref_counted.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IInvoker
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IInvoker> TPtr;

    virtual void Invoke(const TClosure& action) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
