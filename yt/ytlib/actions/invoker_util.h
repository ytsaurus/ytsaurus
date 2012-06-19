#pragma once

#include "common.h"
#include "callback_forward.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move to cpp, leave GetSyncInvoker in h
class TSyncInvoker
    : public IInvoker
{
public:
    virtual void Invoke(const TClosure& action);

    static IInvokerPtr Get();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
