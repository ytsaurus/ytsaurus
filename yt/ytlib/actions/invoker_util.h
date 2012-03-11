#pragma once

#include "common.h"
#include "action.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move to cpp, leave GetSyncInvoker in h
class TSyncInvoker
    : public IInvoker
{
public:
    virtual void Invoke(TIntrusivePtr<IAction> action);

    static IInvoker* Get();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
