#pragma once

#include "action.h"

#include "../misc/common.h"
#include "../misc/common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSyncInvoker
    : public IInvoker
{
public:
    virtual void Invoke(TIntrusivePtr<IAction> action);

    static IInvoker::TPtr Get();
};

////////////////////////////////////////////////////////////////////////////////


class TCancelableInvoker
    : public IInvoker
{
public:
    typedef TIntrusivePtr<TCancelableInvoker> TPtr;

    explicit TCancelableInvoker(IInvoker::TPtr underlyingInvoker);

    virtual void Invoke(TIntrusivePtr<IAction> action);

    void Cancel();
    bool IsCanceled() const;

private:
    volatile bool Canceled;
    IInvoker::TPtr UnderlyingInvoker;

    void ActionThunk(TIntrusivePtr<IAction> action);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
