#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IAction;

////////////////////////////////////////////////////////////////////////////////

struct IInvoker
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IInvoker> TPtr;

    virtual void Invoke(TIntrusivePtr<IAction> action) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSyncInvoker
    : public IInvoker
{
public:
    virtual void Invoke(TIntrusivePtr<IAction> action);

    static TSyncInvoker* Get();
};

////////////////////////////////////////////////////////////////////////////////

// TODO: move to separate h/cpp
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

}
