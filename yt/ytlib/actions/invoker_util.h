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

// TODO(babenko): move to cpp, leave CreateCancelableInvoker in h
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
