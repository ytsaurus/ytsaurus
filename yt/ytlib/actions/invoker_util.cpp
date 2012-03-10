#include "stdafx.h"
#include "invoker_util.h"
#include "action.h"
#include "action_util.h"

#include <ytlib/misc/singleton.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TSyncInvoker::Invoke(IAction::TPtr action)
{
    action->Do();
}

IInvoker* TSyncInvoker::Get()
{
    return ~RefCountedSingleton<TSyncInvoker>();
}

////////////////////////////////////////////////////////////////////////////////

class TCancelableContext::TCancelableInvoker
    : public IInvoker
{
public:
    typedef TIntrusivePtr<TCancelableInvoker> TPtr;

    TCancelableInvoker(
        TCancelableContext* context,
        IInvoker* underlyingInvoker)
        : Context(context)
        , UnderlyingInvoker(underlyingInvoker)
    {
        YASSERT(underlyingInvoker);
    }

    virtual void Invoke(TIntrusivePtr<IAction> action)
    {
        YASSERT(action);

        if (Context->Canceled)
            return;

        auto context = Context;
        UnderlyingInvoker->Invoke(FromFunctor([=] {
                if (!context->Canceled) {
                    action->Do();
                }
            }));
    }

    void Cancel();
    bool IsCanceled() const;

private:
    TCancelableContextPtr Context;
    IInvoker::TPtr UnderlyingInvoker;

    void ActionThunk(TIntrusivePtr<IAction> action);

};

TCancelableContext::TCancelableContext()
    : Canceled(false)
{ }

bool TCancelableContext::IsCanceled() const
{
    return Canceled;
}

void TCancelableContext::Cancel()
{
    Canceled = true;
}

IInvoker::TPtr TCancelableContext::CreateInvoker(IInvoker* underlyingInvoker)
{
    return New<TCancelableInvoker>(this, underlyingInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT