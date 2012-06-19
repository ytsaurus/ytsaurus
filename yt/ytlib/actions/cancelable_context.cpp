#include "stdafx.h"
#include "cancelable_context.h"
#include "bind.h"
#include "callback.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCancelableContext::TCancelableInvoker
    : public IInvoker
{
public:
    typedef TIntrusivePtr<TCancelableInvoker> TPtr;

    TCancelableInvoker(
        TCancelableContextPtr context,
        IInvokerPtr underlyingInvoker)
        : Context(context)
        , UnderlyingInvoker(underlyingInvoker)
    {
        YASSERT(underlyingInvoker);
    }

    virtual void Invoke(const TClosure& action)
    {
        YASSERT(!action.IsNull());

        if (Context->Canceled)
            return;

        auto context = Context;
        UnderlyingInvoker->Invoke(BIND([=] {
            if (!context->Canceled) {
                action.Run();
            }
        }));
    }

    void Cancel();
    bool IsCanceled() const;

private:
    TCancelableContextPtr Context;
    IInvokerPtr UnderlyingInvoker;

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

IInvokerPtr TCancelableContext::CreateInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TCancelableInvoker>(this, underlyingInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
