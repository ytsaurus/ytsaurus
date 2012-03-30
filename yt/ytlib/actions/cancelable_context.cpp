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
        IInvoker::TPtr underlyingInvoker)
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

        UnderlyingInvoker->Invoke(Bind([=] {
                if (!Context->Canceled) {
                    action.Run();
                }
            }));
    }

    void Cancel();
    bool IsCanceled() const;

private:
    TCancelableContextPtr Context;
    IInvoker::TPtr UnderlyingInvoker;

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

IInvoker::TPtr TCancelableContext::CreateInvoker(IInvoker::TPtr underlyingInvoker)
{
    return New<TCancelableInvoker>(this, underlyingInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
