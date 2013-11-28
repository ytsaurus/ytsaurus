#include "stdafx.h"
#include "cancelable_context.h"
#include "callback.h"
#include "invoker_util.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCancelableContext::TCancelableInvoker
    : public IInvoker
{
public:
    TCancelableInvoker(
        TCancelableContextPtr context,
        IInvokerPtr underlyingInvoker)
        : Context(std::move(context))
        , UnderlyingInvoker(std::move(underlyingInvoker))
    {
        YCHECK(Context);
        YCHECK(UnderlyingInvoker);
    }

    virtual bool Invoke(const TClosure& action) override
    {
        YASSERT(action);

        if (Context->Canceled) {
            return false;
        }

        auto this_ = MakeStrong(this);
        return UnderlyingInvoker->Invoke(BIND([this, this_, action] {
            if (!Context->Canceled) {
                TCurrentInvokerGuard guard(this_);
                action.Run();
            }
        }));
    }

    virtual NConcurrency::TThreadId GetThreadId() const override
    {
        return UnderlyingInvoker->GetThreadId();
    }

private:
    TCancelableContextPtr Context;
    IInvokerPtr UnderlyingInvoker;

};

////////////////////////////////////////////////////////////////////////////////

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
    return New<TCancelableInvoker>(this, std::move(underlyingInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
