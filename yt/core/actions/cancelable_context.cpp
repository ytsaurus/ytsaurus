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
        : Context_(std::move(context))
        , UnderlyingInvoker_(std::move(underlyingInvoker))
    {
        YCHECK(Context_);
        YCHECK(UnderlyingInvoker_);
    }

    virtual void Invoke(const TClosure& callback) override
    {
        YASSERT(callback);

        if (Context_->Canceled)
            return;

        auto this_ = MakeStrong(this);
        return UnderlyingInvoker_->Invoke(BIND([this, this_, callback] {
            if (!Context_->Canceled) {
                TCurrentInvokerGuard guard(this_);
                callback.Run();
            }
        }));
    }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual void VerifyAffinity() const override
    {
        UnderlyingInvoker_->VerifyAffinity();
    }

    virtual NConcurrency::TThreadId GetThreadId() const override
    {
        return UnderlyingInvoker_->GetThreadId();
    }
#endif

private:
    TCancelableContextPtr Context_;
    IInvokerPtr UnderlyingInvoker_;

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
