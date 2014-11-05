#include "stdafx.h"
#include "cancelable_context.h"
#include "callback.h"
#include "invoker_util.h"
#include "invoker_detail.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCancelableContext::TCancelableInvoker
    : public TInvokerWrapper
{
public:
    TCancelableInvoker(
        TCancelableContextPtr context,
        IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
        , Context_(std::move(context))
    {
        YCHECK(Context_);
    }

    virtual void Invoke(const TClosure& callback) override
    {
        YASSERT(callback);

        if (Context_->Canceled_)
            return;

        auto this_ = MakeStrong(this);
        return UnderlyingInvoker_->Invoke(BIND([this, this_, callback] {
            if (!Context_->Canceled_) {
                TCurrentInvokerGuard guard(this_);
                callback.Run();
            }
        }));
    }

private:
    TCancelableContextPtr Context_;

};

////////////////////////////////////////////////////////////////////////////////

bool TCancelableContext::IsCanceled() const
{
    return Canceled_;
}

void TCancelableContext::Cancel()
{
    Canceled_ = true;
}

IInvokerPtr TCancelableContext::CreateInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TCancelableInvoker>(this, std::move(underlyingInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
