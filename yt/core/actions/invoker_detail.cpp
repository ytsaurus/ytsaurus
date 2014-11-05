#include "stdafx.h"
#include "invoker_detail.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TInvokerWrapper::TInvokerWrapper(IInvokerPtr underlyingInvoker)
    : UnderlyingInvoker_(std::move(underlyingInvoker))
{
    YCHECK(UnderlyingInvoker_);
}

void TInvokerWrapper::Invoke(const TClosure& callback)
{
    return UnderlyingInvoker_->Invoke(callback);
}

NConcurrency::TThreadId TInvokerWrapper::GetThreadId() const
{
    return UnderlyingInvoker_->GetThreadId();
}

bool TInvokerWrapper::CheckAffinity(IInvokerPtr invoker) const
{
    return
        invoker.Get() == this ||
        UnderlyingInvoker_->CheckAffinity(invoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
