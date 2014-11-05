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

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
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
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
