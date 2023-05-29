#include "invoker_detail.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TInvokerWrapper::TInvokerWrapper(IInvokerPtr underlyingInvoker)
    : UnderlyingInvoker_(std::move(underlyingInvoker))
{
    YT_VERIFY(UnderlyingInvoker_);
}

void TInvokerWrapper::Invoke(TClosure callback)
{
    return UnderlyingInvoker_->Invoke(std::move(callback));
}

void TInvokerWrapper::Invoke(TMutableRange<TClosure> callbacks)
{
    return UnderlyingInvoker_->Invoke(callbacks);
}

NThreading::TThreadId TInvokerWrapper::GetThreadId() const
{
    return UnderlyingInvoker_->GetThreadId();
}

bool TInvokerWrapper::CheckAffinity(const IInvokerPtr& invoker) const
{
    return
        invoker.Get() == this ||
        UnderlyingInvoker_->CheckAffinity(invoker);
}

bool TInvokerWrapper::IsSerialized() const
{
    return UnderlyingInvoker_->IsSerialized();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
