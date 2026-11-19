#include "resource.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TResourceMockBase::TResourceMockBase()
    : DestroyedPromise_(NewPromise<void>())
    , DestroyedGuard_(BIND([promise = DestroyedPromise_] {
        promise.Set();
    }))
    , CancelableContext_(New<TCancelableContext>())
{ }

TResourceMockBase::~TResourceMockBase()
{
    Stop();
}

TResourceStatus TResourceMockBase::GetStatus() const
{
    if (CancelableContext_->IsCanceled()) {
        return {.Error = CancelableContext_->GetCancelationError()};
    }
    return {.RevisionState = GetRevisionState()};
}

const TCancelableContextPtr& TResourceMockBase::GetCancelableContext() const
{
    return CancelableContext_;
}

TFuture<void> TResourceMockBase::GetDestroyedFuture() const
{
    return DestroyedPromise_.ToFuture().ToUncancelable();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
