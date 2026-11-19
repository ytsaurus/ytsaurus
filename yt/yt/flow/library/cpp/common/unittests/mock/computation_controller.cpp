#include "computation_controller.h"

#include <library/cpp/yt/memory/new.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TComputationControllerMockBase::TComputationControllerMockBase()
    : CancelableContext_(New<TCancelableContext>())
{ }

const TCancelableContextPtr& TComputationControllerMockBase::GetCancelableContext() const
{
    return CancelableContext_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
