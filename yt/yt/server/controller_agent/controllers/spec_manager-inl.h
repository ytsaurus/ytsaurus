#ifndef SPEC_MANAGER_INL_H
#error "Direct inclusion of this file is not allowed, include spec_manager.h"
// For the sake of sane code completion.
#include "spec_manager.h"
#endif

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

template <class TSpec>
TIntrusivePtr<TSpec> TSpecManager::GetSpec() const
{
    auto specBase = DynamicSpec_.Acquire();
    if constexpr (std::is_same_v<TSpec, NScheduler::TOperationSpecBase>) {
        return specBase;
    }
    return DynamicPointerCast<TSpec>(specBase);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
