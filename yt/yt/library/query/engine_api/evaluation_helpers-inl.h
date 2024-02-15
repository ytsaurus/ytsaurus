#ifndef EVALUATION_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include evaluation_helpers.h"
// For the sake of sane code completion.
#include "evaluation_helpers.h"
#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class T, class... TArgs>
int TCGVariables::AddOpaque(TArgs&& ... args)
{
    auto pointer = Holder_.Register(new T(std::forward<TArgs>(args)...));

    int index = static_cast<int>(OpaquePointers_.size());
    OpaquePointers_.push_back(pointer);
    OpaquePointeeSizes_.push_back(std::is_trivially_copyable_v<T> ? sizeof(T) : 0);

    return index;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
