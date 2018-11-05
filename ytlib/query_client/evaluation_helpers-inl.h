#pragma once

#ifndef EVALUATION_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include evaluation_helpers.h"
// For the sake of sane code completion.
#include "evaluation_helpers.h"
#endif

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class T, class... TArgs>
int TCGVariables::AddOpaque(TArgs&& ... args)
{
    int index = static_cast<int>(OpaquePointers_.size());

    auto pointer = new T(std::forward<TArgs>(args)...);
    auto deleter = [] (void* ptr) {
        static_assert(sizeof(T) > 0, "Cannot delete incomplete type.");
        delete static_cast<T*>(ptr);
    };

    std::unique_ptr<void, void(*)(void*)> holder(pointer, deleter);
    YCHECK(holder);

    OpaqueValues_.push_back(std::move(holder));
    OpaquePointers_.push_back(pointer);

    return index;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

