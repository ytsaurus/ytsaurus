#pragma once

#ifndef PROCESS_FUNCTION_INL_H_
    #error "Direct inclusion of this file is not allowed, include process_function.h"
    // For the sake of sane code completion.
    #include "process_function.h"
#endif

#include <type_traits>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

// Process functions remain abstract until New<T> supplies ref-counted destruction.
template <class TFunction>
struct TProcessFunctionConstructibilityProbe
    : public TFunction
{
    using TFunction::TFunction;

    void DestroyRefCounted() override
    { }
};

template <class TFunction>
inline constexpr bool ProcessFunctionContextConstructible =
    std::is_constructible_v<TProcessFunctionConstructibilityProbe<TFunction>, const TProcessFunctionContextPtr&>;

template <class TFunction>
inline constexpr bool DefaultProcessFunctionConstructible =
    std::is_default_constructible_v<TProcessFunctionConstructibilityProbe<TFunction>>;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TFunction>
IProcessFunctionBasePtr ConstructProcessFunction(const TProcessFunctionContextPtr& context)
{
    static_assert(std::is_base_of_v<IProcessFunctionBase, TFunction>);
    static_assert(
        NDetail::ProcessFunctionContextConstructible<TFunction> ||
        NDetail::DefaultProcessFunctionConstructible<TFunction>);

    if constexpr (NDetail::ProcessFunctionContextConstructible<TFunction>) {
        return New<TFunction>(context);
    } else {
        return New<TFunction>();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
