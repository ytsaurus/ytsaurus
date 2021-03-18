#pragma once
#ifndef ESTIMATE_SIZE_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include format.h"
// For the sake of sane code completion.
#include "estimate_size_helpers.h"
#endif

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
size_t EstimateSize(const std::optional<T>& v)
{
    return v ? EstimateSize(*v) : 0;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename... U>
size_t EstimateSizes(T&& t, U&& ... u)
{
    return EstimateSize(std::forward<T>(t)) + EstimateSizes(std::forward<U>(u)...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
