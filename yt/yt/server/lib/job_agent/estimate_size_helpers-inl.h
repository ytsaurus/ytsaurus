#ifndef ESTIMATE_SIZE_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include estimate_size_helpers.h"
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

template <typename E>
    requires TEnumTraits<E>::IsEnum
size_t EstimateSize(E /*value*/)
{
    return EstimatedValueSize;
}

////////////////////////////////////////////////////////////////////////////////

template <typename... Ts>
size_t EstimateSizes(Ts&&...values)
{
    return (EstimateSize(std::forward<Ts>(values)) + ... + 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
