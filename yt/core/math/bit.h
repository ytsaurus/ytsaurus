#pragma once

#include <type_traits>
#include <cstring>

namespace NYT {

// See: |std::bit_cast| from header <bit> from C++2a.
// Remove this implementation and use the standard one when it becomes available.
template <class TTo, class TFrom>
TTo BitCast(const TFrom &src) noexcept
{
    static_assert(sizeof(TTo) == sizeof(TFrom));
    static_assert(std::is_trivially_copyable_v<TFrom>);
    static_assert(std::is_trivial_v<TTo>);

    TTo dst;
    std::memcpy(&dst, &src, sizeof(TTo));
    return dst;
}


// See: |std::midpoint| from header <numeric> from C++2a.
// Remove this implementation and use the standard one when it becomes available.
template<typename TInt>
TInt Midpoint(TInt a, TInt b) noexcept
{
    static_assert(std::is_integral_v<TInt>);
    static_assert(std::is_same_v<std::remove_cv_t<TInt>, TInt>);
    static_assert(!std::is_same_v<TInt, bool>);

    using TUInt = std::make_unsigned_t<TInt>;

    return TInt(TUInt(a) + (TUInt(b) - TUInt(a)) / 2);
}

} // namespace NYT
