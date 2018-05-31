#pragma once
#ifndef NUMERIC_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include numeric_helpers.h"
#endif

#include <cstdlib>
#include <cinttypes>
#include <algorithm>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T Clamp(const T& value, const T& minValue, const T& maxValue)
{
    auto result = std::min(value, maxValue);
    result = std::max(result, minValue);
    return result;
}

template <class T>
T DivCeil(const T& numerator, const T& denominator)
{
    auto res = std::div(numerator, denominator);
    return res.quot + (res.rem > static_cast<T>(0) ? static_cast<T>(1) : static_cast<T>(0));
}

// A version of division that is a bit less noisy around the situation when numerator is almost divisible by denominator.
// Round up if the remainder is at least half of denominator, otherwise round down.
template <typename T>
T DivRound(const T& numerator, const T& denominator)
{
    auto res = std::div(numerator, denominator);
    return res.quot + (res.rem >= (denominator + 1) / 2 ? static_cast<T>(1) : static_cast<T>(0));
}

template <class T>
T RoundUp(const T& numerator, const T& denominator)
{
    return DivCeil(numerator, denominator) * denominator;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

