#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T Clamp(const T& value, const T& minValue, const T& maxValue);

template <class T>
T DivCeil(const T& numerator, const T& denominator);

template <class T>
T RoundUp(const T& numerator, const T& denominator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define NUMERIC_HELPERS_INL_H_
#include "numeric_helpers-inl.h"
#undef NUMERIC_HELPERS_INL_H_
