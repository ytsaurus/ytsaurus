#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class S>
bool TryIntegralCast(S value, T* result);

template <class T, class S>
T CheckedIntegralCast(S value);

template <class T, class S>
T TryEnumCast(S value);

template <class T, class S>
T CheckedEnumCast(S value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CAST_INL_H_
#include "cast-inl.h"
#undef CAST_INL_H_
