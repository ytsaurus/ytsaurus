#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TPredicate>
size_t LowerBound(size_t lowerIndex, size_t upperIndex, TPredicate less);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ALGORITHM_HELPERS_INL_H_
#include "algorithm_helpers-inl.h"
#undef ALGORITHM_HELPERS_INL_H_
