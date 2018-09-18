#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TPredicate>
size_t LowerBound(size_t lowerIndex, size_t upperIndex, TPredicate less);

//! Similar to std::set_intersection, but returns bool and doesn't output the
//! actual intersection.
//! Input ranges must be sorted.
template <class TInputIt1, class TInputIt2>
bool Intersects(TInputIt1 first1, TInputIt1 last1, TInputIt2 first2, TInputIt2 last2);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ALGORITHM_HELPERS_INL_H_
#include "algorithm_helpers-inl.h"
#undef ALGORITHM_HELPERS_INL_H_
