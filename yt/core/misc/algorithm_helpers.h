#pragma once

#include <algorithm>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TIter, class TPredicate>
TIter BinarySearch(TIter begin, TIter end, TPredicate pred);

template <class TIter, class TPredicate>
TIter ExponentialSearch(TIter begin, TIter end, TPredicate pred);

template <class TIter, class T>
TIter LowerBound(TIter begin, TIter end, const T& value);

template <class TIter, class T>
TIter UpperBound(TIter begin, TIter end, const T& value);

template <class TIter, class T>
TIter ExpLowerBound(TIter begin, TIter end, const T& value);

template <class TIter, class T>
TIter ExpUpperBound(TIter begin, TIter end, const T& value);

//! Similar to std::set_intersection, but returns bool and doesn't output the
//! actual intersection.
//! Input ranges must be sorted.
template <class TInputIt1, class TInputIt2>
bool Intersects(TInputIt1 first1, TInputIt1 last1, TInputIt2 first2, TInputIt2 last2);

template <class T, typename TGetKey>
std::pair<const T&, const T&> MinMaxBy(const T& first, const T& second, const TGetKey& getKey);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ALGORITHM_HELPERS_INL_H_
#include "algorithm_helpers-inl.h"
#undef ALGORITHM_HELPERS_INL_H_
