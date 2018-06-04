#pragma once
#ifndef ALGORITHM_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include algorithm_helpers.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TPredicate>
size_t LowerBound(size_t lowerIndex, size_t upperIndex, TPredicate less)
{
    while (lowerIndex < upperIndex) {
        auto middle = (upperIndex + lowerIndex) / 2;
        if (less(middle)) {
            lowerIndex = middle + 1;
        } else {
            upperIndex = middle;
        }
    }
    return lowerIndex;
}

template <class TInputIt1, class TInputIt2>
bool Intersects(TInputIt1 first1, TInputIt1 last1, TInputIt2 first2, TInputIt2 last2)
{
    while (first1 != last1 && first2 != last2) {
        if (*first1 < *first2) {
            ++first1;
        } else if (*first2 < *first1) {
            ++first2;
        } else {
            return true;
        }

    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
