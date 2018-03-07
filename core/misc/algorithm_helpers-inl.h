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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
