#ifndef TRANSPARENT_PAIR_COMPARE_INL_H_
#error "Direct inclusion of this file is not allowed, include transparent_pair_compare.h"
#endif

#include "transparent_pair_compare.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <CComparableViaLess T, CComparableViaLess U>
bool TTransparentPairCompare<T, U>::operator()(const std::pair<T, U>& lhs, const std::pair<T, U>& rhs) const
{
    return lhs < rhs;
}

template <CComparableViaLess T, CComparableViaLess U>
bool TTransparentPairCompare<T, U>::operator()(typename NMpl::TCallTraits<T>::TType lhs, const std::pair<T, U>& rhs) const
{
    if (!(lhs < rhs.first || rhs.first < lhs)) {
        return true;
    }
    return lhs < rhs.first;
}

template <CComparableViaLess T, CComparableViaLess U>
bool TTransparentPairCompare<T, U>::operator()(const std::pair<T, U>& lhs, typename NMpl::TCallTraits<T>::TType rhs) const
{
    if (!(lhs.first < rhs || rhs < lhs.first)) {
        return false;
    }
    return lhs.first < rhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
