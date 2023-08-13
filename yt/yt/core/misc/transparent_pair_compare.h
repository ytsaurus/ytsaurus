#pragma once

#include <utility>

#include <concepts>

#include "public.h"

#include <yt/yt/core/misc/mpl.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CComparableViaLess = requires(const T& t)
{
    {t < t} -> std::convertible_to<bool>;
};

template <CComparableViaLess T, CComparableViaLess U>
struct TTransparentPairCompare
{
    using is_transparent = void;

    bool operator()(const std::pair<T, U>& lhs, const std::pair<T, U>& rhs) const;
    bool operator()(typename NMpl::TCallTraits<T>::TType lhs, const std::pair<T, U>& rhs) const;
    bool operator()(const std::pair<T, U>& lhs, typename NMpl::TCallTraits<T>::TType rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define TRANSPARENT_PAIR_COMPARE_INL_H_
#include "transparent_pair_compare-inl.h"
#undef TRANSPARENT_PAIR_COMPARE_INL_H_
