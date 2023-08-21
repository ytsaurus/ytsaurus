#pragma once

#include <util/generic/ptr.h>
#include <util/generic/typetraits.h>
#include <type_traits>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept CIntrusivePtr = TIsSpecializationOf<TIntrusivePtr, T>::value;

////////////////////////////////////////////////////////////////////////////////

template<class T, class U>
concept SameHelper = std::is_same_v<T, U>;

template< class T, class U >
concept CSameIs = SameHelper<T, U> && SameHelper<U, T>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
