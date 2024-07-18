#pragma once

#include <util/generic/ptr.h>
#include <util/generic/typetraits.h>
#include <type_traits>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename>
class TOutput;

class IExecutionContext;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept CParDoArgs =
    requires {
        typename T::TInputRow;
        typename T::TOutputRow;
    }
    && std::constructible_from<T, const typename T::TInputRow&, TOutput<typename T::TOutputRow>&, IExecutionContext&>;

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
