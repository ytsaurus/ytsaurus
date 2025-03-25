#pragma once

#include <yt/yt/core/ytree/yson_struct.h>  // ::NYT::NYTree::TYsonStruct

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
concept CIntrusivePtr = TIsSpecializationOf<TIntrusivePtr, T>::value || TIsSpecializationOf<NYT::TIntrusivePtr, T>::value;

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CYsonStruct = std::derived_from<T, ::NYT::NYTree::TYsonStruct>;

////////////////////////////////////////////////////////////////////////////////

template <class X>
struct TTemplateParameterTraits;

template <template<class> class P, class T>
struct TTemplateParameterTraits<P<T>>
{
    using type = T;
};

template <class T>
using TemplateParameterType = typename TTemplateParameterTraits<T>::type;

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CYsonStructPtr = CIntrusivePtr<T> && CYsonStruct<TemplateParameterType<T>>;

////////////////////////////////////////////////////////////////////////////////

template<class T, class U>
concept SameHelper = std::is_same_v<T, U>;

template< class T, class U >
concept CSameIs = SameHelper<T, U> && SameHelper<U, T>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
