#pragma once

#include <yt/yt/core/ytree/yson_struct.h>  // ::NYT::NYTree::TYsonStruct

#include <util/generic/ptr.h>
#include <util/generic/typetraits.h>
#include <type_traits>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept CIntrusivePtr = TIsSpecializationOf<TIntrusivePtr, T>::value || TIsSpecializationOf<NYT::TIntrusivePtr, T>::value;

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CYsonStruct = std::derived_from<T, ::NYT::NYTree::TYsonStruct>;

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TTemplateParameterTraits;

template <template<class...> class P, class... Ts>
struct TTemplateParameterTraits<P<Ts...>>
{
    using type = TTypeList<Ts...>;
};

template <class T>
using TExtractTemplateArgs = typename TTemplateParameterTraits<T>::type;

template <class T, size_t N = 0>
using TExtractTemplateArg = typename TExtractTemplateArgs<T>::template TGet<N>;

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CYsonStructPtr = CIntrusivePtr<T> && CYsonStruct<TExtractTemplateArg<T>>;

////////////////////////////////////////////////////////////////////////////////

template<class T, class U>
concept SameHelper = std::is_same_v<T, U>;

template< class T, class U >
concept CSameIs = SameHelper<T, U> && SameHelper<U, T>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
