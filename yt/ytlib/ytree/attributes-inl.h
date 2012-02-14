#ifndef ATTRIBUTES_INL_H_
#error "Direct inclusion of this file is not allowed, include attributes.h"
#endif
#undef ATTRIBUTES_INL_H_

#include "serialize.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
typename TDeserializeTraits<T>::TReturnType IAttributeDictionary::Get(const Stroka& name)
{
    const auto& yson = GetYson(name);
    return DeserializeFromYson<T>(yson);
}

template <class T>
typename TNullableTraits<
    typename TDeserializeTraits<T>::TReturnType
>::TNullableType IAttributeDictionary::Find(const Stroka& name)
{
    const auto& yson = FindYson(name);
    if (yson.empty()) {
        return
            typename TNullableTraits<
                typename TDeserializeTraits<T>::TReturnType
            >::TNullableType();
    }
    return DeserializeFromYson<T>(yson);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
