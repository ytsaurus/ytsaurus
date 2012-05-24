#ifndef ATTRIBUTES_INL_H_
#error "Direct inclusion of this file is not allowed, include attributes.h"
#endif
#undef ATTRIBUTES_INL_H_

#include "serialize.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
typename TDeserializeTraits<T>::TReturnType IAttributeDictionary::Get(const Stroka& key) const
{
    const auto& yson = GetYson(key);
    return DeserializeFromYson<T>(yson);
}

template <class T>
T IAttributeDictionary::Get(const Stroka& key, const T& defaultValue) const
{
    return Find<T>(key).Get(defaultValue);
}

template <class T>
typename TNullableTraits<
    typename TDeserializeTraits<T>::TReturnType
>::TNullableType IAttributeDictionary::Find(const Stroka& key) const
{
    const auto& yson = FindYson(key);
    if (!yson) {
        return
            typename TNullableTraits<
                typename TDeserializeTraits<T>::TReturnType
            >::TNullableType();
    }
    return DeserializeFromYson<T>(*yson);
}

template <class T>
void IAttributeDictionary::Set(const Stroka& key, const T& value)
{
    const auto& yson = SerializeToYson(value);
    SetYson(key, yson);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
