#ifndef ATTRIBUTE_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include attribute_helpers.h"
#endif
#undef ATTRIBUTE_HELPERS_INL_H_

#include "attribute_consumer.h"
#include "convert.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T IAttributeDictionary::Get(const Stroka& key) const
{
    auto yson = GetYson(key);
    return ConvertTo<T>(yson);
}

template <class T>
T IAttributeDictionary::Get(const Stroka& key, const T& defaultValue) const
{
    return Find<T>(key).Get(defaultValue);
}

template <class T>
typename TNullableTraits<T>::TNullableType IAttributeDictionary::Find(const Stroka& key) const
{
    auto yson = FindYson(key);
    if (!yson) {
        return typename TNullableTraits<T>::TNullableType();
    }
    return ConvertTo<T>(*yson);
}

template <class T>
void IAttributeDictionary::Set(const Stroka& key, const T& value)
{
    auto yson = ConvertToYsonString(value, EYsonFormat::Binary);
    SetYson(key, yson);
}

template <>
inline void IAttributeDictionary::Set(const Stroka& key, const Stroka& value)
{
    Set(key, TRawString(value));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
