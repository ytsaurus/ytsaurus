#pragma once
#ifndef CAST_INL_H_
#error "Direct inclusion of this file is not allowed, include cast.h"
#endif

#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T, class S>
typename std::enable_if<std::is_signed<T>::value && std::is_signed<S>::value, bool>::type IsInIntegralRange(S value)
{
    return value >= std::numeric_limits<T>::min() && value <= std::numeric_limits<T>::max();
}

template <class T, class S>
static typename std::enable_if<std::is_signed<T>::value && std::is_unsigned<S>::value, bool>::type IsInIntegralRange(S value)
{
    return value <= static_cast<typename std::make_unsigned<T>::type>(std::numeric_limits<T>::max());
}

template <class T, class S>
static typename std::enable_if<std::is_unsigned<T>::value && std::is_signed<S>::value, bool>::type IsInIntegralRange(S value)
{
    return value >= 0 && static_cast<typename std::make_unsigned<S>::type>(value) <= std::numeric_limits<T>::max();
}

template <class T, class S>
typename std::enable_if<std::is_unsigned<T>::value && std::is_unsigned<S>::value, bool>::type IsInIntegralRange(S value)
{
    return value <= std::numeric_limits<T>::max();
}

} // namespace NDetail

template <class T, class S>
bool TryIntegralCast(S value, T* result)
{
    if (!NDetail::IsInIntegralRange<T>(value)) {
        return false;
    }
    *result = static_cast<T>(value);
    return true;
}

template <class T, class S>
T CheckedIntegralCast(S value)
{
    T result;
    if (!TryIntegralCast<T>(value, &result)) {
        THROW_ERROR_EXCEPTION("Argument value %v is out of expected integral range [%v,%v]",
            value,
            std::numeric_limits<T>::min(),
            std::numeric_limits<T>::max());
    }
    return result;
}

template <class T, class S>
bool TryEnumCast(S value, T* result)
{
    auto candidate = static_cast<T>(value);
    if (!TEnumTraits<T>::FindLiteralByValue(candidate)) {
        return false;
    }
    *result = candidate;
    return true;
}

template <class T, class S>
T CheckedEnumCast(S value)
{
    T result;
    if (!TryEnumCast<T>(value, &result)) {
        THROW_ERROR_EXCEPTION("Invalid value %v of enum type %v",
            static_cast<int>(value),
            TEnumTraits<T>::GetTypeName());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
