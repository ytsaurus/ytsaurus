#pragma once
#ifndef PROFILE_MANAGER_INL_H_
#error "Direct inclusion of this file is not allowed, include profile_manager.h"
// For the sake of sane code completion
#include "profile_manager.h"
#endif
#undef PROFILE_MANAGER_INL_H_

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T, class = void>
struct TTagNameFormatter
{
    static TString Do(const T& value)
    {
        using ::ToString;
        return ToString(value);
    }
};

template <class T>
struct TTagNameFormatter<T, typename std::enable_if<TEnumTraits<T>::IsEnum>::type>
{
    static TString Do(T value)
    {
        return FormatEnum(value);
    }
};

} // namespace

template <class T>
TTagId TProfileManager::RegisterTag(const TString& key, const T& value)
{
    return RegisterTag(TTag{
        key,
        TTagNameFormatter<T>::Do(value)
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
