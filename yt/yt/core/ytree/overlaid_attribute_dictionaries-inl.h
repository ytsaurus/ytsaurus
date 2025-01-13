#ifndef OVERLAID_ATTRIBUTE_DICTIONARIES_INL_H_
#error "Direct inclusion of this file is not allowed, include overlaid_attribute_dictionaries.h"
// For the sake of sane code completion.
#include "overlaid_attribute_dictionaries.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
template <class... Args>
TOverlaidAttributeDictionary<T>::TOverlaidAttributeDictionary(
    Args&&... underlyingDicts)
{
    UnderlyingDictionaries_.reserve(sizeof...(underlyingDicts));
    PushBottom(std::forward<Args>(underlyingDicts)...);
}

template <class T>
template <class U>
void TOverlaidAttributeDictionary<T>::PushBottom(U&& underlyingDict)
{
    UnderlyingDictionaries_.emplace_back(std::forward<U>(underlyingDict));
}

template <class T>
template <class U, class... Args>
void TOverlaidAttributeDictionary<T>::PushBottom(
    U&& underlyingDictsHead,
    Args&&... underlyingDictsTail)
{
    PushBottom(std::forward<U>(underlyingDictsHead));
    PushBottom(std::forward<Args>(underlyingDictsTail)...);
}

template <class T>
auto TOverlaidAttributeDictionary<T>::ListKeys() const -> std::vector<TKey>
{
    std::vector<TKey> result;
    for (const auto& dict : UnderlyingDictionaries_) {
        if (!dict) {
            continue;
        }
        auto dictKeys = dict->ListKeys();
        result.insert(result.end(), dictKeys.begin(), dictKeys.end());
    }
    SortUnique(result);
    return result;
}

template <class T>
auto TOverlaidAttributeDictionary<T>::ListPairs() const -> std::vector<TKeyValuePair>
{
    THashMap<TKey, NYson::TYsonString> result;
    for (int index  = std::ssize(UnderlyingDictionaries_) - 1; index >= 0; --index) {
        const auto& dict = UnderlyingDictionaries_[index];
        if (!dict) {
            continue;
        }
        for (const auto& [key, value]: dict->ListPairs()) {
            result[key] = value;
        }
    }
    return std::vector<TKeyValuePair>(result.begin(), result.end());
}

template <class T>
auto TOverlaidAttributeDictionary<T>::FindYson(TKeyView key) const -> TValue
{
    for (const auto& dict : UnderlyingDictionaries_) {
        if (!dict) {
            continue;
        }
        if (auto value = dict->FindYson(key)) {
            return value;
        }
    }

    return {};
}

template <class T>
void TOverlaidAttributeDictionary<T>::SetYson(TKeyView key, const NYson::TYsonString& value)
{
    auto set = false;
    for (const auto& dict : UnderlyingDictionaries_) {
        if (!dict) {
            continue;
        }

        if (!set) {
            SetYson(*dict, key, value);
            set = true;
        } else {
            Remove(*dict, key);
        }
    }

    if (!set) {
        YT_ABORT();
    }
}

template <class T>
bool TOverlaidAttributeDictionary<T>::Remove(TKeyView key)
{
    auto removed = false;
    for (const auto& dict : UnderlyingDictionaries_) {
        if (!dict) {
            continue;
        }
        if (Remove(*dict, key)) {
            removed = true;
        }
    }

    return removed;
}

template <class T>
void TOverlaidAttributeDictionary<T>::SetYson(
    IAttributeDictionary& dict,
    TKeyView key,
    const NYson::TYsonString& value)
{
    dict.SetYson(key, value);
}

template <class T>
void TOverlaidAttributeDictionary<T>::SetYson(
    const IAttributeDictionary& /*dict*/,
    TKeyView /*key*/,
    const NYson::TYsonString& /*value*/)
{
    // NB: IAttributeDictionary's extension methods require SetYson() to be
    // compilable.
    YT_ABORT();
}

template <class T>
bool TOverlaidAttributeDictionary<T>::Remove(
    IAttributeDictionary& dict,
    TKeyView key)
{
    return dict.Remove(key);
}

template <class T>
bool TOverlaidAttributeDictionary<T>::Remove(
    const IAttributeDictionary& /*dict*/,
    TKeyView /*key*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class... Args>
TIntrusivePtr<TOverlaidAttributeDictionary<typename std::decay<T>::type>>
OverlayAttributeDictionaries(
    T&& topmostUnderlyingDict,
    Args&&... underlyingDicts)
{
    return New<TOverlaidAttributeDictionary<typename std::decay<T>::type>>(
        std::forward<T>(topmostUnderlyingDict),
        std::forward<Args>(underlyingDicts)...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
