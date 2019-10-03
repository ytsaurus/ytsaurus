#ifndef OVERLAID_ATTRIBUTE_DICTIONARIES_INL_H_
#error "Direct inclusion of this file is not allowed, include overlaid_attribute_dictionaries.h"
// For the sake of sane code completion.
#include "overlaid_attribute_dictionaries.h"
#endif

namespace NYT::NYTree {

///////////////////////////////////////////////////////////////////////////////

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
std::vector<TString> TOverlaidAttributeDictionary<T>::ListKeys() const
{
    std::vector<TString> result;
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
std::vector<NYTree::IAttributeDictionary::TKeyValuePair> TOverlaidAttributeDictionary<T>::ListPairs() const
{
    THashMap<TString, NYson::TYsonString> result;
    for (int index  = static_cast<int>(UnderlyingDictionaries_.size()) - 1; index >= 0; --index) {
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
NYson::TYsonString TOverlaidAttributeDictionary<T>::FindYson(TStringBuf key) const
{
    for (const auto& dict : UnderlyingDictionaries_) {
        if (!dict) {
            continue;
        }
        auto value = dict->FindYson(key);
        if (value) {
            return value;
        }
    }

    return {};
}

template <class T>
void TOverlaidAttributeDictionary<T>::SetYson(const TString& key, const NYson::TYsonString& value)
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
bool TOverlaidAttributeDictionary<T>::Remove(const TString& key)
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
    const TString& key,
    const NYson::TYsonString& value)
{
    dict.SetYson(key, value);
}

template <class T>
void TOverlaidAttributeDictionary<T>::SetYson(
    const IAttributeDictionary& /*dict*/,
    const TString& /*key*/,
    const NYson::TYsonString& /*value*/)
{
    // NB: IAttributeDictionary's extension methods require SetYson() to be
    // compilable.
    YT_ABORT();
}

template <class T>
bool TOverlaidAttributeDictionary<T>::Remove(
    IAttributeDictionary& dict,
    const TString& key)
{
    return dict.Remove(key);
}

template <class T>
bool TOverlaidAttributeDictionary<T>::Remove(
    const IAttributeDictionary& /*dict*/,
    const TString& /*key*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class... Args>
TOverlaidAttributeDictionary<typename std::decay<T>::type>
OverlayAttributeDictionaries(
    T&& topmostUnderlyingDict,
    Args&&... underlyingDicts)
{
    return TOverlaidAttributeDictionary<typename std::decay<T>::type>(
        std::forward<T>(topmostUnderlyingDict),
        std::forward<Args>(underlyingDicts)...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
