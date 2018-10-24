#ifndef OVERLAID_ATTRIBUTE_DICTIONARIES_INL_H_
#error "Direct inclusion of this file is not allowed, include overlaid_attribute_dictionaries.h"
// For the sake of sane code completion
#include "overlaid_attribute_dictionaries.h"
#endif

namespace NYT {
namespace NYTree {

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
std::vector<TString> TOverlaidAttributeDictionary<T>::List() const
{
    std::vector<TString> result;
    for (const auto& dict : UnderlyingDictionaries_) {
        if (!dict) {
            continue;
        }
        auto dictKeys = dict->List();
        result.insert(result.end(), dictKeys.begin(), dictKeys.end());
    }

    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());

    return result;
}

template <class T>
NYson::TYsonString TOverlaidAttributeDictionary<T>::FindYson(const TString& key) const
{
    for (const auto& dict : UnderlyingDictionaries_) {
        auto maybeResult = dict ? dict->FindYson(key) : NYson::TYsonString();
        if (maybeResult) {
            return maybeResult;
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
        Y_UNREACHABLE();
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
    const IAttributeDictionary& dict,
    const TString& key,
    const NYson::TYsonString& value)
{
    // NB: IAttributeDictionary's extension methods require SetYson() to be
    // compilable.
    Y_UNREACHABLE();
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
    const IAttributeDictionary& dict,
    const TString& key)
{
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class... Args>
TOverlaidAttributeDictionary<typename std::remove_reference<T>::type>
OverlayAttributeDictionaries(
    T&& topmostUnderlyingDict,
    Args&&... underlyingDicts)
{
    return TOverlaidAttributeDictionary<typename std::remove_reference<T>::type>(
        std::forward<T>(topmostUnderlyingDict),
        std::forward<Args>(underlyingDicts)...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
