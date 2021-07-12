#pragma once
#ifndef COLLECTION_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include collection_helpers.h"
// For the sake of sane code completion.
#include "collection_helpers.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <bool IsSet>
struct TKeyLess;

template <>
struct TKeyLess<true>
{
    template<typename T>
    bool operator()(const T& lhs, const T& rhs) const
    {
        return lhs < rhs;
    }
};

template <>
struct TKeyLess<false>
{
    template<typename T>
    bool operator()(const T& lhs, const T& rhs) const
    {
        return lhs.first < rhs.first;
    }
};

template <class TItem, class T, class TGetter>
std::vector<TItem> GetIthsImpl(const T& collection, size_t sizeLimit, const TGetter& getter)
{
    std::vector<TItem> result;
    result.reserve(std::min(collection.size(), sizeLimit));
    for (const auto& item : collection) {
        if (result.size() >= sizeLimit)
            break;
        result.push_back(getter(item));
    }
    return result;
}

} // namespace

template <class T, class C>
std::vector<typename T::const_iterator> GetSortedIterators(const T& set, C comp)
{
    using TIterator = typename T::const_iterator;
    std::vector<TIterator> iterators;
    iterators.reserve(set.size());
    for (auto it = set.cbegin(); it != set.cend(); ++it) {
        iterators.emplace_back(it);
    }

    std::sort(
        iterators.begin(),
        iterators.end(),
        [comp=std::move(comp)] (TIterator a, TIterator b) {
            return comp(*a, *b);
        });

    return iterators;
}

template <class T>
std::vector<typename T::const_iterator> GetSortedIterators(const T& collection)
{
    using TIsSet = std::is_same<typename T::key_type, typename T::value_type>;
    return GetSortedIterators(collection, TKeyLess<TIsSet::value>());
}

template <class T>
std::vector<typename T::key_type> GetKeys(const T& collection, size_t sizeLimit)
{
    return GetIthsImpl<typename T::key_type>(
        collection,
        sizeLimit,
        [] (const auto& item) {
            return std::get<0u>(item);
        });
}

template <class T>
std::vector<typename T::mapped_type> GetValues(const T& collection, size_t sizeLimit)
{
    return GetIthsImpl<typename T::mapped_type>(
        collection,
        sizeLimit,
        [] (const auto& item) {
            return std::get<1u>(item);
        });
}

template <class T>
std::vector<typename T::value_type> GetItems(const T& collection, size_t sizeLimit)
{
    return GetIthsImpl<typename T::value_type>(
        collection,
        sizeLimit,
        [] (const auto& item) {
            return item;
        });
}

template <size_t I, class T>
std::vector<typename std::tuple_element<I, typename T::value_type>::type> GetIths(const T& collection, size_t sizeLimit)
{
    return GetIthsImpl<typename std::tuple_element<I, typename T::value_type>::type>(
        collection,
        sizeLimit,
        [] (const auto& item) {
            return std::get<I>(item);
        });
}

template <class T>
bool ShrinkHashTable(T* collection)
{
    if (collection->bucket_count() <= 4 * collection->size() || collection->bucket_count() <= 16) {
        return false;
    }

    typename std::remove_reference<decltype(*collection)>::type collectionCopy(collection->begin(), collection->end());
    collectionCopy.swap(*collection);
    return true;
}

template <class TSource, class TTarget>
void MergeFrom(TTarget* target, const TSource& source)
{
    for (const auto& item : source) {
        target->insert(item);
    }
}

template <class TMap, class TKey>
const auto& GetOrCrash(const TMap& map, const TKey& key)
{
    auto it = map.find(key);
    YT_VERIFY(it != map.end());
    return it->second;
}

template <class TMap, class TKey>
auto& GetOrCrash(TMap& map, const TKey& key)
{
    auto it = map.find(key);
    YT_VERIFY(it != map.end());
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

template <size_t Index, class... Ts>
struct TMakeArrayTraits;

template <size_t Index>
struct TMakeArrayTraits<Index>
{
    template <class V>
    static void Do(V*)
    { }
};

template <size_t Index, class T, class... Ts>
struct TMakeArrayTraits<Index, T, Ts...>
{
    template <class V>
    static void Do(V* array, const T& head, const Ts&... tail)
    {
        (*array)[Index] = head;
        TMakeArrayTraits<Index + 1, Ts...>::Do(array, tail...);
    }
};

template <class... Ts>
auto MakeArray(
    const Ts&... values)
-> std::array<std::tuple_element_t<0, std::tuple<Ts...>>, sizeof...(Ts)>
{
    std::array<std::tuple_element_t<0, std::tuple<Ts...>>, sizeof...(Ts)> array;
    TMakeArrayTraits<0, Ts...>::Do(&array, values...);
    return array;
}

template <class T>
std::array<T, 0> MakeArray()
{
    return std::array<T, 0>();
}

////////////////////////////////////////////////////////////////////////////////

// See https://stackoverflow.com/questions/23439221/variadic-template-function-to-concatenate-stdvector-containers.
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

// Nice syntax to allow in-order expansion of parameter packs.
struct TDoInOrder
{
    template <class T>
    TDoInOrder(std::initializer_list<T>&&) { }
};

// const& version.
template <class TVector>
void AppendVector(TVector& destination, const TVector& source)
{
    destination.insert(destination.end(), source.begin(), source.end());
}

// && version.
template <class TVector>
void AppendVector(TVector& destination, TVector&& source)
{
    destination.insert(
        destination.end(),
        std::make_move_iterator(source.begin()),
        std::make_move_iterator(source.end()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <class T, class... TArgs>
std::vector<T> ConcatVectors(std::vector<T> first, TArgs&&... rest)
{
    // We need to put results somewhere; that's why we accept first parameter by value and use it as a resulting vector.
    // First, calculate total size of the result.
    std::size_t totalSize = first.size();
    NDetail::TDoInOrder { totalSize += rest.size() ... };
    first.reserve(totalSize);
    // Then, append all remaining arguments to first. Note that depending on rvalue-ness of the argument,
    // suitable overload of AppendVector will be used.
    NDetail::TDoInOrder { (NDetail::AppendVector(first, std::forward<TArgs>(rest)), 0)... };
    // Not quite sure why, but in the original article result is explicitly moved.
    return std::move(first);
}

template <class T>
void SortByFirst(T begin, T end)
{
    std::sort(begin, end, [] (const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
}

template <class T>
void SortByFirst(T& collection)
{
    SortByFirst(collection.begin(), collection.end());
}

template <class T>
std::vector<std::pair<typename T::key_type, typename T::mapped_type>> SortHashMapByKeys(const T& hashMap)
{
    std::vector<std::pair<typename T::key_type, typename T::mapped_type>> vector;
    vector.reserve(hashMap.size());
    for (const auto& [key, value] : hashMap) {
        vector.emplace_back(key, value);
    }
    SortByFirst(vector);
    return vector;
}

template <class T>
Y_FORCE_INLINE void EnsureVectorSize(std::vector<T>& vector, ssize_t size, const T& defaultValue)
{
    if (static_cast<ssize_t>(vector.size()) < size) {
        vector.resize(size, defaultValue);
    }
}

template <class T>
Y_FORCE_INLINE void EnsureVectorIndex(std::vector<T>& vector, ssize_t index, const T& defaultValue)
{
    EnsureVectorSize(vector, index + 1, defaultValue);
}

template <class T>
Y_FORCE_INLINE void AssignVectorAt(std::vector<T>& vector, ssize_t index, const T& value, const T& defaultValue)
{
    EnsureVectorIndex(vector, index, defaultValue);
    vector[index] = value;
}

template <class T>
Y_FORCE_INLINE void AssignVectorAt(std::vector<T>& vector, ssize_t index, T&& value, const T& defaultValue)
{
    EnsureVectorIndex(vector, index, defaultValue);
    vector[index] = std::move(value);
}

template <class T>
Y_FORCE_INLINE const T& VectorAtOr(const std::vector<T>& vector, ssize_t index, const T& defaultValue)
{
    return index < static_cast<ssize_t>(vector.size()) ? vector[index] : defaultValue;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
