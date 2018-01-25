#pragma once
#ifndef COLLECTION_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include collection_helpers.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class C, class T>
std::vector<typename T::const_iterator> GetSortedIterators(const T& set)
{
    std::vector<typename T::const_iterator> iterators;
    iterators.reserve(set.size());
    for (auto it = set.cbegin(); it != set.cend(); ++it) {
        iterators.emplace_back(it);
    }

    std::sort(iterators.begin(), iterators.end(), C());
    return iterators;
}

template <bool IsSet>
struct TKeyLess;

template <>
struct TKeyLess<true>
{
    template<typename T>
    bool operator()(const T& lhs, const T& rhs) const
    {
        return *lhs < *rhs;
    }
};

template <>
struct TKeyLess<false>
{
    template<typename T>
    bool operator()(const T& lhs, const T& rhs) const
    {
        return lhs->first < rhs->first;
    }
};

template <size_t I, class TItem, class T>
std::vector<TItem> GetIthsImpl(const T& collection, size_t sizeLimit)
{
    std::vector<TItem> result;
    result.reserve(std::min(collection.size(), sizeLimit));
    for (const auto& item : collection) {
        if (result.size() >= sizeLimit)
            break;
        result.emplace_back(std::get<I>(item));
    }
    return result;
}

} // namespace

template <class T>
std::vector<typename T::const_iterator> GetSortedIterators(const T& collection)
{
    using TIsSet = std::is_same<typename T::key_type, typename T::value_type>;
    return GetSortedIterators<TKeyLess<TIsSet::value>>(collection);
}

template <class T>
std::vector<typename T::key_type> GetKeys(const T& collection, size_t sizeLimit)
{
    return GetIthsImpl<0U, typename T::key_type>(collection, sizeLimit);
}

template <class T>
std::vector<typename T::mapped_type> GetValues(const T& collection, size_t sizeLimit)
{
    return GetIthsImpl<1U, typename T::mapped_type>(collection, sizeLimit);
}

template <size_t I, class T>
std::vector<typename std::tuple_element<I, typename T::value_type>::type> GetIths(const T& collection, size_t sizeLimit)
{
    return GetIthsImpl<I, typename std::tuple_element<I, typename T::value_type>::type>(collection, sizeLimit);
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

} // namespace NYT
