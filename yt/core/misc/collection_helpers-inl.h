#ifndef COLLECTION_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include collection_helpers.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

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

template <class T>
std::vector<typename T::const_iterator> GetSortedIterators(const T& collection)
{
    using TIsSet = std::is_same<typename T::key_type, typename T::value_type>;
    return GetSortedIterators<TKeyLess<TIsSet::value>>(collection);
}

template <class T>
std::vector<typename T::key_type> GetKeys(const T& collection, size_t sizeLimit)
{
    std::vector<typename T::key_type> result;
    result.reserve(std::min(collection.size(), sizeLimit));
    for (const auto& pair : collection) {
        if (result.size() >= sizeLimit)
            break;
        result.push_back(pair.first);
    }
    return result;
}

template <class T>
std::vector<typename T::mapped_type> GetValues(const T& collection, size_t sizeLimit)
{
    std::vector<typename T::mapped_type> result;
    result.reserve(std::min(collection.size(), sizeLimit));
    for (const auto& pair : collection) {
        if (result.size() >= sizeLimit)
            break;
        result.push_back(pair.second);
    }
    return result;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
