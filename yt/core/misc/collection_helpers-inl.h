#ifndef COLLECTION_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include collection_helpers.h"
#endif
#undef COLLECTION_HELPERS_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::vector<typename T::const_iterator> GetSortedSetIterators(const T& set)
{
    typedef typename T::const_iterator TIterator;
    std::vector<TIterator> iterators;
    iterators.reserve(set.size());
    for (auto it = set.begin(); it != set.end(); ++it) {
        iterators.push_back(it);
    }
    std::sort(
        iterators.begin(),
        iterators.end(),
        [] (TIterator lhs, TIterator rhs) {
            return *lhs < *rhs;
        });
    return iterators;
}

template <class T>
std::vector<typename T::const_iterator> GetSortedMapIterators(const T& map)
{
    typedef typename T::const_iterator TIterator;
    std::vector<TIterator> iterators;
    iterators.reserve(map.size());
    for (auto it = map.begin(); it != map.end(); ++it) {
        iterators.push_back(it);
    }
    std::sort(
        iterators.begin(),
        iterators.end(),
        [] (TIterator lhs, TIterator rhs) {
            return lhs->first < rhs->first;
        });
    return iterators;
}

template <class TKey>
std::vector<typename yhash_set<TKey>::const_iterator> GetSortedIterators(
    const yhash_set<TKey>& set)
{
    return GetSortedSetIterators(set);
}

template <class TKey, class TValue>
std::vector<typename yhash_map<TKey, TValue>::const_iterator> GetSortedIterators(
    const yhash_map<TKey, TValue>& map)
{
    return GetSortedMapIterators(map);
}

template <class TKey, class TValue>
std::vector<typename yhash_multimap<TKey, TValue>::const_iterator> GetSortedIterators(
    const yhash_multimap<TKey, TValue>& map)
{
    return GetSortedMapIterators(map);
}

template <class TKey, class TValue>
std::vector <typename std::map<TKey, TValue>::const_iterator> GetSortedIterators(
    const std::map<TKey, TValue>& map)
{
    return GetSortedMapIterators(map);
}

template <class TKey, class TValue>
std::vector<typename std::multimap<TKey, TValue>::const_iterator> GetSortedIterators(
    const std::multimap<TKey, TValue>& map)
{
    return GetSortedMapIterators(map);
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
std::vector<typename T::value_type> GetValues(const T& collection, size_t sizeLimit)
{
    std::vector<typename T::value_type> result;
    result.reserve(std::min(collection.size(), sizeLimit));
    for (const auto& pair : collection) {
        if (result.size() >= sizeLimit)
            break;
        result.push_back(pair.second);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
