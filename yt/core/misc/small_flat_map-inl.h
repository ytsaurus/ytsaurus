#ifndef SMALL_FLAT_MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include small_flat_map.h"
// For the sake of sane code completion.
#include "small_flat_map.h"
#endif

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

template <class K, class V, unsigned N>
typename TSmallFlatMap<K, V, N>::iterator TSmallFlatMap<K, V, N>::begin()
{
    return Storage_.begin();
}

template <class K, class V, unsigned N>
typename TSmallFlatMap<K, V, N>::const_iterator TSmallFlatMap<K, V, N>::begin() const
{
    return Storage_.begin();
}

template <class K, class V, unsigned N>
typename TSmallFlatMap<K, V, N>::iterator TSmallFlatMap<K, V, N>::end()
{
    return Storage_.end();
}

template <class K, class V, unsigned N>
typename TSmallFlatMap<K, V, N>::const_iterator TSmallFlatMap<K, V, N>::end() const
{
    return Storage_.end();
}

template <class K, class V, unsigned N>
void TSmallFlatMap<K, V, N>::reserve(size_type n)
{
    Storage_.reserve(n);
}

template <class K, class V, unsigned N>
typename TSmallFlatMap<K, V, N>::iterator TSmallFlatMap<K, V, N>::find(const K& k)
{
    auto [rangeBegin, rangeEnd] = EqualRange(k);
    return rangeBegin == rangeEnd ? end() : rangeBegin;
}

template <class K, class V, unsigned N>
typename TSmallFlatMap<K, V, N>::const_iterator TSmallFlatMap<K, V, N>::find(const K& k) const
{
    auto [rangeBegin, rangeEnd] = EqualRange(k);
    return rangeBegin == rangeEnd ? end() : rangeBegin;
}

template <class K, class V, unsigned N>
std::pair<typename TSmallFlatMap<K, V, N>::iterator, bool> TSmallFlatMap<K, V, N>::insert(const value_type& value)
{
    auto [rangeBegin, rangeEnd] = EqualRange(value.first);
    if (rangeBegin != rangeEnd) {
        return {rangeBegin, false};
    } else {
        Storage_.insert(rangeBegin, value);
        return {rangeBegin, true};
    }
}

template <class K, class V, unsigned N>
V& TSmallFlatMap<K, V, N>::operator[](const K& k)
{
    auto [it, inserted] = insert({k, V()});
    return it->second;
}

template <class K, class V, unsigned N>
typename TSmallFlatMap<K, V, N>::iterator TSmallFlatMap<K, V, N>::erase(const K& k)
{
    auto [rangeBegin, rangeEnd] = EqualRange(k);
    return erase(rangeBegin, rangeEnd);
}

template <class K, class V, unsigned N>
typename TSmallFlatMap<K, V, N>::iterator TSmallFlatMap<K, V, N>::erase(iterator b, iterator e)
{
    auto result = Storage_.erase(b, e);
    // Try to keep the storage inline.
    Storage_.shrink_to_small();
    return result;
}

template <class K, class V, unsigned N>
std::pair<typename TSmallFlatMap<K, V, N>::iterator, typename TSmallFlatMap<K, V, N>::iterator>
TSmallFlatMap<K, V, N>::EqualRange(const K& k)
{
    auto result = std::equal_range(Storage_.begin(), Storage_.end(), k, TKeyCompare());
    YT_ASSERT(std::distance(result.first, result.second) <= 1);
    return result;
}

template <class K, class V, unsigned N>
std::pair<typename TSmallFlatMap<K, V, N>::const_iterator, typename TSmallFlatMap<K, V, N>::const_iterator>
TSmallFlatMap<K, V, N>::EqualRange(const K& k) const
{
    auto result = std::equal_range(Storage_.begin(), Storage_.end(), k, TKeyCompare());
    YT_ASSERT(std::distance(result.first, result.second) <= 1);
    return result;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
