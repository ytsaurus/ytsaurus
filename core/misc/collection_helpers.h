#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::vector<typename T::const_iterator> GetSortedIterators(const T& collection);

template <class T>
std::vector<typename T::key_type> GetKeys(
    const T& collection,
    size_t sizeLimit = std::numeric_limits<size_t>::max());

template <class T>
std::vector<typename T::mapped_type> GetValues(
    const T& collection,
    size_t sizeLimit = std::numeric_limits<size_t>::max());

template <size_t N, class T>
std::vector<typename std::tuple_element<N, typename T::value_type>::type> GetIths(
    const T& collection,
    size_t sizeLimit = std::numeric_limits<size_t>::max());

template <class T>
bool ShrinkHashTable(T* collection);

template <class TSource, class TTarget>
void MergeFrom(TTarget* target, const TSource& source);

template <class... Ts>
auto MakeArray(
    const Ts&... values)
-> std::array<std::tuple_element_t<0, std::tuple<Ts...>>, sizeof...(Ts)>;

template <class T>
std::array<T, 0> MakeArray();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define COLLECTION_HELPERS_INL_H_
#include "collection_helpers-inl.h"
#undef COLLECTION_HELPERS_INL_H_

