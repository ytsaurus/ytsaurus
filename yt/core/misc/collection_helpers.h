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
std::vector<typename T::value_type> GetValues(
    const T& collection,
    size_t sizeLimit = std::numeric_limits<size_t>::max());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define COLLECTION_HELPERS_INL_H_
#include "collection_helpers-inl.h"
#undef COLLECTION_HELPERS_INL_H_

