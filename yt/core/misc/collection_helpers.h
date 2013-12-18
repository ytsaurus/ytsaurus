#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::vector<typename T::const_iterator> GetSortedIterators(const T& collection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define COLLECTION_HELPERS_INL_H_
#include "collection_helpers-inl.h"
#undef COLLECTION_HELPERS_INL_H_

