#pragma once

#include "range.h"

#include <library/cpp/yt/small_containers/compact_vector.h>

template <class T, size_t N>
struct hash<NYT::TCompactVector<T, N>>
{
    size_t operator()(const NYT::TCompactVector<T, N>& container) const
    {
        return hash<NYT::TRange<T>>()(NYT::MakeRange(container));
    }
};
