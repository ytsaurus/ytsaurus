#pragma once

#include "range.h"
#include "compact_vector.h"
#include "small_vector.h"

template <class T, unsigned N>
struct hash<NYT::SmallVector<T, N>>
{
    size_t operator()(const NYT::SmallVector<T, N>& container) const
    {
        return hash<NYT::TRange<T>>()(NYT::MakeRange(container));
    }
};

template <class T, size_t N>
struct hash<NYT::TCompactVector<T, N>>
{
    size_t operator()(const NYT::TCompactVector<T, N>& container) const
    {
        return hash<NYT::TRange<T>>()(NYT::MakeRange(container));
    }
};
