#pragma once

#include "range.h"
#include "small_vector.h"

template <class T, unsigned N>
struct hash<NYT::SmallVector<T, N>>
{
    size_t operator()(const NYT::SmallVector<T, N>& container) const
    {
        return hash<NYT::TRange<T>>()(NYT::MakeRange(container));
    }
};
