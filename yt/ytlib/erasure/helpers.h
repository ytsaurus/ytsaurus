#pragma once

#include "public.h"

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

// All vectors here are assumed to be sorted.

TBlockIndexList MakeSegment(int begin, int end);

TBlockIndexList MakeSingleton(int elem);

TBlockIndexList Difference(int begin, int end, const TBlockIndexList& subtrahend);

TBlockIndexList Difference(const TBlockIndexList& first, const TBlockIndexList& second);

TBlockIndexList Difference(const TBlockIndexList& first, int elem);

TBlockIndexList Intersection(const TBlockIndexList& first, const TBlockIndexList& second);

TBlockIndexList Union(const TBlockIndexList& first, const TBlockIndexList& second);

bool Contains(const TBlockIndexList& set, int elem);

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT

