#pragma once

#include "public.h"

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

// All vectors here are assumed to be sorted.

TPartIndexList MakeSegment(int begin, int end);

TPartIndexList MakeSingleton(int elem);

TPartIndexList Difference(int begin, int end, const TPartIndexList& subtrahend);

TPartIndexList Difference(const TPartIndexList& first, const TPartIndexList& second);

TPartIndexList Difference(const TPartIndexList& first, int elem);

TPartIndexList Intersection(const TPartIndexList& first, const TPartIndexList& second);

TPartIndexList Union(const TPartIndexList& first, const TPartIndexList& second);

bool Contains(const TPartIndexList& set, int elem);

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT

