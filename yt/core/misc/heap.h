#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Constructs a min-heap on |[begin, end)|.
template <class Iterator, class Comparer>
void MakeHeap(Iterator begin, Iterator end, const Comparer& comparer);

//! Readjusts the min-heap on |[begin, end)| by pushing its front item down if needed.
template <class Iterator, class Comparer>
void AdjustHeap(Iterator begin, Iterator end, const Comparer& comparer);

//! Extracts the front from the heap on |[begin, end)| by moving
//! its back to the front and then pushing it down if needed.
template <class Iterator, class Comparer>
void ExtractHeap(Iterator begin, Iterator end, const Comparer& comparer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define HEAP_INL_H_
#include "heap-inl.h"
#undef HEAP_INL_H_
