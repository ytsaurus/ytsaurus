#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Constructs a min-heap on |[begin, end)|.
template <class TIterator, class TComparer>
void MakeHeap(TIterator begin, TIterator end, TComparer comparer);
template <class TIterator>
void MakeHeap(TIterator begin, TIterator end);

//! Readjusts the min-heap on |[begin, end)| by pushing its front item down if needed.
template <class TIterator, class TComparer>
void AdjustHeapFront(TIterator begin, TIterator end, TComparer comparer);
template <class TIterator>
void AdjustHeapFront(TIterator begin, TIterator end);

//! Readjusts the min-heap on |[begin, end)| by pushing its back item up if needed.
template <class TIterator, class TComparer>
void AdjustHeapBack(TIterator begin, TIterator end, TComparer comparer);

//! Extracts the front from the heap on |[begin, end)| by moving
//! its back to the front and then pushing it down if needed.
template <class TIterator, class TComparer>
void ExtractHeap(TIterator begin, TIterator end, TComparer comparer);
template <class TIterator>
void ExtractHeap(TIterator begin, TIterator end);

//! Readjusts the min-heap on |[begin, end)| by pushing current item down if needed.
template <class TIterator, class TComparer>
void SiftDown(TIterator begin, TIterator end, TIterator current, TComparer comparer);

//! Readjusts the min-heap on |[begin, end)| by pushing current item up if needed.
template <class TIterator, class TComparer>
void SiftUp(TIterator begin, TIterator end, TIterator current, TComparer comparer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define HEAP_INL_H_
#include "heap-inl.h"
#undef HEAP_INL_H_
