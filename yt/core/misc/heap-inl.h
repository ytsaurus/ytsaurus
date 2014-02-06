#ifndef HEAP_INL_H_
#error "Direct inclusion of this file is not allowed, include heap.h"
#endif
#undef HEAP_INL_H_

#include <iterator>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TIterator, class TComparer>
void SiftDown(TIterator begin, TIterator end, TIterator current, const TComparer& comparer)
{
    auto value = *current;
    while (true) {
        size_t dist = std::distance(begin, current);
        auto left = begin + 2 * dist + 1;
        auto right = left + 1;
        if (left >= end) {
            break;
        }

        TIterator min;
        if (right >= end) {
            min = left;
        } else {
            min = comparer(*left, *right) ? left : right;
        }

        auto minValue = *min;
        if (comparer(value, minValue)) {
            break;
        }

        *current = minValue;
        current = min;
    }
    *current = value;
}

template <class TIterator, class TComparer>
void SiftUp(TIterator begin, TIterator end, TIterator current, const TComparer& comparer)
{
    auto value = *current;
    while (current != begin) {
        size_t dist = std::distance(begin, current);
        auto parent = begin + (dist - 1) / 2;
        auto parentValue = *parent;
        if (comparer(parentValue, value))
            break;

        *current = parentValue;
        current = parent;
    }
    *current = value;
}

} // namespace NDetail

template <class TIterator, class TComparer>
void MakeHeap(TIterator begin, TIterator end, const TComparer& comparer)
{
    size_t size = std::distance(begin, end);
    for (auto current = begin + size / 2 - 1; current >= begin; --current) {
        NYT::NDetail::SiftDown(begin, end, current, comparer);
    }
}

template <class TIterator, class TComparer>
void AdjustHeapFront(TIterator begin, TIterator end, const TComparer& comparer)
{
    NYT::NDetail::SiftDown(begin, end, begin, comparer);
}

template <class TIterator, class TComparer>
void AdjustHeapBack(TIterator begin, TIterator end, const TComparer& comparer)
{
    NYT::NDetail::SiftUp(begin, end, end - 1, comparer);
}

template <class TIterator, class TComparer>
void ExtractHeap(TIterator begin, TIterator end, const TComparer& comparer)
{
    auto newEnd = end - 1;
    std::swap(*begin, *newEnd);
    NYT::NDetail::SiftDown(begin, newEnd, begin, comparer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
