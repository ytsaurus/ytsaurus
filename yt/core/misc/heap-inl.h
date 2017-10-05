#pragma once
#ifndef HEAP_INL_H_
#error "Direct inclusion of this file is not allowed, include heap.h"
#endif

#include <iterator>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TIterator, class TComparer>
void SiftDown(TIterator begin, TIterator end, TIterator current, TComparer comparer)
{
    size_t size = std::distance(begin, end);
    size_t offset = std::distance(begin, current);

    auto value = begin[offset];
    while (true) {
        size_t left = 2 * offset + 1;

        if (left >= size) {
            break;
        }

        size_t right = left + 1;
        size_t min;

        if (right >= size) {
            min = left;
        } else {
            min = comparer(begin[left], begin[right]) ? left : right;
        }

        auto minValue = begin[min];
        if (comparer(value, minValue)) {
            break;
        }

        begin[offset] = minValue;
        offset = min;
    }
    begin[offset] = value;
}

template <class TIterator, class TComparer>
void SiftUp(TIterator begin, TIterator end, TIterator current, TComparer comparer)
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
void MakeHeap(TIterator begin, TIterator end, TComparer comparer)
{
    size_t size = std::distance(begin, end);
    if (size > 1) {
        for (size_t current = size / 2; current > 0; ) {
            --current;
            NYT::NDetail::SiftDown(begin, end, begin + current, comparer);
        }
    }
}

template <class TIterator>
void MakeHeap(TIterator begin, TIterator end)
{
    MakeHeap(std::move(begin), std::move(end), std::less<>());
}

template <class TIterator, class TComparer>
void AdjustHeapFront(TIterator begin, TIterator end, TComparer comparer)
{
    if (end - begin > 1) {
        NYT::NDetail::SiftDown(begin, end, begin, comparer);
    }
}

template <class TIterator>
void AdjustHeapFront(TIterator begin, TIterator end)
{
    AdjustHeapFront(std::move(begin), std::move(end), std::less<>());
}

template <class TIterator, class TComparer>
void AdjustHeapBack(TIterator begin, TIterator end, TComparer comparer)
{
    if (end - begin > 1) {
        NYT::NDetail::SiftUp(begin, end, end - 1, comparer);
    }
}

template <class TIterator>
void AdjustHeapBack(TIterator begin, TIterator end)
{
    AdjustHeapBack(std::move(begin), std::move(end), std::less<>());
}

template <class TIterator, class TComparer>
void ExtractHeap(TIterator begin, TIterator end, TComparer comparer)
{
    Y_ASSERT(begin != end);
    auto newEnd = end - 1;
    std::swap(*begin, *newEnd);
    NYT::NDetail::SiftDown(begin, newEnd, begin, comparer);
}

template <class TIterator>
void ExtractHeap(TIterator begin, TIterator end)
{
    ExtractHeap(std::move(begin), std::move(end), std::less<>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
