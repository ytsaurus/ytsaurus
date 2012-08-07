#ifndef HEAP_INL_H_
#error "Direct inclusion of this file is not allowed, include heap.h"
#endif
#undef HEAP_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class Iterator, class Comparer>
void SiftDown(Iterator begin, Iterator end, Iterator current, const Comparer& comparer)
{
    auto value = *current;
    while (true) {
        size_t dist = std::distance(begin, current);
        auto left = begin + 2 * dist + 1;
        auto right = left + 1;
        if (left >= end) {
            break;
        }
        
        Iterator min;
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

template <class Iterator, class Comparer>
void MakeHeap(Iterator begin, Iterator end, const Comparer& comparer)
{
    size_t size = std::distance(begin, end);
    for (auto current = begin + size / 2 - 1; current >= begin; --current) {
        SiftDown(begin, end, current, comparer);
    }
}

template <class Iterator, class Comparer>
void AdjustHeap(Iterator begin, Iterator end, const Comparer& comparer)
{
    SiftDown(begin, end, begin, comparer);
}

template <class Iterator, class Comparer>
void ExtractHeap(Iterator begin, Iterator end, const Comparer& comparer)
{
    auto newEnd = end - 1;
    *begin = *newEnd;
    SiftDown(begin, newEnd, begin, comparer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
