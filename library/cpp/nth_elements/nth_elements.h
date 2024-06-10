#pragma once

#include <util/generic/algorithm.h>


namespace NPrivate {

    template <typename TCompare, typename TForwardIterator>
    void Sort3(TForwardIterator x, TForwardIterator y, TForwardIterator z,
                   TCompare compare) {
        if (!compare(*y, *x)) { // if x <= y
            if (!compare(*z, *y)) { // if y <= z
                return; // x <= y && y <= z
            }
            // x <= y && y > z
            DoSwap(*y, *z);
            // x <= z && y < z
            if (compare(*y, *x)) { // if x > y
                DoSwap(*x, *y);
                return; // x < y && y <= z
            }
            return; // x <= y && y < z
        }
        // x > y
        if (compare(*z, *y)) { // if y > z
            DoSwap(*x, *z);
            return; // x < y && y < z
        }
        // x > y && z >= y
        DoSwap(*x, *y);
        // x < y && x <= z
        if (compare(*z, *y)) { // if y > z
            DoSwap(*y, *z);
            return; // x <= y && y < z
        }
        return; // x < y && y <= z
    }

    template <typename RandomIterator, typename NthsRandomIterator, typename Compare>
    static inline void NthElementsImpl(RandomIterator begin, RandomIterator end,
                                       NthsRandomIterator nthsBegin, NthsRandomIterator nthsEnd,
                                       Compare compare) {
        if (end - begin <= 1 || nthsBegin == nthsEnd) {
            return;
        }

        if (end - begin == 2) {
            auto second = begin + 1;
            if (compare(*second, *begin)) {
                DoSwap(*begin, *second);
            }
            return;
        }

        Sort3(begin + (end - begin) / 2, begin, end - 1, compare);

        auto i = begin + 1;
        auto j = end - 1;

        do {
            while (i < end && compare(*i, *begin)) {
                ++i;
            }
            while (j > begin && compare(*begin, *j)) {
                --j;
            }
            if (i <= j) {
                DoSwap(*i, *j);
                ++i;
                --j;
            }

        } while (i <= j);

        auto nthsBound = nthsBegin;

        while (nthsBound != nthsEnd && *nthsBound < j + 1) {
            ++nthsBound;
        }
        NthElementsImpl(begin, j + 1, nthsBegin, nthsBound, compare);

        while (nthsBound != nthsEnd && *nthsBound < i) {
            ++nthsBound;
        }
        NthElementsImpl(i, end, nthsBound, nthsEnd, compare);
    }
}


//! NthElements is a partial sorting algorithm that
//! rearranges elements in [begin, end) such that:
//! The elements pointed at by [nthsBegin, nthsEnd)
//! are changed to whatever elements would occur in that position
//! if [begin, end) were sorted.
//! @param begin, end    random access iterators defining the range sort
//! @param nthsBegin, nthsEnd    random access iterators defining
//! the set of iterators defining sort partition points.
//! Result of dereferencing NthsRandomIterator is RandomIterator
//! @param compare    comparator for partial sort
template <typename RandomIterator, typename NthsRandomIterator, typename Compare = TLess<>>
static inline void NthElements(RandomIterator begin, RandomIterator end,
                               NthsRandomIterator nthsBegin, NthsRandomIterator nthsEnd,
                               Compare compare = {}) {
    if (IsSorted(nthsBegin, nthsEnd)) {
        ::NPrivate::NthElementsImpl(begin, end, nthsBegin, nthsEnd, compare);
    } else {
        TVector<RandomIterator> nths(nthsBegin, nthsEnd);
        Sort(nths);
        ::NPrivate::NthElementsImpl(begin, end, nths.begin(), nths.end(), compare);
    }
}
