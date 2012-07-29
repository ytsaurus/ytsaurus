#pragma once

#include <util/random/mersenne.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A fully deterministic pseudo-random number generator.
class TRandomGenerator
{
public:
    explicit TRandomGenerator(ui64 seed)
        : Generator(seed)
    { }

    template <class T>
    T GetNext();

private:
    TMersenne<ui64> Generator;
};

////////////////////////////////////////////////////////////////////////////////

template <class TForwardIterator, class TOutputIterator, class TDistance>
TOutputIterator RandomSampleN(
    TForwardIterator begin, TForwardIterator end,
    TOutputIterator output, const TDistance n)
{
    TDistance remaining = std::distance(begin, end);
    TDistance m = Min(n, remaining);

    while (m > 0) {
        if ((std::rand() % remaining) < m) {
            *output = *begin;
            ++output;
            --m;
        }

        --remaining;
        ++begin;
    }

    return output;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define RANDOM_INL_H_
#include "random-inl.h"
#undef RANDOM_INL_H_
