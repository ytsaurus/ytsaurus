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

} // namespace NYT

#define RANDOM_INL_H_
#include "random-inl.h"
#undef RANDOM_INL_H_
