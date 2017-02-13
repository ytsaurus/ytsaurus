#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A fully deterministic pseudo-random number generator.
class TRandomGenerator
{
public:
    TRandomGenerator();
    explicit TRandomGenerator(ui64 seed);

    template <class T>
    T Generate();

private:
    ui64 Current_;

    ui64 GenerateInteger();
    double GenerateDouble();

};

////////////////////////////////////////////////////////////////////////////////

template <class TForwardIterator, class TOutputIterator>
TOutputIterator RandomSampleN(
    TForwardIterator begin,
    TForwardIterator end,
    TOutputIterator output,
    size_t n);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define RANDOM_INL_H_
#include "random-inl.h"
#undef RANDOM_INL_H_
