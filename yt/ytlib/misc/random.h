#pragma once

#include <util/random/mersenne.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Note: this generator deterministic (doesn't use global seed)
class TRandomGenerator
{
public:
    TRandomGenerator(ui64 seed)
        : Generator(seed)
    { }

    template <class T>
    T GetNext();

private:
    TMersenne<ui64> Generator;
};

////////////////////////////////////////////////////////////////////////////////

template <>
double TRandomGenerator::GetNext()
{
    return Generator.GenRandReal2();
}

template <>
float TRandomGenerator::GetNext()
{
    return Generator.GenRandReal2();
}

template <>
long double TRandomGenerator::GetNext()
{
    return Generator.GenRandReal2();
}

template<class T>
T TRandomGenerator::GetNext()
{
    return static_cast<T>(Generator.GenRand());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
