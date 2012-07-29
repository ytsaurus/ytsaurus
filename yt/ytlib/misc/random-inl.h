#ifndef RANDOM_INL_H_
#error "Direct inclusion of this file is not allowed, include random.h"
#endif
#undef RANDOM_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <>
inline double TRandomGenerator::GetNext()
{
    return Generator.GenRandReal2();
}

template <>
inline float TRandomGenerator::GetNext()
{
    return Generator.GenRandReal2();
}

template <>
inline long double TRandomGenerator::GetNext()
{
    return Generator.GenRandReal2();
}

template <class T>
T TRandomGenerator::GetNext()
{
    return static_cast<T>(Generator.GenRand());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
