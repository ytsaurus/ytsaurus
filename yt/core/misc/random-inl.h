#ifndef RANDOM_INL_H_
#error "Direct inclusion of this file is not allowed, include random.h"
#endif
#undef RANDOM_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline TRandomGenerator::TRandomGenerator()
    : Current_(0)
{ }

inline TRandomGenerator::TRandomGenerator(ui64 seed)
    : Current_(seed)
{ }

template <>
inline double TRandomGenerator::Generate()
{
    return GenerateDouble();
}

template <>
inline float TRandomGenerator::Generate()
{
    return GenerateDouble();
}

template <>
inline long double TRandomGenerator::Generate()
{
    return GenerateDouble();
}

template <class T>
T TRandomGenerator::Generate()
{
    return static_cast<T>(GenerateInteger());
}

////////////////////////////////////////////////////////////////////////////////

template <class TForwardIterator, class TOutputIterator>
TOutputIterator RandomSampleN(
    TForwardIterator begin,
    TForwardIterator end,
    TOutputIterator output,
    size_t n)
{
    size_t remaining = std::distance(begin, end);
    size_t m = Min(n, remaining);

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
