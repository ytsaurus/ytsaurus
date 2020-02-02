#pragma once

#include <yt/core/misc/common.h>

#include <random>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TElement
{
    ui64 Hash;
    ui32 Size;
    char Data[0];
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

template <>
struct THash<NYT::TElement>
{
    inline size_t operator()(const NYT::TElement* value) const
    {
        return value->Hash;
    }
};

template <>
struct TEqualTo<NYT::TElement>
{
    inline bool operator()(const NYT::TElement* lhs, const NYT::TElement* rhs) const
    {
        return lhs->Hash == rhs->Hash &&
            lhs->Size == rhs->Size &&
            memcmp(lhs->Data, rhs->Data, lhs->Size) == 0;
    }
};

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRandomCharGenerator
{
    std::default_random_engine Engine;
    std::uniform_int_distribution<char> Uniform;

    explicit TRandomCharGenerator(size_t seed)
        : Engine(seed)
        , Uniform(0, 'z' - 'a' + '9' - '0' + 1)
    { }

    char operator() ()
    {
        char symbol = Uniform(Engine);
        auto result = symbol >= 10 ? symbol - 10 + 'a' : symbol + '0';
        YT_VERIFY((result <= 'z' &&  result >= 'a') || (result <= '9' &&  result >= '0'));
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
