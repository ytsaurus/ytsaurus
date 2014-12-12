#ifndef ENUM_INL_H_
#error "Direct inclusion of this file is not allowed, include enum.h"
#endif
#undef ENUM_INL_H_

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

constexpr int Max(int x, int y) {
    return x > y ? x : y;
}

template <typename... Ts>
constexpr int GenericMax(int x, Ts... args)
{
    return Max(x, GenericMax(args...));
}

template <>
constexpr int GenericMax(int x)
{
    return x;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
