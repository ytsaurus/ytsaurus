#pragma once

#include <util/system/defaults.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Functions that provide coding of integers with property: 0 <= f(x) <= 2 * |x|
//! Actually taken 'as is' from protobuf/wire_format_lite.h

inline ui32 ZigZagEncode32(i32 n)
{
    // Note:  the right-shift must be arithmetic.
    // Note:  left shift must be unsigned because of overflow.
    return (static_cast<ui32>(n) << 1) ^ static_cast<ui32>(n >> 31);
}

inline i32 ZigZagDecode32(ui32 n)
{
    // Note:  using unsigned types prevent undefined behavior.
    return static_cast<i32>((n >> 1) ^ (~(n & 1) + 1));
}

inline ui64 ZigZagEncode64(i64 n)
{
    // Note:  the right-shift must be arithmetic.
    // Note:  left shift must be unsigned because of overflow.
    return (static_cast<ui64>(n) << 1) ^ static_cast<ui64>(n >> 63);
}

inline i64 ZigZagDecode64(ui64 n)
{
    // Note:  using unsigned types prevent undefined behavior.
    return static_cast<i64>((n >> 1) ^ (~(n & 1) + 1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
