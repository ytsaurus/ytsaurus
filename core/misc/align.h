#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Alignment size; measured in bytes and must be a power of two.
constexpr size_t SerializationAlignment = 8;
static_assert(
    (SerializationAlignment & (SerializationAlignment - 1)) == 0,
    "SerializationAlignment should be a power of two.");

namespace NDetail {

constexpr ui8 SerializationPadding[SerializationAlignment] = {};

} // namespace NDetail

//! Returns the minimum number whose addition to #size makes
//! the result divisible by #SerializationAlignment.
inline constexpr size_t GetPaddingSize(size_t size)
{
    return
        (SerializationAlignment - (size & (SerializationAlignment - 1))) &
        (SerializationAlignment - 1);
}

//! Rounds up #size to the nearest factor of #SerializationAlignment.
inline constexpr size_t AlignUp(size_t size)
{
    return (size + SerializationAlignment - 1) & ~(SerializationAlignment - 1);
}

inline char* AlignUp(char* p)
{
    return reinterpret_cast<char*>(AlignUp(reinterpret_cast<uintptr_t>(p)));
}

inline char* AlignUp(char* p, size_t alignment)
{
    uintptr_t pn = reinterpret_cast<uintptr_t>(p);
    pn = (pn + alignment - 1) & ~(alignment - 1);
    return reinterpret_cast<char*>(pn);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

