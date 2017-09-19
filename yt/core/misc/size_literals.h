#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// NOTE: Literal operator has restrictions on parameter list that's why
// unsigned long long is used instead of i64.
// See http://en.cppreference.com/w/cpp/language/user_literal

constexpr i64 operator"" _B(unsigned long long value) noexcept
{
    return value;
}

constexpr i64 operator"" _KB(unsigned long long value) noexcept
{
    return value * 1024_B;
}

constexpr i64 operator"" _MB(unsigned long long value) noexcept
{
    return value * 1024_KB;
}

constexpr i64 operator"" _GB(unsigned long long value) noexcept
{
    return value * 1024_MB;
}

constexpr i64 operator"" _TB(unsigned long long value) noexcept
{
    return value * 1024_GB;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
