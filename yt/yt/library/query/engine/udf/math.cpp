#include <yt/yt/library/query/misc/udf_cpp_abi.h>

#include <cmath>
#include <numeric>
#include <bit>

using namespace NYT::NQueryClient::NUdf;

extern "C" double math_acos(
    TExpressionContext * /*context*/,
    double value)
{
    return std::acos(value);
}

extern "C" double math_asin(
    TExpressionContext * /*context*/,
    double value)
{
    return std::asin(value);
}

extern "C" int64_t math_ceil(
    TExpressionContext * /*context*/,
    double value)
{
    return std::ceil(value);
}

extern "C" double math_cbrt(
    TExpressionContext * /*context*/,
    double value)
{
    return std::cbrt(value);
}

extern "C" double math_cos(
    TExpressionContext * /*context*/,
    double value)
{
    return std::cos(value);
}

extern "C" double math_cot(
    TExpressionContext * /*context*/,
    double value)
{
    return 1.0 / std::tan(value);
}

extern "C" double math_degrees(
    TExpressionContext * /*context*/,
    double value)
{
    return value * 180.0 / M_PI;
}

extern "C" int64_t math_even(
    TExpressionContext * /*context*/,
    double value)
{
    /**
     * Round to next even number by rounding away from zero.
     * For example:
     * math_even(2.5) -> 4
     * math_even(-2.5) -> -4
     */
    int64_t result;
    if (value >= 0) {
        result = std::ceil(value);
    } else {
        result = std::ceil(-value);
        result = -result;
    }

    if (result % 2 != 0) {
        if (value >= 0) {
            result += 1;
        } else {
            result -= 1;
        }
    }

    return result;
}

extern "C" double math_exp(
    TExpressionContext * /*context*/,
    double value)
{
    return std::exp(value);
}

extern "C" int64_t math_floor(
    TExpressionContext * /*context*/,
    double value)
{
    return std::floor(value);
}

extern "C" double math_gamma(
    TExpressionContext * /*context*/,
    double value)
{
    return std::tgamma(value);
}

extern "C" int8_t math_is_inf(
    TExpressionContext * /*context*/,
    double value)
{
    return std::isinf(value);
}

extern "C" double math_lgamma(
    TExpressionContext * /*context*/,
    double value)
{
    return std::lgamma(value);
}

extern "C" double math_ln(
    TExpressionContext * /*context*/,
    double value)
{
    return std::log(value);
}

extern "C" double math_log(
    TExpressionContext * /*context*/,
    double value)
{
    return std::log10(value);
}

extern "C" double math_log10(
    TExpressionContext * /*context*/,
    double value)
{
    return std::log10(value);
}

extern "C" double math_log2(
    TExpressionContext * /*context*/,
    double value)
{
    return std::log2(value);
}

extern "C" double math_radians(
    TExpressionContext * /*context*/,
    double value)
{
    return value * M_PI / 180.0;
}

extern "C" int64_t math_sign(
    TExpressionContext * /*context*/,
    double value)
{
    return value < 0 ? -1 : value > 0 ? 1 : 0;
}

extern "C" int8_t math_signbit(
    TExpressionContext * /*context*/,
    double value)
{
    return std::signbit(value);
}

extern "C" double math_sin(
    TExpressionContext * /*context*/,
    double value)
{
    return std::sin(value);
}

extern "C" double math_sqrt(
    TExpressionContext * /*context*/,
    double value)
{
    return std::sqrt(value);
}

extern "C" double math_tan(
    TExpressionContext * /*context*/,
    double value)
{
    return std::tan(value);
}

extern "C" int64_t math_trunc(
    TExpressionContext * /*context*/,
    double value)
{
    return std::trunc(value);
}

extern "C" int64_t math_bit_count(
    TExpressionContext * /*context*/,
    uint64_t value)
{
    return std::popcount(value);
}

extern "C" double math_atan2(
    TExpressionContext * /*context*/,
    double x,
    double y)
{
    return std::atan2(x, y);
}

static constexpr uint64_t FactorialValues[] = {
    1ULL, 1ULL, 2ULL, 6ULL, 24ULL, 120ULL, 720ULL, 5040ULL, 40320ULL, 362880ULL, 3628800ULL,
    39916800ULL, 479001600ULL, 6227020800ULL, 87178291200ULL, 1307674368000ULL,
    20922789888000ULL, 355687428096000ULL, 6402373705728000ULL,
    121645100408832000ULL, 2432902008176640000ULL
};

extern "C" uint64_t math_factorial(
    TExpressionContext * /*context*/,
    uint64_t value)
{
    constexpr size_t maxFactorialIndex = sizeof(FactorialValues) / sizeof(FactorialValues[0]);

    if (value < maxFactorialIndex) {
        return FactorialValues[value];
    } else {
        ThrowException("Factorial argument is too big");
        __builtin_unreachable();
    }
}

extern "C" uint64_t math_gcd(
    TExpressionContext * /*context*/,
    uint64_t x,
    uint64_t y)
{
    return std::gcd(x, y);
}

extern "C" uint64_t math_lcm(
    TExpressionContext * /*context*/,
    uint64_t x,
    uint64_t y)
{
    return std::lcm(x, y);
}

extern "C" double math_pow(
    TExpressionContext * /*context*/,
    double x,
    double y)
{
    return std::pow(x, y);
}

extern "C" int64_t math_round(
    TExpressionContext * /*context*/,
    double value)
{
    return std::round(value);
}

extern "C" uint64_t math_xor(
    TExpressionContext * /*context*/,
    uint64_t x,
    uint64_t y)
{
    return x ^ y;
}
