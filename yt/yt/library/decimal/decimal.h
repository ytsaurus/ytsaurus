#pragma once

#include <library/cpp/int128/int128.h>

#include <util/system/defaults.h>
#include <util/generic/string.h>

namespace NYT::NDecimal {

////////////////////////////////////////////////////////////////////////////////

class TDecimal
{
public:
    // Maximum precision supported by YT
    static constexpr int MaxPrecision = 35;
    static constexpr int MaxBinarySize = 16;

    // NB. Sometimes we print values that exceed MaxPrecision (e.g. in error messages)
    // MaxTextSize is chosen so we can print ANY i128 number as decimal.
    static constexpr int MaxTextSize =
        std::numeric_limits<ui128>::digits + 1 // max number of digits in ui128 number
        + 1 // possible decimal point
        + 1; // possible minus sign

    static void ValidatePrecisionAndScale(int precision, int scale);

    static void ValidateBinaryValue(TStringBuf binaryValue, int precision, int scale);

    static TString BinaryToText(TStringBuf binaryDecimal, int precision, int scale);
    static TString TextToBinary(TStringBuf textDecimal, int precision, int scale);

    // More efficient versions of conversion functions without allocations.
    // `buffer` must be at least of size MaxTextSize / MaxBinarySize.
    // Returned value is substring of buffer.
    static TStringBuf BinaryToText(TStringBuf binaryDecimal, int precision, int scale, char* buffer, size_t bufferLength);
    static TStringBuf TextToBinary(TStringBuf textDecimal, int precision, int scale, char* buffer, size_t bufferLength);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDecimal
