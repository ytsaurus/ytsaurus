#pragma once

#include <util/system/defaults.h>
#include <util/generic/string.h>

namespace NYT::NDecimal {

////////////////////////////////////////////////////////////////////////////////

class TDecimal
{
public:
    static constexpr int MaxPrecision = 35;
    static constexpr int MaxTextSize = MaxPrecision + /*minus sign*/ 1 + /*decimal point*/ + 1;
    static constexpr int MaxBinarySize = 16;

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