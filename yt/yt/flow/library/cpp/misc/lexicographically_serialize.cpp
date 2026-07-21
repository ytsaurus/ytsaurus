#include "lexicographically_serialize.h"

#include <library/cpp/yt/error/error.h>
#include <library/cpp/yt/string/format.h>

#include <util/string/cast.h>
#include <util/string/escape.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFlow {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

std::string LexicographicallySerialize(ui64 value)
{
    // Total layout: one hex digit holding (hexLen-1), followed by hexLen lowercase hex digits.
    // hexLen is the number of significant hex digits of value (1..16); for value=0 it is 1.
    static constexpr char Hex[] = "0123456789abcdef";

    int hexLen = 1;
    for (ui64 tmp = value >> 4; tmp != 0; tmp >>= 4) {
        ++hexLen;
    }

    std::string result(static_cast<size_t>(hexLen) + 1, '\0');
    result[0] = Hex[hexLen - 1];
    for (int i = hexLen; i >= 1; --i) {
        result[i] = Hex[value & 0xfULL];
        value >>= 4;
    }
    return result;
}

void LexicographicallyRead(TStringBuf& serialized, ui64& destination)
{
    THROW_ERROR_EXCEPTION_IF(serialized.size() < 2ull, "Serialized value %Qv is too short", serialized);
    const ui64 length = IntFromString<ui64, 16>(serialized.substr(0, 1));
    serialized.Skip(1);
    THROW_ERROR_EXCEPTION_IF(length > serialized.size(), "Serialized value %Qv is too short", serialized);
    destination = IntFromString<ui64, 16>(serialized.substr(0, length + 1));
    serialized.Skip(length + 1);
}

////////////////////////////////////////////////////////////////////////////////

std::string DigitRevertHex(TStringBuf original)
{
    std::string result;
    result.reserve(original.size());
    for (const char c : original) {
        THROW_ERROR_EXCEPTION_IF(c < '0', "Bad character in hex string %Qv", original);
        if (c < '6') {
            result.push_back('f' - (c - '0'));
            continue;
        }
        if (c <= '9') {
            result.push_back('9' - (c - '6'));
            continue;
        }
        THROW_ERROR_EXCEPTION_IF(c < 'a' || c > 'f', "Bad character in hex string %Qv", original);
        result.push_back('5' - (c - 'a'));
    }
    return result;
}

std::string LexicographicallySerialize(i64 value)
{
    if (value >= 0) {
        return LexicographicallySerialize(static_cast<ui64>(value));
    }
    // Avoid integer overflow (when value is -2^63). Will be optimized by compiler.
    const ui64 castedValue = static_cast<ui64>(-(value + 1)) + 1u;
    return "-" + DigitRevertHex(LexicographicallySerialize(castedValue));
}

void LexicographicallyRead(TStringBuf& serialized, i64& destination)
{
    THROW_ERROR_EXCEPTION_IF(serialized.size() < 2ull, "Serialized value %Qv is too short", serialized);

    if (serialized[0] != '-') {
        ui64 value = 0;
        LexicographicallyRead(serialized, value);
        THROW_ERROR_EXCEPTION_IF(value > static_cast<ui64>(std::numeric_limits<i64>::max()), "Value %v is too big", value);
        destination = static_cast<i64>(value);
        return;
    }
    serialized.Skip(1);

    const ui64 length = 16 - IntFromString<ui64, 16>(serialized.substr(0, 1));
    const ui64 value = LexicographicallyParse<ui64>(DigitRevertHex(serialized.substr(0, length + 1)));
    serialized.Skip(length + 1);

    THROW_ERROR_EXCEPTION_IF(value > static_cast<ui64>(std::numeric_limits<i64>::max()) + 1u, "Value %v is too big for a negative number", value);
    THROW_ERROR_EXCEPTION_IF(value == 0, "Minus zero is not a correct serialized value: got %v", value);
    destination = -static_cast<i64>(value - 1u) - 1; // Avoid integer overflow (when value is 2^63). Will be optimized by compiler.
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

const unsigned char AsciiPrintableMin = 32;  // ' '. And the marker of string end.
const unsigned char AsciiPrintableMax = 126; // '~'. And high escaping symbol.

const unsigned char StringEscapingLowSymbol = AsciiPrintableMin + 1; // '!'. To escape low not readable chars and marker of string end.
const unsigned char StringEscapingLowBaseSymbol = 94;                // '^'. Nice symbol for translate AsciiPrintableMin to.

// To escape high not readable chars.
const unsigned char StringEscapingHigh1Symbol = AsciiPrintableMax - 1;          // '}'.
const unsigned char StringEscapingHigh2Symbol = AsciiPrintableMax;              // '~'.
const unsigned char StringEscapingHighBaseSymbol = StringEscapingLowSymbol + 1; // '"'.
const unsigned char StringEscapingHighOneSymbolCapacity = StringEscapingHigh1Symbol - StringEscapingHighBaseSymbol;

const unsigned char StringEscapingDelta = StringEscapingLowBaseSymbol - AsciiPrintableMin; // Translate AsciiPrintableMin to easy symbol '^' for readability.

static_assert(StringEscapingDelta > StringEscapingLowSymbol && StringEscapingLowSymbol + StringEscapingDelta < AsciiPrintableMax);
static_assert(256 - StringEscapingHigh1Symbol < 2 * (StringEscapingHigh1Symbol - StringEscapingLowSymbol - 1));

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::string LexicographicallySerialize(TStringBuf unescaped)
{
    // Just add companions after StringEscapedSymbol.
    size_t resultLength = unescaped.size();
    for (const unsigned char c : unescaped) {
        if (c <= StringEscapingLowSymbol || c >= StringEscapingHigh1Symbol) {
            resultLength += 1;
        }
    }
    resultLength += 1; // For end-of-string symbol.

    std::string result;
    result.reserve(resultLength);
    for (const unsigned char c : unescaped) {
        if (c <= StringEscapingLowSymbol) {
            result.push_back(StringEscapingLowSymbol);
            result.push_back(c + StringEscapingDelta);
        } else if (c >= StringEscapingHigh1Symbol) {
            int value = c - StringEscapingHigh1Symbol;
            if (value < StringEscapingHighOneSymbolCapacity) {
                result.push_back(StringEscapingHigh1Symbol);
                result.push_back(StringEscapingHighBaseSymbol + value);
            } else {
                result.push_back(StringEscapingHigh2Symbol);
                result.push_back(StringEscapingHighBaseSymbol + (value - StringEscapingHighOneSymbolCapacity));
            }
        } else {
            result.push_back(c);
        }
    }
    result.push_back(AsciiPrintableMin);
    return result;
}

void LexicographicallyRead(TStringBuf& serialized, std::string& destination)
{
    const size_t pos = serialized.find(AsciiPrintableMin);
    THROW_ERROR_EXCEPTION_IF(pos == TStringBuf::npos, "No string end marker found in serialized string %Qv", serialized);

    TStringBuf escaped = serialized.substr(0, pos);
    serialized.Skip(pos + 1);

    destination.clear();
    destination.reserve(escaped.size());
    for (size_t i = 0; i < escaped.size(); ++i) {
        const char c = escaped[i];

        auto consumeNextChar = [&] () {
            i += 1;
            THROW_ERROR_EXCEPTION_IF(i >= escaped.size(), "Unexpected end of string %Qv after escaping symbol", escaped);
            return escaped[i];
        };

        if (c == StringEscapingLowSymbol) {
            const char c1 = consumeNextChar();
            THROW_ERROR_EXCEPTION_IF(c1 < StringEscapingLowSymbol || c1 - StringEscapingDelta > StringEscapingLowSymbol,
                "Invalid character %Qv after escaping symbol in string %Qv",
                c1,
                escaped);
            destination.push_back(c1 - StringEscapingDelta);
        } else if (c == StringEscapingHigh1Symbol) {
            const char c1 = consumeNextChar();
            THROW_ERROR_EXCEPTION_IF(c1 < StringEscapingHighBaseSymbol || c1 >= StringEscapingHighBaseSymbol + StringEscapingHighOneSymbolCapacity,
                "Invalid character %Qv after escaping symbol in string %Qv",
                c1,
                escaped);
            destination.push_back(StringEscapingHigh1Symbol + (c1 - StringEscapingHighBaseSymbol));
        } else if (c == StringEscapingHigh2Symbol) {
            const char c1 = consumeNextChar();
            THROW_ERROR_EXCEPTION_IF(c1 < StringEscapingHighBaseSymbol || c1 >= StringEscapingHighBaseSymbol + StringEscapingHighOneSymbolCapacity,
                "Invalid character %Qv after escaping symbol in string %Qv",
                c1,
                escaped);
            destination.push_back(StringEscapingHigh1Symbol + StringEscapingHighOneSymbolCapacity + (c1 - StringEscapingHighBaseSymbol));
        } else {
            THROW_ERROR_EXCEPTION_IF(c > AsciiPrintableMax, "Only ASCII symbols can occur in escaped string %Qv", escaped);
            destination.push_back(c);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

std::string LexicographicallySerialize(const std::vector<std::string>& value)
{
    std::string result;
    for (const auto& part : value) {
        result.append(LexicographicallySerialize(TStringBuf(part)));
    }
    return LexicographicallySerialize(TStringBuf(result));
}

void LexicographicallyRead(TStringBuf& serialized, std::vector<std::string>& destination)
{
    std::string concatenatedValues;
    LexicographicallyRead(serialized, concatenatedValues);
    TStringBuf input = concatenatedValues;
    while (!input.empty()) {
        destination.emplace_back();
        LexicographicallyRead(input, destination.back());
    }
}

////////////////////////////////////////////////////////////////////////////////

std::string LexicographicallySerializeUnversionedRowV1(const TUnversionedRow& value)
{
    std::string result;
    for (const auto& unversionedValue : value) {
        switch (unversionedValue.Type) {
            case EValueType::Int64:
                result += LexicographicallySerialize(FromUnversionedValue<i64>(unversionedValue));
                break;
            case EValueType::Uint64:
                result += LexicographicallySerialize(FromUnversionedValue<ui64>(unversionedValue));
                break;
            case EValueType::String:
                result += LexicographicallySerialize(FromUnversionedValue<std::string>(unversionedValue));
                break;
            case EValueType::Null:
                result += "-";
                break;
            default:
                YT_UNIMPLEMENTED(Format("LexicographicallySerializeUnversionedRowV1(const NTableClient::TUnversionedRow&) does not support value type %Qv", unversionedValue.Type));
        }
    }
    return LexicographicallySerialize(result);
}

std::string LexicographicallySerializeUnversionedRowV2(const TUnversionedRow& value)
{
    std::string result;
    for (const auto& unversionedValue : value) {
        result += LexicographicallySerialize(static_cast<ui64>(unversionedValue.Type));
        switch (unversionedValue.Type) {
            case EValueType::Min:
                break;
            case EValueType::TheBottom:
                break;
            case EValueType::Null:
                break;
            case EValueType::Int64:
                result += LexicographicallySerialize(FromUnversionedValue<i64>(unversionedValue));
                break;
            case EValueType::Uint64:
                result += LexicographicallySerialize(FromUnversionedValue<ui64>(unversionedValue));
                break;
            case EValueType::String:
                result += LexicographicallySerialize(FromUnversionedValue<std::string>(unversionedValue));
                break;
            case EValueType::Max:
                break;
            default:
                YT_UNIMPLEMENTED(Format("LexicographicallySerializeUnversionedRowV2(const NTableClient::TUnversionedRow&) does not support value type %Qv", unversionedValue.Type));
        }
    }
    return LexicographicallySerialize(result);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

std::string RevertSerializedDigits(TStringBuf value)
{
    std::string result;
    result.reserve(value.size());
    for (const unsigned char c : value) {
        THROW_ERROR_EXCEPTION_IF(c < AsciiPrintableMin || c > AsciiPrintableMax, "Invalid character in serialized value %Qv", value);
        result.push_back(AsciiPrintableMax - (c - AsciiPrintableMin));
    }
    return result;
}

} // namespace

std::string RevertLexicographicallySerialized(TStringBuf serialized)
{
    return LexicographicallySerialize(RevertSerializedDigits(serialized));
}

std::string UndoRevertLexicographicallySerialized(TStringBuf revertedSerialized)
{
    return RevertSerializedDigits(LexicographicallyParse<std::string>(revertedSerialized));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
