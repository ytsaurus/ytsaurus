#include "string.h"
#include "error.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool TryParseBool(const TString& value, bool& result)
{
    if (value == "true" || value == "1") {
        result = true;
        return true;
    } else if (value == "false" || value == "0") {
        result = false;
        return true;
    } else {
        return false;
    }
}

bool ParseBool(const TString& value)
{
    bool result;
    if (!TryParseBool(value, result)) {
        THROW_ERROR_EXCEPTION("Error parsing boolean value %Qv",
            value);
    }
    return result;
}

TStringBuf FormatBool(bool value)
{
    return value ? TStringBuf("true") : TStringBuf("false");
}

////////////////////////////////////////////////////////////////////////////////

TString DecodeEnumValue(TStringBuf value)
{
    auto camelValue = UnderscoreCaseToCamelCase(value);
    auto underscoreValue = CamelCaseToUnderscoreCase(camelValue);
    if (value != underscoreValue) {
        THROW_ERROR_EXCEPTION("Enum value %Qv is not in a proper underscore case; did you mean %Qv?",
            value,
            underscoreValue);
    }
    return camelValue;
}

TString EncodeEnumValue(TStringBuf value)
{
    return CamelCaseToUnderscoreCase(value);
}

void ThrowEnumParsingError(TStringBuf name, TStringBuf value)
{
    THROW_ERROR_EXCEPTION("Error parsing %v value %qv", name, value);
}

void FormatUnknownEnum(TStringBuilderBase* builder, TStringBuf name, i64 value)
{
    builder->AppendFormat("%v(%v)", name, value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
