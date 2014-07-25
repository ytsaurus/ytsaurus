#include "stdafx.h"
#include "string.h"
#include "error.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void UnderscoreCaseToCamelCase(TStringBuilder* builder, const TStringBuf& str)
{
    bool first = true;
    bool upper = true;
    for (char c : str) {
        if (c == '_') {
            upper = true;
        } else {
            if (upper) {
                if (!std::isalpha(c) && !first) {
                    builder->AppendChar('_');
                }
                c = std::toupper(c);
            }
            builder->AppendChar(c);
            upper = false;
        }
        first = false;
    }
}

Stroka UnderscoreCaseToCamelCase(const TStringBuf& str)
{
    TStringBuilder builder;
    UnderscoreCaseToCamelCase(&builder, str);
    return builder.Flush();
}

void CamelCaseToUnderscoreCase(TStringBuilder* builder, const TStringBuf& str)
{
    bool first = true;
    for (char c : str) {
        if (std::isupper(c) && std::isalpha(c)) {
            if (!first) {
                builder->AppendChar('_');
            }
            c = std::tolower(c);
        }
        builder->AppendChar(c);
        first = false;
    }
}

Stroka CamelCaseToUnderscoreCase(const TStringBuf& str)
{
    TStringBuilder builder;
    CamelCaseToUnderscoreCase(&builder, str);
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

Stroka TrimLeadingWhitespaces(const Stroka& str)
{
    for (int i = 0; i < str.size(); ++i) {
        if (str[i] != ' ') {
            return str.substr(i);
        }
    }
    return "";
}

////////////////////////////////////////////////////////////////////////////////

bool ParseBool(const Stroka& value)
{
    if (value == "true") {
        return true;
    } else if (value == "false") {
        return false;
    } else {
        THROW_ERROR_EXCEPTION("Error parsing boolean value %s",
            ~value.Quote());
    }
}

Stroka FormatBool(bool value)
{
    return value ? "true" : "false";
}

////////////////////////////////////////////////////////////////////////////////

Stroka DecodeEnumValue(const Stroka& value)
{
    auto camelValue = UnderscoreCaseToCamelCase(value);
    return camelValue;
    // TODO(babenko): restore this check; see YT-682
    //auto underscoreValue = CamelCaseToUnderscoreCase(camelValue);
    //if (value != underscoreValue) {
    //    THROW_ERROR_EXCEPTION("Enum value %Qv is not in a proper underscore case; did you mean %Qv?",
    //        value,
    //        underscoreValue);
    //}
    //return camelValue;
}

Stroka EncodeEnumValue(const Stroka& value)
{
    return CamelCaseToUnderscoreCase(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
