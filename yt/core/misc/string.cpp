#include "stdafx.h"
#include "string.h"
#include "error.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka UnderscoreCaseToCamelCase(const Stroka& data)
{
    Stroka result;
    result.reserve(data.length());
    bool first = true;
    bool upper = true;
    FOREACH (char c, data) {
        if (c == '_') {
            upper = true;
        } else {
            if (upper) {
                if (!std::isalpha(c) && !first) {
                    result.push_back('_');
                }
                c = std::toupper(c);
            }
            result.push_back(c);
            upper = false;
        }
        first = false;
    }
    return result;
}

Stroka CamelCaseToUnderscoreCase(const Stroka& data)
{
    Stroka result;
    result.reserve(data.length() * 2);
    bool first = true;
    FOREACH (char c, data) {
        if (std::isupper(c) && std::isalpha(c)) {
            if (!first) {
                result.push_back('_');
            }
            c = std::tolower(c);
        }
        result.push_back(c);
        first = false;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

Stroka TrimLeadingWhitespaces(const Stroka& data)
{
    for (int i = 0; i < data.size(); ++i) {
        if (data[i] != ' ') {
            return data.substr(i);
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
        THROW_ERROR_EXCEPTION("Error parsing boolean value: %s",
            ~value);
    }
}

Stroka FormatBool(bool value)
{
    return value ? "true" : "false";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
