#include "stdafx.h"
#include "string.h"
#include "error.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka UnderscoreCaseToCamelCase(const Stroka& data)
{
    Stroka result;
    bool upper = true;
    FOREACH (char c, data) {
        if (c == '_') {
            upper = true;
        } else {
            if (upper) {
                c = std::toupper(c);
            }
            result.push_back(c);
            upper = false;
        }
    }
    return result;
}

Stroka CamelCaseToUnderscoreCase(const Stroka& data)
{
    Stroka result;
    bool first = true;
    FOREACH (char c, data) {
        if (std::isupper(c)) {
            if (!first) {
                result.push_back('_');
            }
            result.push_back(std::tolower(c));
        } else {
            result.push_back(c);
        }
        first = false;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

Stroka TrimLeadingWhitespace(const Stroka& data)
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
        THROW_ERROR_EXCEPTION("Could not parse boolean value %s",
            ~Stroka(value).Quote());
    }
}

Stroka FormatBool(bool value)
{
    return value ? "true" : "false";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
