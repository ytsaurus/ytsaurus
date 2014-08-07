#include "stdafx.h"
#include "pattern_formatter.h"

#include <core/misc/error.h>

#include <util/stream/str.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const char Dollar = '$';
static const char LeftParen = '(';
static const char RightParen = ')';
static const char Question = '?';

////////////////////////////////////////////////////////////////////////////////

void TPatternFormatter::AddProperty(const Stroka& name, const Stroka& value)
{
    PropertyMap[name] = value;
}

Stroka TPatternFormatter::Format(const Stroka& pattern)
{
    Stroka result;

    for (size_t pos = 0; pos < pattern.size(); ++pos) {
        if (pattern[pos] != Dollar) {
            result.append(pattern[pos]);
            continue;
        }
        ++pos;

        if (pos >= pattern.size() || pattern[pos] != LeftParen) {
            THROW_ERROR_EXCEPTION("Expected \"%c\" at position %v",
                LeftParen,
                static_cast<int>(pos));
        }
        ++pos;

        bool foundRightParen = false;
        size_t startProperty = pos;
        size_t endProperty = 0;

        for (; pos < pattern.size(); ++pos) {
            if (pattern[pos] == RightParen) {
                foundRightParen = true;
                endProperty = pos;
                break;
            }
        }

        if (!foundRightParen) {
            THROW_ERROR_EXCEPTION("Cannot find a matching \"%c\" for \"%c\" at position %v",
                RightParen,
                LeftParen,
                static_cast<int>(startProperty) - 1);
        }

        bool isOptional = false;
        if (pattern[endProperty - 1] == Question) {
            --endProperty;
            isOptional = true;
        }

        Stroka property = pattern.substr(startProperty, endProperty - startProperty);
        auto it = PropertyMap.find(property);
        if (it == PropertyMap.end()) {
            if (!isOptional) {
                THROW_ERROR_EXCEPTION("Property %v is not defined", property);
            }
        } else {
            result.append(it->second);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
