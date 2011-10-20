#include "../misc/stdafx.h"
#include "pattern_formatter.h"

#include <util/stream/str.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const char Dollar = '$';
static const char LeftParen = '(';
static const char RightParen = ')';
static const char Question = '?';

////////////////////////////////////////////////////////////////////////////////

void TPatternFormatter::AddProperty(Stroka name, Stroka value)
{
    PropertyMap[name] = value;
}

Stroka TPatternFormatter::Format(Stroka pattern)
{
    Stroka result;

    for (size_t pos = 0; pos < pattern.size(); ++pos) {
        if (pattern[pos] != Dollar) {
            result.append(pattern[pos]);
            continue;
        }
        ++pos;

        if (pos >= pattern.size() || pattern[pos] != LeftParen) {
            ythrow yexception() <<
                   Sprintf("expected \"%c\" at position %d", LeftParen, (int) pos);
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
            ythrow yexception() <<
                Sprintf("can't find \"%c\" for \"%c\" at position %d",
                        RightParen, LeftParen, (int) startProperty - 1);
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
                ythrow yexception() <<
                    Sprintf("property %s wasn't defined", ~property);
            }
        } else {
            result.append(it->second);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
