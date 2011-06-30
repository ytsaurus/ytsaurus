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
        size_t nextPos = pos + 1;
        if (nextPos >= pattern.size() || pattern[nextPos] != LeftParen) {
            ythrow yexception() <<
                   Sprintf("expected \"%c\" at position %d", LeftParen, (int) nextPos);
        }
        Stroka property;
        bool found = false;
        bool isOptional = false;
        for (pos = nextPos + 1; pos < pattern.size(); ++pos) {
            if (pattern[pos] == RightParen) {
                found = true;
                TPropertyMap::iterator it = PropertyMap.find(property);
                if (it == PropertyMap.end()) {
                    if (!isOptional) {
                        ythrow yexception() <<
                            Sprintf("property %s wasn't defined", ~property);
                    }
                } else {
                    result.append(it->second);
                }
                break;
            }
            if (pattern[pos] != Question) {
                property.append(pattern[pos]);
            } else {
                isOptional = true;
            }
        }
        if (!found) {
            ythrow yexception() <<
                Sprintf("can't find \"%c\" for \"%c\" at position %d",
                        RightParen, LeftParen, (int) nextPos);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
