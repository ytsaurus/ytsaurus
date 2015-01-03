#pragma once

#include "public.h"

namespace NYT {
namespace NYPath {

////////////////////////////////////////////////////////////////////////////////

extern TStringBuf WildcardToken;
extern TStringBuf SuppressRedirectToken;
extern TStringBuf ListBeginToken;
extern TStringBuf ListEndToken;
extern TStringBuf ListBeforeToken;
extern TStringBuf ListAfterToken;

DEFINE_ENUM(ETokenType,
    (Literal)
    (Slash)
    (Ampersand)
    (At)
    (StartOfStream)
    (EndOfStream)
    (Range)
);

Stroka ToYPathLiteral(const TStringBuf& value);
Stroka ToYPathLiteral(i64 value);

TStringBuf ExtractListIndex(const TStringBuf& token);
int ParseListIndex(const TStringBuf& token);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYPath
} // namespace NYT
