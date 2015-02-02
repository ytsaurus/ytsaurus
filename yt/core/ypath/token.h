#pragma once

#include "public.h"

namespace NYT {
namespace NYPath {

////////////////////////////////////////////////////////////////////////////////

extern const TStringBuf ListBeginToken;
extern const TStringBuf ListEndToken;
extern const TStringBuf ListBeforeToken;
extern const TStringBuf ListAfterToken;

DEFINE_ENUM(ETokenType,
    (Literal)
    (Slash)
    (Ampersand)
    (At)
    (Asterisk)
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
