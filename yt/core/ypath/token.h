#pragma once

#include "public.h"

namespace NYT::NYPath {

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

TString ToYPathLiteral(TStringBuf value);
TString ToYPathLiteral(i64 value);

TStringBuf ExtractListIndex(TStringBuf token);
int ParseListIndex(TStringBuf token);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
