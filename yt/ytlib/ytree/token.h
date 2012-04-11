#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETokenType,
    (None) // Empty or uninitialized token (used for EndOfStream)

    (String)
    (Integer)
    (Double)

    // Special values:
    // YSON
    (Semicolon) // ;
    (Equals) // =
    (LeftBracket) // [
    (RightBracket) // ]
    (LeftBrace) // {
    (RightBrace) // }
    (LeftAngle) // <
    (RightAngle) // >
    // YPath
    (LeftParenthesis) // (
    (RightParenthesis) // )
    (Slash) // /
    (At) // @
    (Hash) // #
    (Bang) // !
    (Plus) // +
    (Caret) // ^
    (Colon) // :
    (Comma) // ,
    (Tilde) // ~
);

////////////////////////////////////////////////////////////////////////////////

class TLexer;

class TToken
{
    friend class TLexer;

public:
    static const TToken EndOfStream;

    TToken();

    DEFINE_BYVAL_RO_PROPERTY(ETokenType, Type);

    bool IsEmpty() const;
    const Stroka& GetStringValue() const;
    i64 GetIntegerValue() const;
    double GetDoubleValue() const;

    Stroka ToString() const;

    const TToken& CheckType(ETokenType expectedType) const;

private:
    Stroka StringValue;
    i64 IntegerValue;
    double DoubleValue;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(roizner): Add specifier
void ThrowUnexpectedToken(const TToken& token);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
