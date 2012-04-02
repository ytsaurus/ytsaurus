#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETokenType,
    (None) // Empty or uninitialized token (used for EndOfStream)

    (String)
    (Int64)
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
    (Slash) // /
    (At) // @
    (Hash) // #
    (Bang) // !
    (Plus) // +
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
    i64 GetInt64Value() const;
    double GetDoubleValue() const;

    Stroka ToString() const;

private:
    Stroka StringValue;
    i64 Int64Value;
    double DoubleValue;
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
