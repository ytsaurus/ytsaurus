#include "stdafx.h"
#include "token.h"

//#include <util/string/cast.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

ETokenType CharToTokenType(char ch)
{
    switch (ch) {
        case ';': return ETokenType::Semicolon;
        case '=': return ETokenType::Equals;
        case '#': return ETokenType::Hash;
        case '[': return ETokenType::LeftBracket;
        case ']': return ETokenType::RightBracket;
        case '{': return ETokenType::LeftBrace;
        case '}': return ETokenType::RightBrace;
        case '<': return ETokenType::LeftAngle;
        case '>': return ETokenType::RightAngle;
        case '(': return ETokenType::LeftParenthesis;
        case ')': return ETokenType::RightParenthesis;
        case '/': return ETokenType::Slash;
        case '@': return ETokenType::At;
        case '!': return ETokenType::Bang;
        case '+': return ETokenType::Plus;
        case '^': return ETokenType::Caret;
        case ':': return ETokenType::Colon;
        case ',': return ETokenType::Comma;
        case '~': return ETokenType::Tilde;
        case '&': return ETokenType::Ampersand;
        default:  return ETokenType::EndOfStream;
    }
}

char TokenTypeToChar(ETokenType type)
{
    switch (type) {
        case ETokenType::Semicolon:         return ';';
        case ETokenType::Equals:            return '=';
        case ETokenType::Hash:              return '#';
        case ETokenType::LeftBracket:       return '[';
        case ETokenType::RightBracket:      return ']';
        case ETokenType::LeftBrace:         return '{';
        case ETokenType::RightBrace:        return '}';
        case ETokenType::LeftAngle:         return '<';
        case ETokenType::RightAngle:        return '>';
        case ETokenType::LeftParenthesis:   return '(';
        case ETokenType::RightParenthesis:  return ')';
        case ETokenType::Slash:             return '/';
        case ETokenType::At:                return '@';
        case ETokenType::Bang:              return '!';
        case ETokenType::Plus:              return '+';
        case ETokenType::Caret:             return '^';
        case ETokenType::Colon:             return ':';
        case ETokenType::Comma:             return ',';
        case ETokenType::Tilde:             return '~';
        case ETokenType::Ampersand:         return '&';
        default:                            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

const TToken TToken::EndOfStream;

TToken::TToken(ETokenType type)
    : Type_(type)
    , IntegerValue(0)
    , DoubleValue(0.0)
{
    switch (type) {
        case ETokenType::String:
        case ETokenType::Integer:
        case ETokenType::Double:
            YUNREACHABLE();
        default:
            break;
    }
}

TToken::TToken(const TStringBuf& stringValue)
    : Type_(ETokenType::String)
    , StringValue(stringValue)
    , IntegerValue(0)
    , DoubleValue(0.0)
{ }

TToken::TToken(i64 integerValue)
    : Type_(ETokenType::Integer)
    , IntegerValue(integerValue)
    , DoubleValue(0.0)
{ }

TToken::TToken(double doubleValue)
    : Type_(ETokenType::Double)
    , IntegerValue(0)
    , DoubleValue(doubleValue)
{ }

bool TToken::IsEmpty() const
{
    return Type_ == ETokenType::EndOfStream;
}

const TStringBuf& TToken::GetStringValue() const
{
    CheckType(ETokenType::String);
    return StringValue;
}

i64 TToken::GetIntegerValue() const
{
    CheckType(ETokenType::Integer);
    return IntegerValue;
}

double TToken::GetDoubleValue() const
{
    CheckType(ETokenType::Double);
    return DoubleValue;
}

Stroka TToken::ToString() const
{
    switch (Type_) {
        case ETokenType::EndOfStream:
            return Stroka();

        case ETokenType::String:
            return Stroka(StringValue);

        case ETokenType::Integer:
            return ::ToString(IntegerValue);

        case ETokenType::Double:
            return ::ToString(DoubleValue);

        default:
            return Stroka(TokenTypeToChar(Type_));
    }
}

void TToken::CheckType(ETokenType expectedType) const
{
    if (Type_ != expectedType) {
        if (Type_ == ETokenType::EndOfStream) {
            ythrow yexception() << Sprintf("Unexpected end of stream (token of type %s was expected)",
                ~expectedType.ToString());
        } else {
            ythrow yexception() << Sprintf("Unexpected token (Token: %s, TokenType: %s, ExpectedType: %s)",
                ~ToString().Quote(),
                ~Type_.ToString(),
                ~expectedType.ToString());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ThrowUnexpectedToken(const TToken& token)
{
    ythrow yexception() << Sprintf("Unexpected token (Token: %s, TokenType: %s)",
        ~token.ToString().Quote(),
        ~token.GetType().ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
