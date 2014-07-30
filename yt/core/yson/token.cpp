#include "stdafx.h"
#include "token.h"

#include <core/misc/error.h>
#include <util/string/vector.h>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

ETokenType CharToTokenType(char ch)
{
    switch (ch) {
        case ';': return ETokenType::Semicolon;
        case '=': return ETokenType::Equals;
        case '{': return ETokenType::LeftBrace;
        case '}': return ETokenType::RightBrace;
        case '#': return ETokenType::Hash;
        case '[': return ETokenType::LeftBracket;
        case ']': return ETokenType::RightBracket;
        case '<': return ETokenType::LeftAngle;
        case '>': return ETokenType::RightAngle;
        case '(': return ETokenType::LeftParenthesis;
        case ')': return ETokenType::RightParenthesis;
        case '+': return ETokenType::Plus;
        case ':': return ETokenType::Colon;
        case ',': return ETokenType::Comma;
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
        case ETokenType::Plus:              return '+';
        case ETokenType::Colon:             return ':';
        case ETokenType::Comma:             return ',';
        default:                            YUNREACHABLE();
    }
}

Stroka TokenTypeToString(ETokenType type)
{
    return Stroka(TokenTypeToChar(type));
}

////////////////////////////////////////////////////////////////////////////////

const TToken TToken::EndOfStream;

TToken::TToken()
    : Type_(ETokenType::EndOfStream)
    , Int64Value(0)
    , DoubleValue(0.0)
    , BooleanValue(false)
{ }

TToken::TToken(ETokenType type)
    : Type_(type)
    , Int64Value(0)
    , DoubleValue(0.0)
    , BooleanValue(false)
{
    switch (type) {
        case ETokenType::String:
        case ETokenType::Int64:
        case ETokenType::Double:
        case ETokenType::Boolean:
            YUNREACHABLE();
        default:
            break;
    }
}

TToken::TToken(const TStringBuf& stringValue)
    : Type_(ETokenType::String)
    , StringValue(stringValue)
    , Int64Value(0)
    , DoubleValue(0.0)
    , BooleanValue(false)
{ }

TToken::TToken(i64 int64Value)
    : Type_(ETokenType::Int64)
    , Int64Value(int64Value)
    , DoubleValue(0.0)
    , BooleanValue(false)
{ }

TToken::TToken(double doubleValue)
    : Type_(ETokenType::Double)
    , Int64Value(0)
    , DoubleValue(doubleValue)
    , BooleanValue(false)
{ }

TToken::TToken(bool booleanValue)
    : Type_(ETokenType::Boolean)
    , Int64Value(0)
    , DoubleValue(0.0)
    , BooleanValue(booleanValue)
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

i64 TToken::GetInt64Value() const
{
    CheckType(ETokenType::Int64);
    return Int64Value;
}

double TToken::GetDoubleValue() const
{
    CheckType(ETokenType::Double);
    return DoubleValue;
}

bool TToken::GetBooleanValue() const
{
    CheckType(ETokenType::Boolean);
    return BooleanValue;
}

void TToken::CheckType(const std::vector<ETokenType>& expectedTypes) const
{
    if (expectedTypes.size() == 1) {
        CheckType(expectedTypes.front());
    } else if (std::find(expectedTypes.begin(), expectedTypes.end(), Type_) == expectedTypes.end()) {
        auto typesString = JoinStroku(expectedTypes.begin(), expectedTypes.end(), " or ");
        if (Type_ == ETokenType::EndOfStream) {
            THROW_ERROR_EXCEPTION("Unexpected end of stream (ExpectedType: %v)",
                typesString);
        } else {
            THROW_ERROR_EXCEPTION("Unexpected token (Token: %Qv, Type: %v, ExpectedTypes: %v)",
                *this,
                Type_,
                typesString);
        }
    }
}

void TToken::CheckType(ETokenType expectedType) const
{
    if (Type_ != expectedType) {
        if (Type_ == ETokenType::EndOfStream) {
            THROW_ERROR_EXCEPTION("Unexpected end of stream (ExpectedType: %v)",
                expectedType);
        } else {
            THROW_ERROR_EXCEPTION("Unexpected token (Token: %Qv, Type: %v, ExpectedType: %v)",
                *this,
                Type_,
                expectedType);
        }
    }
}

void TToken::Reset()
{
    Type_ = ETokenType::EndOfStream;
    Int64Value = 0;
    DoubleValue = 0.0;
    StringValue = TStringBuf();
    BooleanValue = false;
}

Stroka ToString(const TToken& token)
{
    switch (token.GetType()) {
        case ETokenType::EndOfStream:
            return Stroka();

        case ETokenType::String:
            return Stroka(token.GetStringValue());

        case ETokenType::Int64:
            return ::ToString(token.GetInt64Value());

        case ETokenType::Double:
            return ::ToString(token.GetDoubleValue());

        case ETokenType::Boolean:
            return Stroka(FormatBool(token.GetBooleanValue()));

        default:
            return TokenTypeToString(token.GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
