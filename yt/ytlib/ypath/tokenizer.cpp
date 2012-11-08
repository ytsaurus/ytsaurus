#include "tokenizer.h"

#include <ytlib/misc/error.h>

namespace NYT {
namespace NYPath {

////////////////////////////////////////////////////////////////////////////////

TTokenizer::TTokenizer(const TYPath& path)
{
    Path_ = path;
    Input_ = Path_;
    Token_ = TStringBuf();
    Type_  = ETokenType::StartOfStream;
    LiteralValue_.reserve(Path_.length());
}

ETokenType TTokenizer::Advance()
{
    // Replace Input_ with suffix.
    Input_ = TStringBuf(Input_.begin() + Token_.length(), Input_.end());
    LiteralValue_.clear();

    // Check for EndOfStream.
    const char* current = Input_.begin();
    if (current == Input_.end()) {
        Type_ = ETokenType::EndOfStream;
        return Type_;
    }

    Type_ = ETokenType::Literal;
    bool proceed = true;
    while (proceed && current != Input_.end()) {
        switch (*current) {
            case '/':
            case '@':
            case '&':
                if (current == Input_.begin()) {
                    Token_ = TStringBuf(current, current + 1);
                    switch (*current) {
                        case '/': Type_ = ETokenType::Slash;     break;
                        case '@': Type_ = ETokenType::At;        break;
                        case '&': Type_ = ETokenType::Ampersand; break;
                        default:  YUNREACHABLE();
                    }
                    return Type_;
                }
                proceed = false;
                break;

            // TODO(babenko): legacy
            case '[':
            case '{':
                proceed = false;
                break;

            case '\\':
                current = AdvanceEscaped(current);
                break;

            default:
                LiteralValue_.append(*current);
                ++current;
                break;
        }
    }

    Token_ = TStringBuf(Input_.begin(), current);
    return Type_;
}

void TTokenizer::ThrowMalformedEscapeSequence(const TStringBuf& context)
{
    THROW_ERROR_EXCEPTION("Malformed escape sequence: %s",
        ~context);
}

const char* TTokenizer::AdvanceEscaped(const char* current)
{
    YASSERT(*current == '\\');
    ++current;

    if (current == Input_.end()) {
        THROW_ERROR_EXCEPTION("Premature end-of-string while parsing escape sequence");
    }

    switch (*current) {
        case '\\':
        case '/':
        case '@':
        case '&':
        case '[':
        case '{':
            LiteralValue_.append(*current);
            ++current;
            break;

        case 'x': {
            if (current + 2 >= Input_.end()) {
                ThrowMalformedEscapeSequence(TStringBuf(current - 1, Input_.end()));
            }
            TStringBuf context(current - 1, current + 3);
            int hi = ParseHexDigit(current[1], context);
            int lo = ParseHexDigit(current[2], context);
            LiteralValue_.append((hi << 4) + lo);
            current = context.end();
            break;
        }

        default:
            ThrowMalformedEscapeSequence(TStringBuf(current - 1, current + 1));
    }

    return current;
}

int TTokenizer::ParseHexDigit(char ch, const TStringBuf& context)
{
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    
    if (ch >= 'a' && ch <= 'f') {
        return ch - 'a' + 10;
    }
    
    if (ch >= 'A' && ch <= 'F') {
        return ch - 'A' + 10;
    }

    ThrowMalformedEscapeSequence(context);
    YUNREACHABLE();
}

void TTokenizer::Expect(ETokenType expectedType)
{
    if (expectedType != Type_) {
        if (Type_ == ETokenType::EndOfStream) {
            THROW_ERROR_EXCEPTION("Premature end-of-string while expecting %s",
                ~expectedType.ToString());
        } else {
            THROW_ERROR_EXCEPTION("%s found while expecting %s: %s",
                ~Type_.ToString(),
                ~expectedType.ToString(),
                ~Token_);
        }
    }
}

void TTokenizer::ThrowUnexpected()
{
    THROW_ERROR_EXCEPTION("Unexpected %s: %s",
        ~Type_.ToString(),
        ~Token_);
}

ETokenType TTokenizer::GetType() const
{
    return Type_;
}

const TStringBuf& TTokenizer::GetToken() const
{
    return Token_;
}

TYPath TTokenizer::GetSuffix() const
{
    return TYPath(Input_.begin() + Token_.length(), Input_.end());
}

TYPath TTokenizer::GetInput() const
{
    return TYPath(Input_);
}

const Stroka& TTokenizer::GetLiteralValue() const
{
    return LiteralValue_;
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYPath
} // namespace NYT
