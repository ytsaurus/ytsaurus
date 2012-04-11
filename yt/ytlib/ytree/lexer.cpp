#include "stdafx.h"
#include "lexer.h"

#include "token.h"

#include <ytlib/misc/zigzag.h>
#include <ytlib/misc/property.h>

#include <util/string/escape.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TLexer::TImpl
{
public:
    DECLARE_ENUM(EInnerState,
        (None)
        (InsideBinaryInteger)
        (InsideBinaryDouble)
        (InsideBinaryString)
        (InsideUnquotedString)
        (InsideQuotedString)
        (InsideNumeric)
        (InsideDouble)
        (AfterPlus)
    );

    TImpl()
    {
        Reset();
    }

    void Reset()
    {
        State_ = EState::None;
        InnerState = EInnerState::None;
        Token = TToken();
        BytesRead = 0;
    }

    const TToken& GetToken() const
    {
        YASSERT(State_ == EState::Terminal);
        return Token;
    }
    
    //! Returns true iff the character was read.
    bool Consume(char ch)
    {
        switch (State_) {
            case EState::None:
                ConsumeStart(ch);
                return true;

            case EState::InProgress:
                switch (InnerState) {
                    case EInnerState::InsideUnquotedString:
                        return ConsumeUnquotedString(ch);

                    case EInnerState::InsideQuotedString:
                        ConsumeQuotedString(ch);
                        return true;

                    case EInnerState::InsideBinaryString:
                        ConsumeBinaryString(ch);
                        return true;

                    case EInnerState::InsideNumeric:
                        return ConsumeNumeric(ch);

                    case EInnerState::InsideDouble:
                        return ConsumeDouble(ch);

                    case EInnerState::InsideBinaryInteger:
                        ConsumeBinaryInteger(ch);
                        return true;

                    case EInnerState::InsideBinaryDouble:
                        ConsumeBinaryDouble(ch);
                        return true;

                    case EInnerState::AfterPlus:
                    	return ConsumePlus(ch);

                    default:
                        YUNREACHABLE();
                }

            default:
                // Should not consume chars in terminal states
                YUNREACHABLE();
        }
    }

    void Finish()
    {
        switch (State_) {
            case EState::InProgress:
                switch (InnerState) {
                    case EInnerState::InsideBinaryInteger:
                    case EInnerState::InsideBinaryDouble:
                    case EInnerState::InsideBinaryString:
                    case EInnerState::InsideQuotedString:
                        ythrow yexception() << Sprintf("Premature end of stream (LexerState: %s, BytesRead: %d)",
                            ~InnerState.ToString(),
                            BytesRead);
    
                    case EInnerState::InsideUnquotedString:
                        FinishString();
                        break;

                    case EInnerState::InsideNumeric:
                        FinishNumeric();
                        break;

                    case EInnerState::InsideDouble:
                        FinishDouble();
                        break;

                    case EInnerState::AfterPlus:
                    	FinishPlus();
                    	break;

                    default:
                        YUNREACHABLE();
                }
                break;

            default:
                break;
        }
        
    }

    DEFINE_BYVAL_RO_PROPERTY(EState, State)

private:

    void ConsumeStart(char ch)
    {
        if (isspace(ch))
            return;

        switch (ch) {
            case ';':
                ProduceToken(ETokenType::Semicolon, Stroka(ch));
                break;

            case '=':
                ProduceToken(ETokenType::Equals, Stroka(ch));
                break;

            case '[':
                ProduceToken(ETokenType::LeftBracket, Stroka(ch));
                break;

            case ']':
                ProduceToken(ETokenType::RightBracket, Stroka(ch));
                break;

            case '{':
                ProduceToken(ETokenType::LeftBrace, Stroka(ch));
                break;

            case '}':
                ProduceToken(ETokenType::RightBrace, Stroka(ch));
                break;

            case '<':
                ProduceToken(ETokenType::LeftAngle, Stroka(ch));
                break;

            case '>':
                ProduceToken(ETokenType::RightAngle, Stroka(ch));
                break;

            case '(':
                ProduceToken(ETokenType::LeftParenthesis, Stroka(ch));
                break;

            case ')':
                ProduceToken(ETokenType::RightParenthesis, Stroka(ch));
                break;

            case '/':
                ProduceToken(ETokenType::Slash, Stroka(ch));
                break;

            case '@':
                ProduceToken(ETokenType::At, Stroka(ch));
                break;

            case '#':
                ProduceToken(ETokenType::Hash, Stroka(ch));
                break;

            case '!':
                ProduceToken(ETokenType::Bang, Stroka(ch));
                break;

            case '+':
            	SetInProgressState(EInnerState::AfterPlus);
            	break;

            case '^':
                ProduceToken(ETokenType::Caret, Stroka(ch));
                break;

            case ',':
                ProduceToken(ETokenType::Comma, Stroka(ch));
                break;

            case ':':
                ProduceToken(ETokenType::Colon, Stroka(ch));
                break;

            case '~':
                ProduceToken(ETokenType::Tilde, Stroka(ch));
                break;

            case '\x01':
                SetInProgressState(EInnerState::InsideBinaryString);
                YASSERT(Token.StringValue.empty());
                YASSERT(BytesRead == 0);
                break;

            case '\x02':
                SetInProgressState(EInnerState::InsideBinaryInteger);
                YASSERT(Token.IntegerValue == 0);
                YASSERT(BytesRead == 0);
                break;

            case '\x03':
                SetInProgressState(EInnerState::InsideBinaryDouble);
                YASSERT(Token.DoubleValue == 0.0);
                YASSERT(BytesRead == 0);
                BytesRead = -static_cast<int>(sizeof(double));
                break;

            case '"':
                SetInProgressState(EInnerState::InsideQuotedString);
                YASSERT(Token.StringValue.empty());
                YASSERT(BytesRead == 0);
                break;

            default:
                if (isspace(ch)) {
                    break;
                } else if (isdigit(ch) || ch == '-') { // case of '+' is handled in AfterPlus state
                    Token.StringValue = Stroka(ch);
                    SetInProgressState(EInnerState::InsideNumeric);
                } else if (isalpha(ch) || ch == '_' || ch == '%') {
                    Token.StringValue = Stroka(ch);
                    SetInProgressState(EInnerState::InsideUnquotedString);
                } else {
                    ythrow yexception() << Sprintf("Unexpected character %s",
                        ~Stroka(ch).Quote());
                }
                break;
        }
    }

    bool ConsumeUnquotedString(char ch)
    {
        if (isalpha(ch) || isdigit(ch) || ch == '_' || ch == '-' || ch == '%') {
            Token.StringValue.append(ch);
            return true;
        } else {
            FinishString();
            return false;
        }
    }

    void ConsumeQuotedString(char ch)
    {
        bool finish = false;
        auto& stringValue = Token.StringValue;
        if (ch != '"') {
            stringValue.append(ch);
        } else {
            // We must count the number of '\' at the end of StringValue
            // to check if it's not \"
            int slashCount = 0;
            int length = stringValue.length();
            while (slashCount < length && stringValue[length - 1 - slashCount] == '\\')
                ++slashCount;
            if (slashCount % 2 == 0) {
                finish = true;
            } else {
                stringValue.append(ch);
            }
        }

        if (finish) {
            stringValue = UnescapeC(stringValue);
            FinishString();
        } else {
            ++BytesRead;
        }
    }

    void ConsumeBinaryInteger(char ch) {
        ui8 byte = static_cast<ui8>(ch);

        if (7 * BytesRead > 8 * sizeof(ui64) ) {
            ythrow yexception() << Sprintf("The data is too long to read binary Integer");
        }

        ui64 ui64Value = static_cast<ui64>(Token.IntegerValue);
        ui64Value |= (static_cast<ui64> (byte & 0x7F)) << (7 * BytesRead);
        ++BytesRead;

        if ((byte & 0x80) == 0) {
            Token.IntegerValue = ZigZagDecode64(static_cast<ui64>(ui64Value));
            ProduceToken(ETokenType::Integer);
            BytesRead = 0;
        } else {
            Token.IntegerValue = static_cast<i64>(ui64Value);
        }
    }

    void ConsumeBinaryString(char ch)
    {
        if (BytesRead < 0) {
            Token.StringValue.append(ch);
            ++BytesRead;
        } else { // We are reading length
            ConsumeBinaryInteger(ch);

            if (State_ == EState::Terminal) {
                i64 length = Token.GetIntegerValue();
                if (length < 0) {
                    ythrow yexception() << Sprintf("Error reading binary string: String cannot have negative length (Length: %" PRId64 ")",
                        length);
                }
                Reset();
                SetInProgressState(EInnerState::InsideBinaryString);
                BytesRead = -length;
            } else {
                YASSERT(BytesRead > 0); // So we won't FinishString
            }
        }

        if (BytesRead == 0) {
            FinishString();
        }
    }

    bool ConsumeNumeric(char ch)
    {
        if (isdigit(ch) || ch == '+' || ch == '-') { // Seems like it can't be '+' or '-'
            Token.StringValue.append(ch);
            return true;
        } else if (ch == '.' || ch == 'e' || ch == 'E') {
            Token.StringValue.append(ch);
            InnerState = EInnerState::InsideDouble;
            return true;
        } else if (isalpha(ch)) {
            ythrow yexception() << Sprintf("Unexpected character in numeric (Char: %s, Token: %s)",
                ~Stroka(ch).Quote(),
                ~Token.StringValue);
        } else {
            FinishNumeric();
            return false;
        }
    }

    bool ConsumeDouble(char ch)
    {
        if (isdigit(ch) ||
            ch == '+' || ch == '-' ||
            ch == '.' ||
            ch == 'e' || ch == 'E')
        {
            Token.StringValue.append(ch);
            return true;
        } else if (isalpha(ch)) {
            ythrow yexception() << Sprintf("Unexpected character in numeric (Char: %s, Token: %s)",
                ~Stroka(ch).Quote(),
                ~Token.StringValue);
        } else {
            FinishDouble();
            return false;
        }
    }

    void ConsumeBinaryDouble(char ch)
    {
        ui8 byte = static_cast<ui8>(ch);

        *(reinterpret_cast<ui64*>(&Token.DoubleValue)) |=
            static_cast<ui64>(byte) << (8 * (8 + BytesRead));
        ++BytesRead;

        if (BytesRead == 0) {
            ProduceToken(ETokenType::Double);
        }
    }

    bool ConsumePlus(char ch)
    {
    	if (!isdigit(ch)) {
    		ProduceToken(ETokenType::Plus, "+");
    		return false;
    	}

    	Reset();
    	Token.StringValue.append('+');
    	Token.StringValue.append(ch);
    	SetInProgressState(EInnerState::InsideNumeric);
    	return true;
    }

    void FinishString()
    {
        ProduceToken(ETokenType::String);
    }

    void FinishNumeric()
    {
        try {
            Token.IntegerValue = FromString<i64>(Token.StringValue);
        } catch (const std::exception& ex) {
            // This exception is wrapped in parser
            ythrow yexception() << Sprintf("Failed to parse Integer literal %s",
                ~Token.StringValue.Quote());
        }
        Token.StringValue = Stroka();
        ProduceToken(ETokenType::Integer);
    }

    void FinishDouble()
    {
        try {
            Token.DoubleValue = FromString<double>(Token.StringValue);
        } catch (const std::exception& ex) {
            // This exception is wrapped in parser
            ythrow yexception() << Sprintf("Failed to parse Double literal %s",
                ~Token.StringValue.Quote());
        }
        Token.StringValue = Stroka();
        ProduceToken(ETokenType::Double);
    }

    void FinishPlus()
    {
    	ProduceToken(ETokenType::Plus, "+");
    }

    void ProduceToken(ETokenType type, Stroka stringValue = Stroka())
    {
        YASSERT(State_ == EState::None || State_ == EState::InProgress);
        State_ = EState::Terminal;
        InnerState = EInnerState::None;
        Token.Type_ = type;
        if (!stringValue.empty()) {
            Token.StringValue = stringValue;
        }
    }

    void SetInProgressState(EInnerState innerState)
    {
        YASSERT(State_ == EState::None);
        YASSERT(InnerState == EInnerState::None);
        YASSERT(innerState != EInnerState::None);
        State_ = EState::InProgress;
        InnerState = innerState;
    }

    EInnerState InnerState;
    TToken Token;

    /*
     * BytesRead > 0 means we've read BytesRead bytes (in binary Integer)
     * BytesRead < 0 means we are expecting -BytesRead bytes more (in binary doubles and strings)
     * BytesRead = 0 also means we don't the number of bytes yet
     */
    int BytesRead; // For varints
};

////////////////////////////////////////////////////////////////////////////////

TLexer::TLexer()
    : Impl(new TImpl())
{ }

TLexer::~TLexer()
{ }

bool TLexer::Consume(char ch)
{
    return Impl->Consume(ch);
}

void TLexer::Finish()
{
    Impl->Finish();
}

void TLexer::Reset()
{
    Impl->Reset();
}

TLexer::EState TLexer::GetState() const
{
    return Impl->GetState();
}

const TToken& TLexer::GetToken() const
{
    return Impl->GetToken();
}

////////////////////////////////////////////////////////////////////////////////

bool IsEmpty(const TStringBuf& data)
{
    return ChopToken(data).GetType() == ETokenType::None;
}

// TODO(roizner): Make suffix TStringBuf*
TToken ChopToken(const TStringBuf& data, Stroka* suffix)
{
    TLexer lexer;
    int position = 0;
    while (lexer.GetState() != TLexer::EState::Terminal && position < data.length()) {
        if (lexer.Consume(data[position])) {
            ++position;
        }
    }
    lexer.Finish();
    if (suffix) {
        *suffix = data.SubStr(position);
    }
    auto token = lexer.GetState() == TLexer::EState::Terminal ? lexer.GetToken() : TToken();
    return token;
}

Stroka ChopStringToken(const TStringBuf& data, Stroka* suffix)
{
    auto token = ChopToken(data, suffix);
    if (token.GetType() != ETokenType::String) {
        ythrow yexception() << Sprintf("Expected String token, but token %s of type %s found",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
    }
    return token.GetStringValue();
}

i64 ChopIntegerToken(const TStringBuf& data, Stroka* suffix)
{
    auto token = ChopToken(data, suffix);
    if (token.GetType() != ETokenType::Integer) {
        ythrow yexception() << Sprintf("Expected Integer token, but token %s of type %s found",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
    }
    return token.GetIntegerValue();
}

double ChopDoubleToken(const TStringBuf& data, Stroka* suffix)
{
    auto token = ChopToken(data, suffix);
    if (token.GetType() != ETokenType::Double) {
        ythrow yexception() << Sprintf("Expected Double token, but token %s of type %s found",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
    }
    return token.GetDoubleValue();
}

ETokenType ChopSpecialToken(const TStringBuf& data, Stroka* suffix)
{
    auto token = ChopToken(data, suffix);
    if (token.GetType() <= ETokenType::Double) {
        ythrow yexception() << Sprintf("Expected special value token, but token %s of type %s found",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
    }
    return token.GetType();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
