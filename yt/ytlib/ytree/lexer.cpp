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
        (InsideBinaryInt64)
        (InsideBinaryDouble)
        (InsideBinaryString)
        (InsideUnquotedString)
        (InsideQuotedString)
        (InsideNumeric)
        (InsideDouble)
    );

    TImpl()
    {
        Reset();
    }

    void Reset()
    {
        YASSERT(InnerState == EState::None);
        YASSERT(State_ != EState::InProgress);
        State_ = EState::None;
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

                    case EInnerState::InsideBinaryInt64:
                        ConsumeBinaryInt64(ch);
                        return true;

                    case EInnerState::InsideBinaryDouble:
                        ConsumeBinaryDouble(ch);
                        return true;

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
            case EState::None:
                break;

            case EState::InProgress:
                switch (InnerState) {
                    case EInnerState::InsideBinaryInt64:
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

                    default:
                        YUNREACHABLE();
                }

            default:
                // Should not finish in terminal states
                YUNREACHABLE();
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
                break;;

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

            case '\x01':
                SetInProgressState(EInnerState::InsideBinaryString);
                YASSERT(Token.StringValue.empty());
                YASSERT(BytesRead == 0);
                break;

            case '\x02':
                SetInProgressState(EInnerState::InsideBinaryInt64);
                YASSERT(Token.Int64Value == 0);
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
                } else if (isdigit(ch) || ch == '+' || ch == '-') {
                    Token.StringValue = Stroka(ch);
                    SetInProgressState(EInnerState::InsideNumeric);
                } else if (isalpha(ch) || ch == '_') {
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
        if (isalpha(ch) || isdigit(ch) || ch == '_') {
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

    void ConsumeBinaryInt64(char ch) {
        ui8 byte = static_cast<ui8>(ch);

        if (7 * BytesRead > 8 * sizeof(ui64) ) {
            ythrow yexception() << Sprintf("The data is too long to read binary Int64");
        }

        ui64 ui64Value = static_cast<ui64>(Token.Int64Value);
        ui64Value |= (static_cast<ui64> (byte & 0x7F)) << (7 * BytesRead);
        ++BytesRead;

        if ((byte & 0x80) == 0) {
            Token.Int64Value = ZigZagDecode64(static_cast<ui64>(ui64Value));
            ProduceToken(ETokenType::Int64);
            BytesRead = 0;
        } else {
            Token.Int64Value = static_cast<i64>(ui64Value);
        }
    }

    void ConsumeBinaryString(char ch)
    {
        if (BytesRead < 0) {
            Token.StringValue.append(ch);
            ++BytesRead;
        } else { // We are reading length
            ConsumeBinaryInt64(ch);

            if (State_ == EState::Terminal) {
                BytesRead = -Token.GetInt64Value();
                Token.Int64Value = 0;
                State_ = EState::None; // SetInProgressState asserts it
                SetInProgressState(EInnerState::InsideBinaryString);
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

    void FinishString()
    {
        ProduceToken(ETokenType::String);
    }

    void FinishNumeric()
    {
        try {
            Token.Int64Value = FromString<i64>(Token.StringValue);
        } catch (const std::exception& ex) {
            // This exception is wrapped in parser
            ythrow yexception() << Sprintf("Failed to parse Int64 literal %s",
                ~Token.StringValue.Quote());
        }
        Token.StringValue = Stroka();
        ProduceToken(ETokenType::Int64);
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
     * BytesRead > 0 means we've read BytesRead bytes (in binary Int64)
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
      
namespace {

void DoChopToken(TLexer& lexer, const TStringBuf& data, TStringBuf* suffix)
{
    int position = 0;
    while (lexer.GetState() != TLexer::EState::Terminal && position < data.length()) {
        if (lexer.Consume(data[position])) {
            ++position;
        }
    }
    if (lexer.GetState() != TLexer::EState::Terminal) {
        lexer.Finish();
    }
    YASSERT(lexer.GetState() == TLexer::EState::Terminal);
    if (suffix) {
        *suffix = data.SubStr(position);
    }
}

} // namespace

TToken ChopToken(const TStringBuf& data, TStringBuf* suffix)
{
    TLexer lexer;
    DoChopToken(lexer, data, suffix);
    return lexer.GetToken();
}

Stroka ChopStringToken(const TStringBuf& data, TStringBuf* suffix)
{
    TLexer lexer;
    DoChopToken(lexer, data, suffix);
    const auto& token = lexer.GetToken();
    if (token.GetType() != ETokenType::String) {
        ythrow yexception() << Sprintf("Expected String token, but token %s of type %s found",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
    }
    return token.GetStringValue();
}

i64 ChopInt64Token(const TStringBuf& data, TStringBuf* suffix)
{
    TLexer lexer;
    DoChopToken(lexer, data, suffix);
    const auto& token = lexer.GetToken();
    if (token.GetType() != ETokenType::Int64) {
        ythrow yexception() << Sprintf("Expected Int64 token, but token %s of type %s found",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
    }
    return token.GetInt64Value();
}

double ChopDoubleToken(const TStringBuf& data, TStringBuf* suffix)
{
    TLexer lexer;
    DoChopToken(lexer, data, suffix);
    const auto& token = lexer.GetToken();
    if (token.GetType() != ETokenType::Double) {
        ythrow yexception() << Sprintf("Expected Double token, but token %s of type %s found",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
    }
    return token.GetDoubleValue();
}

ETokenType ChopSpecialToken(const TStringBuf& data, TStringBuf* suffix)
{
    TLexer lexer;
    DoChopToken(lexer, data, suffix);
    const auto& token = lexer.GetToken();
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
