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
        StringValue.reserve(StringBufferSize);
    }

    void Reset()
    {
        State_ = EState::None;
        InnerState = EInnerState::None;
        Token = TToken();
        StringValue.clear();
        IntegerValue = 0;
        DoubleValue = 0.0;
        BytesRead = 0;
    }

    const TToken& GetToken() const
    {
        YASSERT(State_ == EState::Terminal);
        return Token;
    }
    
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

    size_t Consume(const TStringBuf& data)
    {
        const auto begin = data.begin();
        const auto end = data.end();
        auto current = begin;
        while (current != end) {
            // NB: Conditions order is optimized for quick rejection.
            if (InnerState == EInnerState::InsideBinaryString &&
                State_ == EState::InProgress &&
                BytesRead < 0 )
            {
                // Optimized version for binary string literals.
                int bytesRemaining = static_cast<int>(end - current);
                int bytesNeeded = -BytesRead;
                if (bytesRemaining < bytesNeeded) {
                    StringValue.append(current, end);
                    current = end;
                    BytesRead += bytesRemaining;
                } else {
                    StringValue.append(current, current + bytesNeeded);
                    current += bytesNeeded;
                    // NB: Setting BytesRead to zero is redundant.
                    FinishString();
                    YASSERT(State_ == EState::Terminal);
                }
            } else {
                // Fallback to the usual, symbol-by-symbol version.
                if (Consume(*current)) {
                    ++current;
                }
                if (State_ == EState::Terminal) {
                    break;
                }
            }
        }
        return current - begin;
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
    static const int StringBufferSize = 1 << 16;

    void ConsumeStart(char ch)
    {
        if (isspace(ch))
            return;

        switch (ch) {
            case '"':
                SetInProgressState(EInnerState::InsideQuotedString);
                YASSERT(StringValue.empty());
                YASSERT(BytesRead == 0);
                break;

            case '\x01':
                SetInProgressState(EInnerState::InsideBinaryString);
                YASSERT(StringValue.empty());
                YASSERT(BytesRead == 0);
                break;

            case '\x02':
                SetInProgressState(EInnerState::InsideBinaryInteger);
                YASSERT(IntegerValue == 0);
                YASSERT(BytesRead == 0);
                break;

            case '\x03':
                SetInProgressState(EInnerState::InsideBinaryDouble);
                YASSERT(DoubleValue == 0.0);
                YASSERT(BytesRead == 0);
                BytesRead = -static_cast<int>(sizeof(double));
                break;

            case '+':
                SetInProgressState(EInnerState::AfterPlus);
                break;


            default: {
                auto specialTokenType = CharToTokenType(ch);
                if (specialTokenType != ETokenType::None) {
                    ProduceToken(specialTokenType);
                } else if (isdigit(ch) || ch == '-') { // case of '+' is handled in AfterPlus state
                    StringValue.append(ch);
                    SetInProgressState(EInnerState::InsideNumeric);
                } else if (isalpha(ch) || ch == '_' || ch == '%') {
                    StringValue.append(ch);
                    SetInProgressState(EInnerState::InsideUnquotedString);
                } else {
                    ythrow yexception() << Sprintf("Unexpected character %s",
                        ~Stroka(ch).Quote());
                }
                break;
            }
        }
    }

    bool ConsumeUnquotedString(char ch)
    {
        if (isalpha(ch) || isdigit(ch) || ch == '_' || ch == '-' || ch == '%') {
            StringValue.append(ch);
            return true;
        } else {
            FinishString();
            return false;
        }
    }

    void ConsumeQuotedString(char ch)
    {
        bool finish = false;
        if (ch != '"') {
            StringValue.append(ch);
        } else {
            // We must count the number of '\' at the end of StringValue
            // to check if it's not \"
            int slashCount = 0;
            int length = StringValue.length();
            while (slashCount < length && StringValue[length - 1 - slashCount] == '\\')
                ++slashCount;
            if (slashCount % 2 == 0) {
                finish = true;
            } else {
                StringValue.append(ch);
            }
        }

        if (finish) {
            StringValue = UnescapeC(StringValue); // ! Creating new Stroka here!
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

        ui64 ui64Value = static_cast<ui64>(IntegerValue);
        ui64Value |= (static_cast<ui64> (byte & 0x7F)) << (7 * BytesRead);
        ++BytesRead;

        if ((byte & 0x80) == 0) {
            IntegerValue = ZigZagDecode64(static_cast<ui64>(ui64Value));
            ProduceToken(ETokenType::Integer);
            BytesRead = 0;
        } else {
            IntegerValue = static_cast<i64>(ui64Value);
        }
    }

    void ConsumeBinaryString(char ch)
    {
        if (BytesRead < 0) {
            StringValue.append(ch);
            ++BytesRead;
        } else { // We are reading length
            ConsumeBinaryInteger(ch);

            if (State_ == EState::Terminal) {
                i64 length = IntegerValue;
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
            StringValue.append(ch);
            return true;
        } else if (ch == '.' || ch == 'e' || ch == 'E') {
            StringValue.append(ch);
            InnerState = EInnerState::InsideDouble;
            return true;
        } else if (isalpha(ch)) {
            ythrow yexception() << Sprintf("Unexpected character in numeric (Char: %s, Token: %s)",
                ~Stroka(ch).Quote(),
                ~StringValue);
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
            StringValue.append(ch);
            return true;
        } else if (isalpha(ch)) {
            ythrow yexception() << Sprintf("Unexpected character in numeric (Char: %s, Token: %s)",
                ~Stroka(ch).Quote(),
                ~StringValue);
        } else {
            FinishDouble();
            return false;
        }
    }

    void ConsumeBinaryDouble(char ch)
    {
        ui8 byte = static_cast<ui8>(ch);

        *(reinterpret_cast<ui64*>(&DoubleValue)) |=
            static_cast<ui64>(byte) << (8 * (8 + BytesRead));
        ++BytesRead;

        if (BytesRead == 0) {
            ProduceToken(ETokenType::Double);
        }
    }

    bool ConsumePlus(char ch)
    {
    	if (!isdigit(ch)) {
            ProduceToken(ETokenType::Plus);
    		return false;
    	}

    	Reset();
        StringValue.append('+');
        StringValue.append(ch);
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
            IntegerValue = FromString<i64>(StringValue);
        } catch (const std::exception& ex) {
            // This exception is wrapped in parser
            ythrow yexception() << Sprintf("Failed to parse Integer literal %s",
                ~StringValue.Quote());
        }
        ProduceToken(ETokenType::Integer);
    }

    void FinishDouble()
    {
        try {
            DoubleValue = FromString<double>(StringValue);
        } catch (const std::exception& ex) {
            // This exception is wrapped in parser
            ythrow yexception() << Sprintf("Failed to parse Double literal %s",
                ~StringValue.Quote());
        }
        ProduceToken(ETokenType::Double);
    }

    void FinishPlus()
    {
        ProduceToken(ETokenType::Plus);
    }

    void ProduceToken(ETokenType type)
    {
        YASSERT(State_ == EState::None || State_ == EState::InProgress);
        State_ = EState::Terminal;
        InnerState = EInnerState::None;
        switch (type) {
            case ETokenType::String:
                Token = TToken(StringValue);
                break;
            case ETokenType::Integer:
                Token = TToken(IntegerValue);
                break;
            case ETokenType::Double:
                Token = TToken(DoubleValue);
                break;
            default:
                Token = TToken(type);
                break;
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
    Stroka StringValue;
    i64 IntegerValue;
    double DoubleValue;

    /*
     * BytesRead > 0 means we've read BytesRead bytes (in binary integers)
     * BytesRead < 0 means we are expecting -BytesRead bytes more (in binary doubles and strings)
     * BytesRead = 0 also means we don't know the number of bytes yet
     */
    int BytesRead;
};

////////////////////////////////////////////////////////////////////////////////

TLexer::TLexer()
    : Impl(new TImpl())
{ }

TLexer::~TLexer()
{ }

//bool TLexer::Consume(char ch)
//{
//    return Impl->Consume(ch);
//}

size_t TLexer::Consume(const TStringBuf& data)
{
    return Impl->Consume(data);
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

} // namespace NYTree
} // namespace NYT
