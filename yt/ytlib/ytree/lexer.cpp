#include "stdafx.h"
#include "lexer.h"

#include "yson_format.h"

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
        StringValue = Stroka();
        Int64Value = 0;
        DoubleValue = 0;
        BytesToRead = 0;
        BytesRead = 0;
    }

    const Stroka& GetStringValue() const
    {
        YASSERT(State_ == EState::String);
        return StringValue;
    }

    i64 GetInt64Value() const
    {
        YASSERT(State_ == EState::Int64);
        return Int64Value;
    }

    double GetDoubleValue() const
    {
        YASSERT(State_ == EState::Double);
        return DoubleValue;
    }

    const Stroka& GetSpecialValue() const
    {
        YASSERT(State_ >= EState::Semicolon);
        YASSERT(!StringValue.empty());
        return StringValue;
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
                switch (State_) {
                    case EInnerState::InsideBinaryInt64:
                    case EInnerState::InsideBinaryDouble:
                    case EInnerState::InsideBinaryString:
                    case EInnerState::InsideQuotedString:
                        // TODO: add BytesRead/BytesToRead
                        ythrow yexception() << Sprintf("Premature end of stream (LexerState: %s)",
                            ~State_.ToString());

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
                ProduceState(EState::Semicolon, Stroka(ch));
                break;;

            case '=':
                ProduceState(EState::Equals, Stroka(ch));
                break;

            case '[':
                ProduceState(EState::LeftBracket, Stroka(ch));
                break;

            case ']':
                ProduceState(EState::RightBracket, Stroka(ch));
                break;

            case '{':
                ProduceState(EState::LeftBrace, Stroka(ch));
                break;

            case '}':
                ProduceState(EState::RightBrace, Stroka(ch));
                break;

            case '<':
                ProduceState(EState::LeftAngle, Stroka(ch));
                break;

            case '>':
                ProduceState(EState::RightAngle, Stroka(ch));
                break;

            case '\x01':
                SetInProgressState(EInnerState::InsideBinaryString);
                YASSERT(BytesToRead == 0);
                break;

            case '\x02':
                SetInProgressState(EInnerState::InsideBinaryInt64);
                YASSERT(Int64Value == 0);
                break;

            case '\x03':
                SetInProgressState(EInnerState::InsideBinaryDouble);
                BytesToRead = static_cast<int>(sizeof(double));
                YASSERT(DoubleValue == 0.0);
                break;

            case '"':
                SetInProgressState(EInnerState::InsideQuotedString);
                YASSERT(StringValue.empty());
                break;

            default:
                if (isspace(ch)) {
                    break;
                } else if (isdigit(ch) || ch == '+' || ch == '-') {
                    StringValue = Stroka(ch);
                    SetInProgressState(EInnerState::InsideNumeric);
                } else if (isalpha(ch) || ch == '_') {
                    StringValue = Stroka(ch);
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
            StringValue = UnescapeC(StringValue);
            FinishString();
        }
    }

    void ConsumeBinaryInt64(char ch) {
        ui8 byte = static_cast<ui8>(ch);

        if (7 * BytesRead > 8 * sizeof(ui64) ) {
            ythrow yexception() << Sprintf("The data is too long to read binary Int64");
        }

        ui64 ui64Value = static_cast<ui64>(Int64Value);
        ui64Value |= (static_cast<ui64> (byte & 0x7F)) << (7 * BytesRead);
        ++BytesRead;

        if ((byte & 0x80) == 0) {
            Int64Value = ZigZagDecode64(static_cast<ui64>(ui64Value));
            ProduceState(EState::Int64);
            BytesRead = 0;
        } else {
            Int64Value = static_cast<i64>(ui64Value);
        }
    }

    void ConsumeBinaryString(char ch)
    {
        if (BytesToRead == 0) {
            ConsumeBinaryInt64(ch);
            if (State_ == EState::Int64) {
                BytesToRead = Int64Value;
                Int64Value = 0;
                State_ = EState::None; // SetInProgressState asserts it
                SetInProgressState(EInnerState::InsideBinaryString);
            }
        } else {
            StringValue.append(ch);
            --BytesToRead;

            if (BytesToRead == 0) {
                FinishString();
            }
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
        } else {
            FinishDouble();
            return false;
        }
    }

    void ConsumeBinaryDouble(char ch)
    {
        ui8 byte = static_cast<ui8>(ch);

        *(reinterpret_cast<ui64*>(&DoubleValue)) |=
            static_cast<ui64>(byte) << (8 * (8 - BytesToRead));
        --BytesToRead;

        if (BytesToRead == 0) {
            ProduceState(EState::Double);
        }
    }

    void FinishString()
    {
        ProduceState(EState::String);
    }

    void FinishNumeric()
    {
        try {
            Int64Value = FromString<i64>(StringValue);
        } catch (const std::exception& ex) {
            // This exception is wrapped in parser
            ythrow yexception() << Sprintf("Failed to parse Int64 literal %s",
                ~StringValue.Quote());
        }
        ProduceState(EState::Int64);
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
        ProduceState(EState::Double);
    }

    void ProduceState(EState state, Stroka stringValue = Stroka())
    {
        YASSERT(State_ == EState::None || State_ == EState::InProgress);
        State_ = state;
        InnerState = EInnerState::None;
        if (!stringValue.empty()) {
            StringValue = stringValue;
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

    Stroka StringValue;
    i64 Int64Value;
    double DoubleValue;
    int BytesToRead; // For binary strings and doubles
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

const Stroka& TLexer::GetStringValue() const
{
    return Impl->GetStringValue();
}

i64 TLexer::GetInt64Value() const
{
    return Impl->GetInt64Value();
}

double TLexer::GetDoubleValue() const
{
    return Impl->GetDoubleValue();
}

const Stroka& TLexer::GetSpecialValue() const
{
    return Impl->GetSpecialValue();
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
