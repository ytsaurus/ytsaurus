#include "stdafx.h"
#include "yson_parser.h"

#include "yson_format.h"
#include "yson_consumer.h"

#include <util/string/escape.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TYsonLexer
{
public:
    DECLARE_ENUM(EState,
        (Start)
        (InsideBinaryInt64)
        (InsideBinaryDouble)
        (InsideBinaryString)
        (InsideUnquotedString)
        (InsideQuotedString)
        (InsideNumeric)
        (InsideDouble)

        // Terminal states:
        (ItemSeparator)
        (KeyValueSeparator)
        (ListStart)
        (ListEnd)
        (MapStart)
        (MapEnd)
        (AttributesStart)
        (AttributesEnd)
        (String)
        (Int64)
        (Double)
    );

    TYsonLexer()
    {
        Reset();
    }

    void Reset()
    {
        State_ = EState::Start;
        StringValue = Stroka();
        Int64Value = 0;
        DoubleValue = 0;
        BytesToRead = 0;
        BytesRead = 0;
    }

    DEFINE_BYVAL_RO_PROPERTY(EState, State)

    bool InTerminalState() const
    {
        return State_ >= EState::ItemSeparator;
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
    
    //! Returns true iff the character was read.
    bool Read(char ch)
    {
        switch (State_) {
            case EState::Start:
                ReadStart(ch);
                return true;
            
            case EState::InsideUnquotedString:
                return ReadUnquotedString(ch);

            case EState::InsideQuotedString:
                ReadQuotedString(ch);
                return true;

            case EState::InsideBinaryString:
                ReadBinaryString(ch);
                return true;

            case EState::InsideNumeric:
                return ReadNumeric(ch);

            case EState::InsideDouble:
                return ReadDouble(ch);

            case EState::InsideBinaryInt64:
                ReadBinaryInt64(ch);
                return true;

            case EState::InsideBinaryDouble:
                ReadBinaryDouble(ch);
                return true;

            default:
                YUNREACHABLE();
        }
    }

    void Finish()
    {
        switch (State_) {
            case EState::Start:
                break;

            case EState::InsideBinaryInt64:
            case EState::InsideBinaryDouble:
            case EState::InsideBinaryString:
            case EState::InsideQuotedString:
                ythrow yexception() << Sprintf("Premature end of stream (LexerState: %s)",
                    ~State_.ToString());

            case EState::InsideUnquotedString:
                FinishString();
                break;

            case EState::InsideNumeric:
                FinishNumeric();
                break;

            case EState::InsideDouble:
                FinishDouble();
                break;

            default:
                YUNREACHABLE();
        }
    }

    bool IsAwaiting() const
    {
        return State_ != EState::Start;
    }

private:

    void ReadStart(char ch)
    {
        if (isspace(ch))
            return;

        switch (ch) {
            case ItemSeparator:
                State_ = EState::ItemSeparator;
                break;;

            case KeyValueSeparator:
                State_ = EState::KeyValueSeparator;
                break;

            case BeginListSymbol:
                State_ = EState::ListStart;
                break;

            case EndListSymbol:
                State_ = EState::ListEnd;
                break;

            case BeginMapSymbol:
                State_ = EState::MapStart;
                break;

            case EndMapSymbol:
                State_ = EState::MapEnd;
                break;

            case BeginAttributesSymbol:
                State_ = EState::AttributesStart;
                break;

            case EndAttributesSymbol:
                State_ = EState::AttributesEnd;
                break;

            case StringMarker:
                State_ = EState::InsideBinaryString;
                YASSERT(BytesToRead == 0);
                break;

            case Int64Marker:
                State_ = EState::InsideBinaryInt64;
                YASSERT(Int64Value == 0);
                break;

            case DoubleMarker:
                State_ = EState::InsideBinaryDouble;
                BytesToRead = static_cast<int>(sizeof(double));
                YASSERT(DoubleValue == 0.0);
                break;

            case '"':
                State_ = EState::InsideQuotedString;
                YASSERT(StringValue.empty());
                break;

            default:
                if (isspace(ch)) {
                    break;;
                } else if (isdigit(ch) || ch == '+' || ch == '-') {
                    StringValue = Stroka(ch);
                    State_ = EState::InsideNumeric;
                } else if (isalpha(ch) || ch == '_') {
                    StringValue = Stroka(ch);
                    State_ = EState::InsideUnquotedString;
                } else {
                    ythrow yexception() << Sprintf("Unexpected character %s",
                        ~Stroka(ch).Quote());
                }
                break;
        }
    }

    bool ReadUnquotedString(char ch)
    {
        if (isalpha(ch) || isdigit(ch) || ch == '_') {
            StringValue.append(ch);
            return true;
        } else {
            FinishString();
            return false;
        }
    }

    void ReadQuotedString(char ch)
    {
        bool finish = false;
        if (ch != '"') {
            StringValue.append(ch);
        } else {
            // We must count the count of '\' at the end of StringValue
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

    void ReadBinaryInt64(char ch) {
        if (7 * BytesRead > 8 * sizeof(ui64) ) {
            ythrow yexception() << Sprintf("The data is too long to read Int64");
        }
        Int64Value |= (static_cast<ui64> (ch & 0x7F)) << (7 * BytesRead);
        ++BytesRead;

        if (ch & 0x80 == 0) {
            Int64Value = (static_cast<ui64>(Int64Value) >> 1) ^ -static_cast<i64>(Int64Value & 1);
            State_ = EState::Int64;
            BytesRead = 0;
        }
    }

    void ReadBinaryString(char ch)
    {
        if (BytesToRead == 0) {
            ReadBinaryInt64(ch);
            if (State_ == EState::Int64) {
                BytesToRead = Int64Value;
                Int64Value = 0;
                State_ = EState::InsideBinaryString;
            }
        } else {
            StringValue.append(ch);
            --BytesToRead;

            if (BytesToRead == 0) {
                FinishString();
            }
        }
    }

    bool ReadNumeric(char ch)
    {
        if (isdigit(ch) || ch == '+' || ch == '-') { // Seems like it can't be '+' or '-'
            StringValue.append(ch);
            return true;
        } else if (ch == '.' || ch == 'e' || ch == 'E') {
            StringValue.append(ch);
            State_ = EState::InsideDouble;
            return true;
        } else {
            FinishNumeric();
            return false;
        }
    }

    bool ReadDouble(char ch)
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

    void ReadBinaryDouble(char ch)
    {
        // If that doesn't work replace (BytesToRead - 1) with (8 - BytesToRead)
        *(reinterpret_cast<ui64*>(&DoubleValue)) |= static_cast<ui64>(ch) << (8 * (BytesToRead - 1));
        --BytesToRead;

        if (BytesToRead == 0) {
            State_ = EState::Double;
        }
    }

    void FinishString()
    {
        State_ = EState::String;
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
        State_ = EState::Int64;
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
        State_ = EState::Double;
    }

    Stroka StringValue;
    i64 Int64Value;
    double DoubleValue;
    int BytesToRead; // For binary strings and doubles
    int BytesRead; // For varints
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TYsonParser::TImpl
{
    DECLARE_ENUM(EState,
        // @ stands for current position
        (None)                  // @ (special value for empty stack)
        (StringEnd)             // "..." @
        (Int64End)              // 123...9 @
        (DoubleEnd)             // 1.23...9 @
        (ListStart)             // [ @
        (ListBeforeItem)        // [...; @
        (ListAfterItem)         // [... @
        (ListEnd)               // [...] @
        (MapStart)              // { @
        (MapBeforeKey)          // {...; @
        (MapAfterKey)           // {...; "..." @
        (MapBeforeValue)        // {...; "..." = @
        (MapAfterValue)         // {...; "..." = ... @
        (MapEnd)                // {...} @
        (AttributesStart)       // < @
        (AttributesBeforeKey)   // <...; @
        (AttributesAfterKey)    // <...; "..." @
        (AttributesBeforeValue) // <...; "..." = @
        (AttributesAfterValue)  // <...; "..." = ... @
    );

    typedef TYsonLexer::EState ELexerState;

    IYsonConsumer* Consumer;

    TYsonLexer Lexer;
    std::stack<EState> StateStack;

    Stroka CachedStringValue;
    i64 CachedInt64Value;
    double CachedDoubleValue;

    // Diagnostics info
    int Offset;
    int Line;
    int Position;

public:
    TImpl(IYsonConsumer* consumer)
        : Consumer(consumer)
        , CachedInt64Value(0)
        , CachedDoubleValue(0.0)
        , Offset(0)
        , Line(1)
        , Position(1)
    { }

    bool IsAwaiting(bool ignoreAttributes) const
    {
        if (Lexer.IsAwaiting())
            return true;

        if (StateStack.empty())
            return false;

        if (!ignoreAttributes)
            return true;

        if (StateStack.size() > 1)
            return true;

        // StateStack.size() == 1
        switch (StateStack.top()) {
            case EState::StringEnd:
            case EState::Int64End:
            case EState::DoubleEnd:
            case EState::ListEnd:
            case EState::MapEnd:
                return false;

            default:
                return true;
        }
    }

    void Consume(char ch)
    {
        bool stop = false;
        while (!stop) {
            try {
                stop = Lexer.Read(ch);
            } catch (...) {
                ythrow yexception() << Sprintf("Error parsing YSON: Could not read symbol %s (%s):\n%s",
                    ~Stroka(ch).Quote(),
                    ~GetPositionInfo(),
                    ~CurrentExceptionMessage());
            }

            if (Lexer.InTerminalState()) {
                ConsumeLexeme();
                Lexer.Reset();
            }
        }

        ++Offset;
        ++Position;
        if (ch == '\n') {
            ++Line;
            Position = 1;
        }
    }

    void Finish()
    {
        Lexer.Finish();
        if (Lexer.InTerminalState()) {
            ConsumeLexeme();
            Lexer.Reset();
        }
        YASSERT(Lexer.GetState() == ELexerState::Start);
        
        while (!StateStack.empty()) {
            switch (CurrentState()) {
                case EState::StringEnd:
                case EState::Int64End:
                case EState::DoubleEnd:
                case EState::ListEnd:
                case EState::MapEnd:
                    YVERIFY(!ConsumeEnd());
                    break;

                default:
                    ythrow yexception() << Sprintf("Error parsing YSON: Cannot finish parsing in state %s (%s)",
                        ~CurrentState().ToString(),
                        ~GetPositionInfo());
            }
        }

        YASSERT(CachedStringValue.empty());
        YASSERT(CachedInt64Value == 0);
        YASSERT(CachedDoubleValue == 0.0);
    }

private:
    void ConsumeLexeme()
    {
        bool consumed = false;
        while (!consumed) {
            switch (CurrentState()) {
                case EState::None:
                    ConsumeAny();
                    consumed = true;
                    break;

                case EState::StringEnd:
                case EState::Int64End:
                case EState::DoubleEnd:
                case EState::ListEnd:
                case EState::MapEnd:
                    consumed = ConsumeEnd();
                    break;

                case EState::ListStart:
                case EState::ListBeforeItem:
                case EState::ListAfterItem:
                    ConsumeList();
                    consumed = true;
                    break;

                case EState::MapStart:
                case EState::MapBeforeKey:
                case EState::MapAfterKey:
                case EState::MapBeforeValue:
                case EState::MapAfterValue:
                    ConsumeMap();
                    consumed = true;
                    break;

                case EState::AttributesStart:
                case EState::AttributesBeforeKey:
                case EState::AttributesAfterKey:
                case EState::AttributesBeforeValue:
                case EState::AttributesAfterValue:
                    ConsumeAttributes();
                    consumed = true;
                    break;

                default:
                    YUNREACHABLE();
            }
        }
    }

    void ConsumeAny()
    {
        switch (Lexer.GetState()) {
            case ELexerState::String:
                CachedStringValue = Lexer.GetStringValue();
                StateStack.push(EState::StringEnd);
                break;

            case ELexerState::Int64:
                CachedInt64Value = Lexer.GetInt64Value();
                StateStack.push(EState::Int64End);
                break;

            case ELexerState::Double:
                CachedInt64Value = Lexer.GetDoubleValue();
                StateStack.push(EState::DoubleEnd);
                break;

            case ELexerState::ListStart:
                Consumer->OnBeginList();
                StateStack.push(EState::ListStart);
                break;

            case ELexerState::MapStart:
                Consumer->OnBeginMap();
                StateStack.push(EState::MapStart);
                break;

            case ELexerState::AttributesStart:
                Consumer->OnEntity(true);
                Consumer->OnBeginAttributes();
                StateStack.push(EState::AttributesStart);
                break;

            default:
                ythrow yexception() << Sprintf("Error parsing YSON: Unexpected lexeme of type %s (%s)",
                    ~Lexer.GetState().ToString(),
                    ~GetPositionInfo());
        }
    }

    void ConsumeList()
    {
        auto lexerState = Lexer.GetState();
        switch (CurrentState()) {
            case EState::ListStart:
            case EState::ListBeforeItem:
                if (lexerState == ELexerState::ListEnd && CurrentState() == EState::ListStart) {
                    StateStack.top() = EState::ListEnd;
                } else {
                    Consumer->OnListItem();
                    ConsumeAny();
                }
                break;

            case EState::ListAfterItem:
                if (lexerState == ELexerState::ListEnd) {
                    StateStack.top() = EState::ListEnd;
                } else if (lexerState == ELexerState::ItemSeparator) {
                    StateStack.top() = EState::ListBeforeItem;
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected ';' or ']', but lexeme of type %s found (%s)",
                        ~lexerState.ToString(),
                        ~GetPositionInfo());
                }
                break;

            default:
                YUNREACHABLE();
        }
    }

    void ConsumeMap()
    {
        auto lexerState = Lexer.GetState();
        switch (CurrentState()) {
            case EState::MapStart:
            case EState::MapBeforeKey:
                if (lexerState == ELexerState::MapEnd && CurrentState() == EState::MapStart) {
                    StateStack.top() = EState::MapEnd;
                } else if (lexerState == ELexerState::String) {
                    Consumer->OnMapItem(Lexer.GetStringValue());
                    StateStack.top() = EState::MapAfterKey;  
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected string literal, but lexeme of type %s found (%s)",
                        ~lexerState.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::MapAfterKey:
                if (lexerState == ELexerState::KeyValueSeparator) {
                    StateStack.top() = EState::MapBeforeValue;
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected '=', but lexeme of type %s found (%s)",
                        ~lexerState.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::MapBeforeValue:
                ConsumeAny();
                break;

            case EState::MapAfterValue:
                if (lexerState == ELexerState::MapEnd) {
                    StateStack.top() = EState::MapEnd;
                } else if (lexerState == ELexerState::ItemSeparator) {
                    StateStack.top() = EState::MapBeforeKey;
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected ';' or '}', but lexeme of type %s found (%s)",
                        ~lexerState.ToString(),
                        ~GetPositionInfo());
                }
                break;

            default:
                YUNREACHABLE();
        }
    }

    void ConsumeAttributes()
    {
        auto lexerState = Lexer.GetState();
        auto currentState = CurrentState();

        if (lexerState == ELexerState::AttributesEnd &&
            (currentState == EState::AttributesStart || currentState == EState::AttributesAfterValue))
        {
            Consumer->OnEndAttributes();
            OnItemConsumed();
            return;
        }

        switch (CurrentState()) {
            case EState::AttributesStart:
            case EState::AttributesBeforeKey:
                if (lexerState == ELexerState::String) {
                    Consumer->OnAttributesItem(Lexer.GetStringValue());
                    StateStack.top() = EState::AttributesAfterKey;  
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected string literal, but lexeme of type %s found (%s)",
                        ~lexerState.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::AttributesAfterKey:
                if (lexerState == ELexerState::KeyValueSeparator) {
                    StateStack.top() = EState::AttributesBeforeValue;
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected '=', but lexeme of type %s found (%s)",
                        ~lexerState.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::AttributesBeforeValue:
                ConsumeAny();
                break;

            case EState::AttributesAfterValue:
                if (lexerState == ELexerState::ItemSeparator) {
                    StateStack.top() = EState::AttributesBeforeKey;
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected ';' or '>', but lexeme of type %s found (%s)",
                        ~lexerState.ToString(),
                        ~GetPositionInfo());
                }
                break;

            default:
                YUNREACHABLE();
        }
    }

    bool ConsumeEnd()
    {
        bool attributes = Lexer.GetState() == ELexerState::AttributesStart;
        switch (CurrentState()) {
            case EState::StringEnd:
                Consumer->OnStringScalar(CachedStringValue, attributes);
                CachedStringValue = Stroka();
                break;

            case EState::Int64End:
                Consumer->OnInt64Scalar(CachedInt64Value, attributes);
                CachedInt64Value = 0;
                break;

            case EState::DoubleEnd:
                Consumer->OnDoubleScalar(CachedDoubleValue, attributes);
                CachedDoubleValue = 0.0;
                break;

            case EState::ListEnd:
                Consumer->OnEndList(attributes);
                break;

            case EState::MapEnd:
                Consumer->OnEndMap(attributes);
                break;

            default:
                YUNREACHABLE();
        }

        if (attributes) {
            Consumer->OnBeginAttributes();
            StateStack.top() = EState::AttributesStart;
            return true;
        } else {
            OnItemConsumed();
            return false;
        }
    }

    void OnItemConsumed()
    {
        StateStack.pop();
        switch (CurrentState()) {
            case EState::None:
                break;

            case EState::ListBeforeItem:
                StateStack.top() = EState::ListAfterItem;
                break;

            case EState::MapBeforeValue:
                StateStack.top() = EState::MapAfterValue;
                break;

            case EState::AttributesBeforeValue:
                StateStack.top() = EState::AttributesAfterValue;
                break;
            
            default:
                YUNREACHABLE();
        }
    }


    EState CurrentState() const
    {
        if (StateStack.empty())
            return EState::None;
        return StateStack.top();
    }

    Stroka GetPositionInfo() const
    {
        return Sprintf("Offset: %d, Line: %d, Position: %d",
            Offset,
            Line,
            Position);
    }
};

////////////////////////////////////////////////////////////////////////////////

TYsonParser::TYsonParser(IYsonConsumer *consumer)
    : Impl(new TImpl(consumer))
{ }

void TYsonParser::Consume(char ch)
{
    Impl->Consume(ch);
}

void TYsonParser::Finish()
{
    Impl->Finish();
}

bool TYsonParser::IsAwaiting(bool ignoreAttributes) const
{
    return Impl->IsAwaiting(ignoreAttributes);
}

////////////////////////////////////////////////////////////////////////////////

void ParseYson(TInputStream* input, IYsonConsumer* consumer)
{
    TYsonParser parser(consumer);
    char ch;
    while (input->ReadChar(ch)) {
        parser.Consume(ch);
    }
    parser.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
