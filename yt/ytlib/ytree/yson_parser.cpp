#include "stdafx.h"
#include "yson_parser.h"

#include "yson_consumer.h"
#include "lexer.h"

#include <stack>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser::TImpl
{
    DECLARE_ENUM(EState,
        // @ stands for current position
        (None)                  // @ (special value for empty stack)
        (StringEnd)             // "..." @
        (Int64End)              // 123...9 @
        (DoubleEnd)             // 0.123...9 @
        (ListBeforeItem)        // [...; @
        (ListAfterItem)         // [... @
        (ListEnd)               // [...] @
        (MapBeforeKey)          // {...; @
        (MapAfterKey)           // {...; "..." @
        (MapBeforeValue)        // {...; "..." = @
        (MapAfterValue)         // {...; "..." = ... @
        (MapEnd)                // {...} @
        (AttributesBeforeKey)   // <...; @
        (AttributesAfterKey)    // <...; "..." @
        (AttributesBeforeValue) // <...; "..." = @
        (AttributesAfterValue)  // <...; "..." = ... @
        (FragmentParsed)        // ...<...> @
    );

    typedef TLexer::EState ELexerState;

    IYsonConsumer* Consumer;
    bool Fragmented;

    TLexer Lexer;
    std::stack<EState> StateStack;

    Stroka CachedStringValue;
    i64 CachedInt64Value;
    double CachedDoubleValue;

    // Diagnostics info
    int Offset;
    int Line;
    int Position;
    int Fragment;

public:
    TImpl(IYsonConsumer* consumer, bool fragmented)
        : Consumer(consumer)
        , Fragmented(fragmented)
        , CachedInt64Value(0)
        , CachedDoubleValue(0.0)
        , Offset(0)
        , Line(1)
        , Position(1)
        , Fragment(0)
    { }

//    bool IsAwaiting(bool ignoreAttributes) const
//    {
//        if (Lexer.IsAwaiting())
//            return true;

//        if (StateStack.empty())
//            return true;

//        if (!ignoreAttributes)
//            return true;

//        if (StateStack.size() > 1)
//            return true;

//        // StateStack.size() == 1
//        switch (StateStack.top()) {
//            case EState::Parsed:
//            case EState::StringEnd:
//            case EState::Int64End:
//            case EState::DoubleEnd:
//            case EState::ListEnd:
//            case EState::MapEnd:
//                return false;

//            default:
//                return true;
//        }
//    }

    void Consume(char ch)
    {
        bool stop = false;
        while (!stop) {
            try {
                stop = Lexer.Consume(ch);
            } catch (...) {
                ythrow yexception() << Sprintf("Error parsing YSON: Could not read symbol %s (%s):\n%s",
                    ~Stroka(ch).Quote(),
                    ~GetPositionInfo(),
                    ~CurrentExceptionMessage());
            }

            if (Lexer.GetState() != ELexerState::None && Lexer.GetState() != ELexerState::InProgress) {
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
        if (Lexer.GetState() != ELexerState::None) {
            ConsumeLexeme();
            Lexer.Reset();
        }
        YASSERT(Lexer.GetState() == ELexerState::None);

        while (!StateStack.empty() && StateStack.top() != EState::FragmentParsed) {
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

        if (!StateStack.empty()) {
            YASSERT(StateStack.top() == EState::FragmentParsed);
            StateStack.pop();
            YASSERT(StateStack.empty());
        } else if (!Fragmented) {
            ythrow yexception() << Sprintf("Error parsing YSON: Cannot finish parsing, nothing was parsed (%s)",
                ~GetPositionInfo());
        }

        YASSERT(CachedStringValue.empty());
        YASSERT(CachedInt64Value == 0);
        YASSERT(CachedDoubleValue == 0.0);
    }

private:
    void ConsumeLexeme()
    {
        YASSERT(Lexer.GetState() != ELexerState::InProgress);
        bool consumed = false;
        while (!consumed) {
            switch (CurrentState()) {
                case EState::None:
                    ConsumeNew();
                    consumed = true;
                    break;

                case EState::StringEnd:
                case EState::Int64End:
                case EState::DoubleEnd:
                case EState::ListEnd:
                case EState::MapEnd:
                    consumed = ConsumeEnd();
                    break;

                case EState::ListBeforeItem:
                case EState::ListAfterItem:
                    ConsumeList();
                    consumed = true;
                    break;

                case EState::MapBeforeKey:
                case EState::MapAfterKey:
                case EState::MapBeforeValue:
                case EState::MapAfterValue:
                    ConsumeMap();
                    consumed = true;
                    break;

                case EState::AttributesBeforeKey:
                case EState::AttributesAfterKey:
                case EState::AttributesBeforeValue:
                case EState::AttributesAfterValue:
                    ConsumeAttributes();
                    consumed = true;
                    break;

                case EState::FragmentParsed:
                    ConsumeParsed();
                    consumed = true;
                    break;

                default:
                    YUNREACHABLE();
            }
        }
    }

    void ConsumeNew()
    {
        if (Fragmented) {
            Consumer->OnListItem();
        }
        ConsumeAny();
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
                CachedDoubleValue = Lexer.GetDoubleValue();
                StateStack.push(EState::DoubleEnd);
                break;

            case ELexerState::LeftBracket:
                Consumer->OnBeginList();
                StateStack.push(EState::ListBeforeItem);
                break;

            case ELexerState::LeftBrace:
                Consumer->OnBeginMap();
                StateStack.push(EState::MapBeforeKey);
                break;

            case ELexerState::LeftAngle:
                Consumer->OnEntity(true);
                Consumer->OnBeginAttributes();
                StateStack.push(EState::AttributesBeforeKey);
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
            case EState::ListBeforeItem:
                if (lexerState == ELexerState::RightBracket) {
                    StateStack.top() = EState::ListEnd;
                } else {
                    Consumer->OnListItem();
                    ConsumeAny();
                }
                break;

            case EState::ListAfterItem:
                if (lexerState == ELexerState::RightBracket) {
                    StateStack.top() = EState::ListEnd;
                } else if (lexerState == ELexerState::Semicolon) {
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
            case EState::MapBeforeKey:
                if (lexerState == ELexerState::RightBrace) {
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
                if (lexerState == ELexerState::Equals) {
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
                if (lexerState == ELexerState::RightBrace) {
                    StateStack.top() = EState::MapEnd;
                } else if (lexerState == ELexerState::Semicolon) {
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

        if (lexerState == ELexerState::RightAngle &&
            (currentState == EState::AttributesBeforeKey || currentState == EState::AttributesAfterValue))
        {
            Consumer->OnEndAttributes();
            OnItemConsumed();
            return;
        }

        switch (CurrentState()) {
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
                if (lexerState == ELexerState::Equals) {
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
                if (lexerState == ELexerState::Semicolon) {
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

    void ConsumeParsed()
    {
        YASSERT(StateStack.top() == EState::FragmentParsed);

        auto lexerState = Lexer.GetState();
        if (!Fragmented) {
            ythrow yexception() << Sprintf("Error parsing YSON: Document is already parsed, but unexpected lexeme of type %s found (%s)",
                ~lexerState.ToString(),
                ~GetPositionInfo());
        }
        if (lexerState != ELexerState::Semicolon) {
            ythrow yexception() << Sprintf("Error parsing YSON: Expected ';', but lexeme of type %s found (%s)",
                ~lexerState.ToString(),
                ~GetPositionInfo());
        }

        StateStack.pop();
        YASSERT(StateStack.empty());
        ++Fragment;
    }

    bool ConsumeEnd()
    {
        bool attributes = Lexer.GetState() == ELexerState::LeftAngle;
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
            StateStack.top() = EState::AttributesBeforeKey;
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
                StateStack.push(EState::FragmentParsed);
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
        if (Fragmented) {
            return Sprintf("Offset: %d, Line: %d, Position: %d, Fragment: %d",
                Offset,
                Line,
                Position,
                Fragment);
        } else {
            return Sprintf("Offset: %d, Line: %d, Position: %d",
                Offset,
                Line,
                Position);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TYsonParser::TYsonParser(IYsonConsumer *consumer, bool fragmented)
    : Impl(new TImpl(consumer, fragmented))
{ }


TYsonParser::~TYsonParser()
{ }

void TYsonParser::Consume(char ch)
{
    Impl->Consume(ch);
}

void TYsonParser::Finish()
{
    Impl->Finish();
}

////////////////////////////////////////////////////////////////////////////////

void ParseYson(TInputStream* input, IYsonConsumer* consumer, bool fragmented)
{
    TYsonParser parser(consumer, fragmented);
    char ch;
    while (input->ReadChar(ch)) {
        parser.Consume(ch);
    }
    parser.Finish();
}

void ParseYson(const Stroka& string, IYsonConsumer* consumer, bool fragmented)
{
    TStringInput input(string);
    ParseYson(&input, consumer, fragmented);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
