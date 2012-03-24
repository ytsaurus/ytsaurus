#include "stdafx.h"
#include "yson_parser.h"

#include "yson_consumer.h"
#include "token.h"
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

            if (Lexer.GetState() == TLexer::EState::Terminal) {
                ConsumeToken(Lexer.GetToken());
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
        if (Lexer.GetState() == TLexer::EState::Terminal) {
            ConsumeToken(Lexer.GetToken());
            Lexer.Reset();
        }
        YASSERT(Lexer.GetState() == TLexer::EState::None);

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
    void ConsumeToken(const TToken& token)
    {
        YASSERT(Lexer.GetState() == TLexer::EState::Terminal);
        bool consumed = false;
        while (!consumed) {
            switch (CurrentState()) {
                case EState::None:
                    ConsumeNew(token);
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
                    ConsumeList(token);
                    consumed = true;
                    break;

                case EState::MapBeforeKey:
                case EState::MapAfterKey:
                case EState::MapBeforeValue:
                case EState::MapAfterValue:
                    ConsumeMap(token);
                    consumed = true;
                    break;

                case EState::AttributesBeforeKey:
                case EState::AttributesAfterKey:
                case EState::AttributesBeforeValue:
                case EState::AttributesAfterValue:
                    ConsumeAttributes(token);
                    consumed = true;
                    break;

                case EState::FragmentParsed:
                    ConsumeParsed(token);
                    consumed = true;
                    break;

                default:
                    YUNREACHABLE();
            }
        }
    }

    void ConsumeNew(const TToken& token)
    {
        if (Fragmented) {
            Consumer->OnListItem();
        }
        ConsumeAny(token);
    }

    void ConsumeAny(const TToken& token)
    {
        switch (token.GetType()) {
            case ETokenType::String:
                CachedStringValue = token.GetStringValue();
                StateStack.push(EState::StringEnd);
                break;

            case ETokenType::Int64:
                CachedInt64Value = token.GetInt64Value();
                StateStack.push(EState::Int64End);
                break;

            case ETokenType::Double:
                CachedDoubleValue = token.GetDoubleValue();
                StateStack.push(EState::DoubleEnd);
                break;

            case ETokenType::LeftBracket:
                Consumer->OnBeginList();
                StateStack.push(EState::ListBeforeItem);
                break;

            case ETokenType::LeftBrace:
                Consumer->OnBeginMap();
                StateStack.push(EState::MapBeforeKey);
                break;

            case ETokenType::LeftAngle:
                Consumer->OnEntity(true);
                Consumer->OnBeginAttributes();
                StateStack.push(EState::AttributesBeforeKey);
                break;

            default:
                ythrow yexception() << Sprintf("Error parsing YSON: Unexpected lexeme %s of type %s (%s)",
                    ~token.ToString().Quote(),
                    ~token.GetType().ToString(),
                    ~GetPositionInfo());
        }
    }

    void ConsumeList(const TToken& token)
    {
        auto tokenType = token.GetType();
        switch (CurrentState()) {
            case EState::ListBeforeItem:
                if (tokenType == ETokenType::RightBracket) {
                    StateStack.top() = EState::ListEnd;
                } else {
                    Consumer->OnListItem();
                    ConsumeAny(token);
                }
                break;

            case EState::ListAfterItem:
                if (tokenType == ETokenType::RightBracket) {
                    StateStack.top() = EState::ListEnd;
                } else if (tokenType == ETokenType::Semicolon) {
                    StateStack.top() = EState::ListBeforeItem;
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected ';' or ']', but lexeme %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            default:
                YUNREACHABLE();
        }
    }

    void ConsumeMap(const TToken& token)
    {
        auto tokenType = token.GetType();
        switch (CurrentState()) {
            case EState::MapBeforeKey:
                if (tokenType == ETokenType::RightBrace) {
                    StateStack.top() = EState::MapEnd;
                } else if (tokenType == ETokenType::String) {
                    Consumer->OnMapItem(token.GetStringValue());
                    StateStack.top() = EState::MapAfterKey;  
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected string literal, but lexeme %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::MapAfterKey:
                if (tokenType == ETokenType::Equals) {
                    StateStack.top() = EState::MapBeforeValue;
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected '=', but lexeme %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::MapBeforeValue:
                ConsumeAny(token);
                break;

            case EState::MapAfterValue:
                if (tokenType == ETokenType::RightBrace) {
                    StateStack.top() = EState::MapEnd;
                } else if (tokenType == ETokenType::Semicolon) {
                    StateStack.top() = EState::MapBeforeKey;
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected ';' or '}', but lexeme %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            default:
                YUNREACHABLE();
        }
    }

    void ConsumeAttributes(const TToken& token)
    {
        auto tokenType = token.GetType();
        auto currentState = CurrentState();

        if (tokenType == ETokenType::RightAngle &&
            (currentState == EState::AttributesBeforeKey || currentState == EState::AttributesAfterValue))
        {
            Consumer->OnEndAttributes();
            OnItemConsumed();
            return;
        }

        switch (CurrentState()) {
            case EState::AttributesBeforeKey:
                if (tokenType == ETokenType::String) {
                    Consumer->OnAttributesItem(token.GetStringValue());
                    StateStack.top() = EState::AttributesAfterKey;  
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected string literal, but lexeme %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::AttributesAfterKey:
                if (tokenType == ETokenType::Equals) {
                    StateStack.top() = EState::AttributesBeforeValue;
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected '=', but lexeme %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::AttributesBeforeValue:
                ConsumeAny(token);
                break;

            case EState::AttributesAfterValue:
                if (tokenType == ETokenType::Semicolon) {
                    StateStack.top() = EState::AttributesBeforeKey;
                } else {
                    ythrow yexception() << Sprintf("Error parsing YSON: Expected ';' or '>', but lexeme %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            default:
                YUNREACHABLE();
        }
    }

    void ConsumeParsed(const TToken& token)
    {
        YASSERT(StateStack.top() == EState::FragmentParsed);

        auto tokenType = token.GetType();
        if (!Fragmented) {
            ythrow yexception() << Sprintf("Error parsing YSON: Document is already parsed, but unexpected lexeme %s of type %s found (%s)",
                ~token.ToString().Quote(),
                ~tokenType.ToString(),
                ~GetPositionInfo());
        }
        if (tokenType != ETokenType::Semicolon) {
            ythrow yexception() << Sprintf("Error parsing YSON: Expected ';', but lexeme %s of type %s found (%s)",
                ~token.ToString().Quote(),
                ~tokenType.ToString(),
                ~GetPositionInfo());
        }

        StateStack.pop();
        YASSERT(StateStack.empty());
        ++Fragment;
    }

    bool ConsumeEnd()
    {
        bool attributes =
            Lexer.GetState() == TLexer::EState::Terminal &&
            Lexer.GetToken().GetType() == ETokenType::LeftAngle;
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

void ParseYson(const TYson& yson, IYsonConsumer* consumer, bool fragmented)
{
    TStringInput input(yson);
    ParseYson(&input, consumer, fragmented);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
