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
        // ^ stands for current position
        (Start)                 // ^ (special value for empty stack)
        (StringEnd)             // "..." ^
        (IntegerEnd)              // 123...9 ^
        (DoubleEnd)             // 0.123...9 ^
        (ListBeforeItem)        // [...; ^
        (ListAfterItem)         // [... ^
        (ListEnd)               // [...] ^
        (MapBeforeKey)          // {...; ^
        (MapAfterKey)           // {...; "..." ^
        (MapBeforeValue)        // {...; "..." = ^
        (MapAfterValue)         // {...; "..." = ... ^
        (MapEnd)                // {...} ^
        (AttributesBeforeKey)   // <...; ^
        (AttributesAfterKey)    // <...; "..." ^
        (AttributesBeforeValue) // <...; "..." = ^
        (AttributesAfterValue)  // <...; "..." = ... ^
        (Parsed)                // ...<...> ^
    );

    IYsonConsumer* Consumer;
    EMode Mode;

    TLexer Lexer;
    std::stack<EState> StateStack;

    Stroka CachedStringValue;
    i64 CachedIntegerValue;
    double CachedDoubleValue;

    // Diagnostics info
    int Offset;
    int Line;
    int Position;
    int Fragment;

public:
    TImpl(IYsonConsumer* consumer, EMode mode)
        : Consumer(consumer)
        , Mode(mode)
        , CachedIntegerValue(0)
        , CachedDoubleValue(0.0)
        , Offset(0)
        , Line(1)
        , Position(1)
        , Fragment(0)
    {
        switch (mode) {
            case EMode::ListFragment:
                StateStack.push(EState::ListBeforeItem);
                break;

            case EMode::MapFragment:
                StateStack.push(EState::MapBeforeKey);
                break;

            default:
                break;
        }
    }

    void Consume(char ch)
    {
        bool stop = false;
        while (!stop) {
            try {
                stop = Lexer.Consume(ch);
            } catch (...) {
                ythrow yexception() << Sprintf("Could not read symbol %s (%s):\n%s",
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

        ConsumeToken(TToken::EndOfStream);

        if (CurrentState() != EState::Parsed) {
            ythrow yexception() << Sprintf("Cannot finish parsing in state %s (%s)",
                ~CurrentState().ToString(),
                ~GetPositionInfo());
        }

        YASSERT(CachedStringValue.empty());
        YASSERT(CachedIntegerValue == 0);
        YASSERT(CachedDoubleValue == 0.0);
    }

private:
    void ConsumeToken(const TToken& token)
    {
        bool consumed = false;
        while (!consumed) {
            switch (CurrentState()) {
                case EState::Start:
                    ConsumeAny(token);
                    consumed = true;
                    break;

                case EState::StringEnd:
                case EState::IntegerEnd:
                case EState::DoubleEnd:
                case EState::ListEnd:
                case EState::MapEnd:
                    consumed = ConsumeEnd(token);
                    break;

                case EState::ListBeforeItem:
                case EState::ListAfterItem:
                    consumed = ConsumeList(token);
                    break;

                case EState::MapBeforeKey:
                case EState::MapAfterKey:
                case EState::MapBeforeValue:
                case EState::MapAfterValue:
                    consumed = ConsumeMap(token);
                    break;

                case EState::AttributesBeforeKey:
                case EState::AttributesAfterKey:
                case EState::AttributesBeforeValue:
                case EState::AttributesAfterValue:
                    ConsumeAttributes(token);
                    consumed = true;
                    break;

                case EState::Parsed:
                    ConsumeParsed(token);
                    consumed = true;
                    break;

                default:
                    YUNREACHABLE();
            }
        }
    }

    void ConsumeAny(const TToken& token)
    {
        switch (token.GetType()) {        
            case ETokenType::None:
                break;

            case ETokenType::String:
                CachedStringValue = token.GetStringValue();
                StateStack.push(EState::StringEnd);
                break;

            case ETokenType::Integer:
                CachedIntegerValue = token.GetIntegerValue();
                StateStack.push(EState::IntegerEnd);
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
                ythrow yexception() << Sprintf("Unexpected lexeme %s of type %s (%s)",
                    ~token.ToString().Quote(),
                    ~token.GetType().ToString(),
                    ~GetPositionInfo());
        }
    }

    bool ConsumeList(const TToken& token)
    {
        auto tokenType = token.GetType();

        if (Mode == EMode::ListFragment && StateStack.size() == 1) {
            if (tokenType == ETokenType::None) {
                StateStack.top() = EState::Parsed;
                return false;
            } else if (tokenType == ETokenType::RightBracket) {
                ythrow yexception() << Sprintf("Unexpected end of list in list fragment (%s)",
                    ~GetPositionInfo());
            }
        }

        if (tokenType == ETokenType::None) {
            return true;
        }

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
                    ythrow yexception() << Sprintf("Expected ';' or ']', but lexeme %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            default:
                YUNREACHABLE();
        }

        return true;
    }

    bool ConsumeMap(const TToken& token)
    {
        auto tokenType = token.GetType();
        auto currentState = CurrentState();

        if (Mode == EMode::MapFragment && StateStack.size() == 1 &&
            (currentState == EState::MapBeforeKey || currentState == EState::MapAfterValue))
        {
            if (tokenType == ETokenType::None) {
                StateStack.top() = EState::Parsed;
                return false;
            } else if (tokenType == ETokenType::RightBrace) {
                ythrow yexception() << Sprintf("Unexpected end of map in map fragment (%s)",
                    ~GetPositionInfo());
            }
        }

        if (tokenType == ETokenType::None) {
            return true;
        }

        switch (currentState) {
            case EState::MapBeforeKey:
                if (tokenType == ETokenType::RightBrace) {
                    StateStack.top() = EState::MapEnd;
                } else if (tokenType == ETokenType::String) {
                    Consumer->OnMapItem(token.GetStringValue());
                    StateStack.top() = EState::MapAfterKey;  
                } else {
                    ythrow yexception() << Sprintf("Expected string literal, but lexeme %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::MapAfterKey:
                if (tokenType == ETokenType::Equals) {
                    StateStack.top() = EState::MapBeforeValue;
                } else {
                    ythrow yexception() << Sprintf("Expected '=', but lexeme %s of type %s found (%s)",
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
                    ythrow yexception() << Sprintf("Expected ';' or '}', but lexeme %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            default:
                YUNREACHABLE();
        }

        return true;
    }

    void ConsumeAttributes(const TToken& token)
    {
        auto tokenType = token.GetType();
        auto currentState = CurrentState();

        if (tokenType == ETokenType::None) {
            return;
        }

        if (tokenType == ETokenType::RightAngle &&
            (currentState == EState::AttributesBeforeKey || currentState == EState::AttributesAfterValue))
        {
            Consumer->OnEndAttributes();
            OnItemConsumed();
            return;
        }

        switch (currentState) {
            case EState::AttributesBeforeKey:
                if (tokenType == ETokenType::String) {
                    Consumer->OnAttributesItem(token.GetStringValue());
                    StateStack.top() = EState::AttributesAfterKey;  
                } else {
                    ythrow yexception() << Sprintf("Expected string literal, but lexeme %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::AttributesAfterKey:
                if (tokenType == ETokenType::Equals) {
                    StateStack.top() = EState::AttributesBeforeValue;
                } else {
                    ythrow yexception() << Sprintf("Expected '=', but lexeme %s of type %s found (%s)",
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
                    ythrow yexception() << Sprintf("Expected ';' or '>', but lexeme %s of type %s found (%s)",
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
        YASSERT(StateStack.top() == EState::Parsed);

        auto tokenType = token.GetType();
        if (tokenType != ETokenType::None) {
            ythrow yexception() << Sprintf("Node is already parsed, but unexpected lexeme %s of type %s found (%s)",
                ~token.ToString().Quote(),
                ~tokenType.ToString(),
                ~GetPositionInfo());
        }
    }

    bool ConsumeEnd(const TToken& token)
    {
        bool attributes = token.GetType() == ETokenType::LeftAngle;
        switch (CurrentState()) {
            case EState::StringEnd:
                Consumer->OnStringScalar(CachedStringValue, attributes);
                CachedStringValue = Stroka();
                break;

            case EState::IntegerEnd:
                Consumer->OnIntegerScalar(CachedIntegerValue, attributes);
                CachedIntegerValue = 0;
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
            case EState::Start:
                StateStack.push(EState::Parsed);
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
            return EState::Start;
        return StateStack.top();
    }

    Stroka GetPositionInfo() const
    {
        if (Mode == EMode::Node) {
            return Sprintf("Offset: %d, Line: %d, Position: %d",
                Offset,
                Line,
                Position);
        } else {
            return Sprintf("Offset: %d, Line: %d, Position: %d, Fragment: %d",
                Offset,
                Line,
                Position,
                Fragment);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TYsonParser::TYsonParser(IYsonConsumer *consumer, EMode mode)
    : Impl(new TImpl(consumer, mode))
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

void ParseYson(TInputStream* input, IYsonConsumer* consumer, TYsonParser::EMode mode)
{
    TYsonParser parser(consumer, mode);
    char ch;
    while (input->ReadChar(ch)) {
        parser.Consume(ch);
    }
    parser.Finish();
}

void ParseYson(const TYson& yson, IYsonConsumer* consumer, TYsonParser::EMode mode)
{
    TStringInput input(yson);
    ParseYson(&input, consumer, mode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
