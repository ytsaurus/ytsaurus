#include "stdafx.h"
#include "yson_parser.h"

#include "yson_consumer.h"
#include "lexer.h"
#include "yson_format.h"

#include <ytlib/misc/foreach.h>

#include <stack>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser::TImpl
{
    DECLARE_ENUM(EState,
        // ^ stands for current position
        (Start)                 // ^ (special value for empty stack)
        (ListBeforeItem)        // [...; ^
        (ListAfterItem)         // [... ^
        (MapBeforeKey)          // {...; ^
        (MapAfterKey)           // {...; "..." ^
        (MapBeforeValue)        // {...; "..." = ^
        (MapAfterValue)         // {...; "..." = ... ^
        (AttributesBeforeKey)   // <...; ^
        (AttributesAfterKey)    // <...; "..." ^
        (AttributesBeforeValue) // <...; "..." = ^
        (AttributesAfterValue)  // <...; "..." = ... ^
        (AfterAttributes)       // <...> ^
        (Parsed)                // ...<...> ^
    );

    IYsonConsumer* Consumer;
    EYsonType Type;

    TLexer Lexer;
    std::stack<EState> StateStack;

    // Diagnostic info
    int Offset;
    int Line;
    int Position;
    int Fragment;

public:
    TImpl(IYsonConsumer* consumer, EYsonType type)
        : Consumer(consumer)
        , Type(type)
        , Offset(0)
        , Line(1)
        , Position(1)
        , Fragment(0)
    {
        switch (type) {
            case EYsonType::ListFragment:
                StateStack.push(EState::ListBeforeItem);
                break;

            case EYsonType::KeyedFragment:
                StateStack.push(EState::MapBeforeKey);
                break;

            default:
                break;
        }
    }

    void Read(const TStringBuf& data)
    {
        auto begin = data.begin();
        auto end = data.end();
        auto current = begin;
        try {
            while (current != end) {
                auto consumed = Lexer.Read(TStringBuf(current, end));
                if (Lexer.GetState() == TLexer::EState::Terminal) {
                    ConsumeToken(Lexer.GetToken());
                    Lexer.Reset();
                }
                OnRangeConsumed(current, current + consumed);
                current += consumed;
            }
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Could not read symbol %s (%s):\n%s",
                ~Stroka(*current).Quote(),
                ~GetPositionInfo(),
                ex.what());
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
    }

private:
    void OnCharConsumed(char ch)
    {
        ++Offset;
        ++Position;
        if (ch == '\n') {
            ++Line;
            Position = 1;
        }
    }

    void OnRangeConsumed(const char* begin, const char* end)
    {
        int position = Position;
        int line = Line;
        for (auto current = begin; current != end; ++current) {
            ++position;
            if (*current == '\n') {
                ++line;
                position = 1;
            }
        }
        Position = position;
        Line = line;
        Offset += end - begin;
    }

    void ConsumeToken(const TToken& token)
    {
        switch (CurrentState()) {
            case EState::Start:
                ConsumeAny(token, true);
                break;

            case EState::ListBeforeItem:
            case EState::ListAfterItem:
                ConsumeList(token);
                break;

            case EState::MapBeforeKey:
            case EState::MapAfterKey:
            case EState::MapBeforeValue:
            case EState::MapAfterValue:
                ConsumeMap(token);
                break;

            case EState::AttributesBeforeKey:
            case EState::AttributesAfterKey:
            case EState::AttributesBeforeValue:
            case EState::AttributesAfterValue:
                ConsumeAttributes(token);
                break;

            case EState::AfterAttributes:
                StateStack.pop();
                ConsumeAny(token, false);
                break;

            case EState::Parsed:
                ConsumeParsed(token);
                break;

            default:
                YUNREACHABLE();
        }
    }

    void ConsumeAny(const TToken& token, bool allowAttributes)
    {
        switch (token.GetType()) {        
            case ETokenType::EndOfStream:
                break;

            case ETokenType::String:
                Consumer->OnStringScalar(token.GetStringValue());
                OnItemConsumed();
                break;

            case ETokenType::Integer:
                Consumer->OnIntegerScalar(token.GetIntegerValue());
                OnItemConsumed();
                break;

            case ETokenType::Double:
                Consumer->OnDoubleScalar(token.GetDoubleValue());
                OnItemConsumed();
                break;

            case EntityToken:
                Consumer->OnEntity();
                OnItemConsumed();
                break;

            case BeginListToken:
                Consumer->OnBeginList();
                StateStack.push(EState::ListBeforeItem);
                break;

            case BeginMapToken:
                Consumer->OnBeginMap();
                StateStack.push(EState::MapBeforeKey);
                break;

            case BeginAttributesToken:
                if (allowAttributes) {
                    Consumer->OnBeginAttributes();
                    StateStack.push(EState::AttributesBeforeKey);
                } else {
                    ythrow yexception() << Sprintf("Repeating attributes (%s)",
                        ~GetPositionInfo());
                }
                break;

            default:
                ythrow yexception() << Sprintf("Unexpected token %s of type %s (%s)",
                    ~token.ToString().Quote(),
                    ~token.GetType().ToString(),
                    ~GetPositionInfo());
        }
    }

    void ConsumeList(const TToken& token)
    {
        bool inFragment = Type == EYsonType::ListFragment && StateStack.size() == 1;
        auto tokenType = token.GetType();
        switch (tokenType) {
            case ETokenType::EndOfStream:
                if (inFragment) {
                    StateStack.top() = EState::Parsed;
                }
                break;

            case EndListToken:
                if (inFragment) {
                    ythrow yexception() << Sprintf("Unexpected end of list in list fragment (%s)",
                        ~GetPositionInfo());
                }
                Consumer->OnEndList();
                StateStack.pop();
                OnItemConsumed();
                break;

            default:
                switch (CurrentState()) {
                    case EState::ListBeforeItem:
                        Consumer->OnListItem();
                        ConsumeAny(token, true);
                        break;

                    case EState::ListAfterItem:
                        if (tokenType == ItemSeparatorToken) {
                            StateStack.top() = EState::ListBeforeItem;
                        } else {
                            ythrow yexception() << Sprintf("Expected ';' or ']', but token %s of type %s found (%s)",
                                ~token.ToString().Quote(),
                                ~tokenType.ToString(),
                                ~GetPositionInfo());
                        }
                        break;

                    default:
                        YUNREACHABLE();
                }
        }
    }

    void ConsumeMap(const TToken& token)
    {
        auto tokenType = token.GetType();
        auto currentState = CurrentState();

        if (Type == EYsonType::KeyedFragment && StateStack.size() == 1 &&
            (currentState == EState::MapBeforeKey || currentState == EState::MapAfterValue))
        {
            if (tokenType == ETokenType::EndOfStream) {
                StateStack.top() = EState::Parsed;
            } else if (tokenType == EndMapToken) {
                ythrow yexception() << Sprintf("Unexpected end of map in map fragment (%s)",
                    ~GetPositionInfo());
            }
        }

        if (tokenType == ETokenType::EndOfStream) {
            return;
        }

        switch (currentState) {
            case EState::MapBeforeKey:
                if (tokenType == EndMapToken) {
                    Consumer->OnEndMap();
                    StateStack.pop();
                    OnItemConsumed();
                } else if (tokenType == ETokenType::String) {
                    Consumer->OnKeyedItem(token.GetStringValue());
                    StateStack.top() = EState::MapAfterKey;  
                } else {
                    ythrow yexception() << Sprintf("Expected string literal, but token %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::MapAfterKey:
                if (tokenType == KeyValueSeparatorToken) {
                    StateStack.top() = EState::MapBeforeValue;
                } else {
                    ythrow yexception() << Sprintf("Expected '=', but token %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::MapBeforeValue:
                ConsumeAny(token, true);
                break;

            case EState::MapAfterValue:
                if (tokenType == EndMapToken) {
                    Consumer->OnEndMap();
                    StateStack.pop();
                    OnItemConsumed();
                } else if (tokenType == ItemSeparatorToken) {
                    StateStack.top() = EState::MapBeforeKey;
                } else {
                    ythrow yexception() << Sprintf("Expected ';' or '}', but token %s of type %s found (%s)",
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

        if (tokenType == ETokenType::EndOfStream) {
            return;
        }

        if (tokenType == EndAttributesToken &&
            (currentState == EState::AttributesBeforeKey || currentState == EState::AttributesAfterValue))
        {
            Consumer->OnEndAttributes();
            StateStack.top() = EState::AfterAttributes;
            return;
        }

        switch (currentState) {
            case EState::AttributesBeforeKey:
                if (tokenType == ETokenType::String) {
                    Consumer->OnKeyedItem(token.GetStringValue());
                    StateStack.top() = EState::AttributesAfterKey;  
                } else {
                    ythrow yexception() << Sprintf("Expected string literal, but token %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::AttributesAfterKey:
                if (tokenType == KeyValueSeparatorToken) {
                    StateStack.top() = EState::AttributesBeforeValue;
                } else {
                    ythrow yexception() << Sprintf("Expected '=', but token %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::AttributesBeforeValue:
                ConsumeAny(token, true);
                break;

            case EState::AttributesAfterValue:
                if (tokenType == ItemSeparatorToken) {
                    StateStack.top() = EState::AttributesBeforeKey;
                } else {
                    ythrow yexception() << Sprintf("Expected ';' or '>', but token %s of type %s found (%s)",
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
        if (tokenType != ETokenType::EndOfStream) {
            ythrow yexception() << Sprintf("Node is already parsed, but unexpected token %s of type %s found (%s)",
                ~token.ToString().Quote(),
                ~tokenType.ToString(),
                ~GetPositionInfo());
        }
    }

    void OnItemConsumed()
    {
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
        if (Type == EYsonType::Node) {
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

TYsonParser::TYsonParser(IYsonConsumer *consumer, EYsonType type)
    : Impl(new TImpl(consumer, type))
{ }

TYsonParser::~TYsonParser()
{ }

void TYsonParser::Read(const TStringBuf& data)
{
    Impl->Read(data);
}

void TYsonParser::Finish()
{
    Impl->Finish();
}

////////////////////////////////////////////////////////////////////////////////

const size_t ParseChunkSize = 1 << 16;

void ParseYson(TInputStream* input, IYsonConsumer* consumer, EYsonType type)
{
    TYsonParser parser(consumer, type);
    char chunk[ParseChunkSize];
    while (true) {
        // Read a chunk.
        size_t bytesRead = input->Read(chunk, ParseChunkSize);
        if (bytesRead == 0) {
            break;
        }
        // Parse the chunk.
        parser.Read(TStringBuf(chunk, bytesRead));
    }
    parser.Finish();
}

void ParseYson(const TStringBuf& yson, IYsonConsumer* consumer, EYsonType type)
{
    TYsonParser parser(consumer, type);
    parser.Read(yson);
    parser.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
