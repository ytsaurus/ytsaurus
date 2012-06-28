#include "stdafx.h"
#include "yson_parser.h"

#include "yson_consumer.h"
#include "lexer.h"
#include "yson_format.h"
#include "yson_stream.h"

#include <ytlib/misc/foreach.h>

#include <stack>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser::TImpl
{
    DECLARE_ENUM(EState,
        // ^ stands for current position
        (Start)                 // ^ (initial state for parsing)
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
    bool EnableLinePositionInfo;
    int Offset;
    int Line;
    int Position;
    int Fragment;

public:
    TImpl(IYsonConsumer* consumer, EYsonType type, bool enableLinePositionInfo)
        : Consumer(consumer)
        , Type(type)
        , EnableLinePositionInfo(enableLinePositionInfo)
        , Offset(0)
        , Line(1)
        , Position(1)
        , Fragment(0)
    {
        switch (type) {
            case EYsonType::Node:
                StateStack.push(EState::Start);
                break;

            case EYsonType::ListFragment:
                StateStack.push(EState::ListBeforeItem);
                break;

            case EYsonType::MapFragment:
                StateStack.push(EState::MapBeforeKey);
                break;

            default:
                YUNREACHABLE();
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

        if (StateStack.top() != EState::Parsed) {
            ythrow yexception() << Sprintf("Premature end of stream (State: %s, %s)",
                ~StateStack.top().ToString(),
                ~GetPositionInfo());
        }
    }

private:
    void OnRangeConsumed(const char* begin, const char* end)
    {
        Offset += end - begin;
        if (UNLIKELY(EnableLinePositionInfo)) { // Performance critical check
            for (auto current = begin; current != end; ++current) {
                ++Position;
                if (*current == '\n') {
                    ++Line;
                    Position = 1;
                }
            }
        }
    }

    void ConsumeToken(const TToken& token)
    {
        switch (StateStack.top()) {
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
        auto& topState = StateStack.top();

        switch (tokenType) {
            case ETokenType::EndOfStream:
                if (inFragment) {
                    topState = EState::Parsed;
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
                switch (topState) {
                    case EState::ListBeforeItem:
                        Consumer->OnListItem();
                        ConsumeAny(token, true);
                        break;

                    case EState::ListAfterItem:
                        if (tokenType == ListItemSeparatorToken) {
                            topState = EState::ListBeforeItem;
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
        bool inFragment = Type == EYsonType::MapFragment && StateStack.size() == 1;
        auto tokenType = token.GetType();
        auto& topState = StateStack.top();
        
        if (inFragment && (topState == EState::MapBeforeKey || topState == EState::MapAfterValue)) {
            if (tokenType == ETokenType::EndOfStream) {
                topState = EState::Parsed;
                return;
            } else if (tokenType == EndMapToken) {
                ythrow yexception() << Sprintf("Unexpected end of map in map fragment (%s)",
                    ~GetPositionInfo());
            }
        }

        if (tokenType == ETokenType::EndOfStream) {
            return;
        }

        switch (topState) {
            case EState::MapBeforeKey:
                if (tokenType == EndMapToken) {
                    Consumer->OnEndMap();
                    StateStack.pop();
                    OnItemConsumed();
                } else if (tokenType == ETokenType::String) {
                    Consumer->OnKeyedItem(token.GetStringValue());
                    topState = EState::MapAfterKey;  
                } else {
                    ythrow yexception() << Sprintf("Expected string literal, but token %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::MapAfterKey:
                if (tokenType == KeyValueSeparatorToken) {
                    topState = EState::MapBeforeValue;
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
                } else if (tokenType == KeyedItemSeparatorToken) {
                    topState = EState::MapBeforeKey;
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
        auto& topState = StateStack.top();

        if (tokenType == ETokenType::EndOfStream) {
            return;
        }

        if (tokenType == EndAttributesToken &&
            (topState == EState::AttributesBeforeKey || topState == EState::AttributesAfterValue))
        {
            Consumer->OnEndAttributes();
            topState = EState::AfterAttributes;
            return;
        }

        switch (topState) {
            case EState::AttributesBeforeKey:
                if (tokenType == ETokenType::String) {
                    Consumer->OnKeyedItem(token.GetStringValue());
                    topState = EState::AttributesAfterKey;  
                } else {
                    ythrow yexception() << Sprintf("Expected string literal, but token %s of type %s found (%s)",
                        ~token.ToString().Quote(),
                        ~tokenType.ToString(),
                        ~GetPositionInfo());
                }
                break;

            case EState::AttributesAfterKey:
                if (tokenType == KeyValueSeparatorToken) {
                    topState = EState::AttributesBeforeValue;
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
                if (tokenType == KeyedItemSeparatorToken) {
                    topState = EState::AttributesBeforeKey;
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
        auto& topState = StateStack.top();
        switch (topState) {
            case EState::Start:
                StateStack.push(EState::Parsed);
                break;

            case EState::ListBeforeItem:
                topState = EState::ListAfterItem;
                break;

            case EState::MapBeforeValue:
                topState = EState::MapAfterValue;
                break;

            case EState::AttributesBeforeValue:
                topState = EState::AttributesAfterValue;
                break;
            
            default:
                YUNREACHABLE();
        }
    }

    Stroka GetPositionInfo() const
    {
        TStringStream stream;
        stream << "Offset: " << Offset;
        if (EnableLinePositionInfo) {
            stream << ", Line: " << Line;
            stream << ", Position: " << Position;
        }
        if (Type != EYsonType::Node) {
            stream << ", Fragment: " << Fragment;
        }
        return stream.Str();
    }
};

////////////////////////////////////////////////////////////////////////////////

TYsonParser::TYsonParser(
    IYsonConsumer *consumer,
    EYsonType type,
    bool enableLinePositionInfo)
    : Impl(new TImpl(consumer, type, enableLinePositionInfo))
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

static const size_t ParseChunkSize = 1 << 16;

void ParseYson(const TYsonInput& input, IYsonConsumer* consumer, bool enableLinePositionInfo)
{
    TYsonParser parser(consumer, input.GetType(), enableLinePositionInfo);
    char chunk[ParseChunkSize];
    while (true) {
        // Read a chunk.
        size_t bytesRead = input.GetStream()->Read(chunk, ParseChunkSize);
        if (bytesRead == 0) {
            break;
        }
        // Parse the chunk.
        parser.Read(TStringBuf(chunk, bytesRead));
    }
    parser.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
