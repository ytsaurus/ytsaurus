#include "stdafx.h"
#include "yamr_parser.h"

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TYamrDelimitedParser
    : public NYTree::IParser
{
public:
    TYamrDelimitedParser(
        NYTree::IYsonConsumer* consumer,
        TYamrFormatConfigPtr config);

    virtual void Read(const TStringBuf& data) override;
    virtual void Finish() override;

private:
    NYTree::IYsonConsumer* Consumer;
    TYamrFormatConfigPtr Config;

    Stroka CurrentToken;
    Stroka Key;

    const char* Consume(const char* begin, const char* end);
    const char* FindNextStopSymbol(const char* begin, const char* end);
    const char* FindEndOfRecord(const char* begin, const char* end);

    // returns pointer to next fragment or NULL if record is not fully present in [begin, end)
    const char* TryConsumeRecord(const char* begin, const char *end);

    bool IsStopSymbol[256];

    DECLARE_ENUM(EState,
        (InsideKey)
        (InsideSubkey)
        (InsideValue)
    );
    EState State;
};

////////////////////////////////////////////////////////////////////////////////

TYamrDelimitedParser::TYamrDelimitedParser(IYsonConsumer* consumer, TYamrFormatConfigPtr config)
    : Consumer(consumer)
    , Config(config)
    , State(EState::InsideKey)
{
    YCHECK(Config);
    YCHECK(!Config->Lenval);

    memset(IsStopSymbol, 0, sizeof(IsStopSymbol));
    IsStopSymbol[Config->RecordSeparator] = true;
    IsStopSymbol[Config->FieldSeparator] = true;
}

void TYamrDelimitedParser::Read(const TStringBuf& data)
{
    auto current = data.begin();
    while (current != data.end()) {
        current = Consume(current, data.end());
    }
}

void TYamrDelimitedParser::Finish()
{
    if (State == EState::InsideValue) {
        Consumer->OnKeyedItem(Config->Value);
        Consumer->OnStringScalar(CurrentToken);
        Consumer->OnEndMap();
        CurrentToken.clear();
    }
}

const char* TYamrDelimitedParser::Consume(const char* begin, const char* end)
{
    // First, try fast parsing
    if (State == EState::InsideKey && CurrentToken.empty()) {
        const char* next = TryConsumeRecord(begin, end);
        if (next !=  NULL) {
            return next;
        }
    }

    // Now, do more slow one
    switch (State) {
        case EState::InsideKey:
        case EState::InsideSubkey: {
            const char* next = FindNextStopSymbol(begin, end);
            CurrentToken.append(begin, next);
            if (next != end) {
                if (*next == Config->RecordSeparator) {
                    CurrentToken.clear();
                    State = EState::InsideKey;
                } else {
                    YCHECK(*next == Config->FieldSeparator);
                    if (State == EState::InsideKey) {
                        Key = CurrentToken;
                        CurrentToken.clear();
                        if (Config->HasSubkey) {
                            State = EState::InsideSubkey;
                        } else {
                            Consumer->OnListItem();
                            Consumer->OnBeginMap();
                            Consumer->OnKeyedItem(Config->Key);
                            Consumer->OnStringScalar(Key);
                            State = EState::InsideValue;
                        }
                    } else {
                        // TODO(panin): extract method
                        Consumer->OnListItem();
                        Consumer->OnBeginMap();
                        Consumer->OnKeyedItem(Config->Key);
                        Consumer->OnStringScalar(Key);
                        Consumer->OnKeyedItem(Config->Subkey);
                        Consumer->OnStringScalar(CurrentToken);
                        CurrentToken.clear();
                        State = EState::InsideValue;
                    }
                }
                ++next;
            }
            return next;
        }
        case EState::InsideValue: {
            const char* next = FindEndOfRecord(begin, end);
            CurrentToken.append(begin, next);
            if (next != end) {
                Consumer->OnKeyedItem(Config->Value);
                Consumer->OnStringScalar(CurrentToken);
                Consumer->OnEndMap();
                CurrentToken.clear();
                State = EState::InsideKey;
                ++next;
            }
            return next;
        }
    }
    YUNREACHABLE();
}

const char* TYamrDelimitedParser::FindNextStopSymbol(const char* begin, const char* end)
{
    auto current = begin;
    for ( ; current != end; ++current) {
        if (IsStopSymbol[static_cast<ui8>(*current)]) {
            return current;
        }
    }
    return end;
}

const char* TYamrDelimitedParser::FindEndOfRecord(const char* begin, const char* end)
{
    auto current = begin;
    for ( ; current != end; ++current) {
        if (*current == Config->RecordSeparator) {
            return current;
        }
    }
    return end;
}

const char* TYamrDelimitedParser::TryConsumeRecord(const char* begin, const char* end)
{
    const char* endOfKey;
    const char* endOfSubkey;
    const char* endOfValue;
    const char* current;

    current = FindNextStopSymbol(begin, end);
    if (current == end) {
        return NULL;
    }
    if (*current == Config->RecordSeparator) {
        return (current + 1);
    }
    YCHECK(*current == Config->FieldSeparator);
    endOfKey = current;
    ++current;

    if (Config->HasSubkey) {
        current = FindNextStopSymbol(current, end);
        if (current == end) {
            return NULL;
        }
        if (*current == Config->RecordSeparator) {
            return (current + 1);
        }
        YCHECK(*current == Config->FieldSeparator);
        endOfSubkey = current;
        ++current;
    }

    current = FindEndOfRecord(current, end);
    if (current == end) {
        return NULL;
    }
    endOfValue = current;

    Consumer->OnListItem();
    Consumer->OnBeginMap();
    Consumer->OnKeyedItem(Config->Key);
    Consumer->OnStringScalar(TStringBuf(begin, endOfKey));
    if (Config->HasSubkey) {
        Consumer->OnKeyedItem(Config->Subkey);
        Consumer->OnStringScalar(TStringBuf(endOfKey + 1, endOfSubkey));
    }
    Consumer->OnKeyedItem(Config->Value);

    const char* beginOfValue = Config->HasSubkey ?
                            endOfSubkey + 1 :
                            endOfKey + 1;
    Consumer->OnStringScalar(TStringBuf(beginOfValue, endOfValue));
    Consumer->OnEndMap();

    return endOfValue + 1;
}


////////////////////////////////////////////////////////////////////////////////

class TYamrLenvalParser
    : public NYTree::IParser
{
public:
    TYamrLenvalParser(
        NYTree::IYsonConsumer* consumer,
        TYamrFormatConfigPtr config);

    virtual void Read(const TStringBuf& data) override;
    virtual void Finish() override;

private:
    NYTree::IYsonConsumer* Consumer;
    TYamrFormatConfigPtr Config;

    Stroka CurrentToken;

    const char* Consume(const char* begin, const char* end);

    union {
        ui32 Length;
        char Bytes[4];
    } Union;
    bool ReadingLength;
    i32 BytesToRead;

    DECLARE_ENUM(EState,
        (InsideKey)
        (InsideSubkey)
        (InsideValue)
    );
    EState State;
};

////////////////////////////////////////////////////////////////////////////////

TYamrLenvalParser::TYamrLenvalParser(IYsonConsumer* consumer, TYamrFormatConfigPtr config)
    : Consumer(consumer)
    , Config(config)
    , ReadingLength(true)
    , BytesToRead(4)
    , State(EState::InsideKey)
{
    YCHECK(Config);
    YCHECK(Config->Lenval);
}

void TYamrLenvalParser::Read(const TStringBuf& data)
{
    auto current = data.begin();
    while (current != data.end()) {
        current = Consume(current, data.end());
    }
}

void TYamrLenvalParser::Finish()
{
    if (State == EState::InsideValue && !ReadingLength && BytesToRead == 0) {
        Consumer->OnKeyedItem(Config->Value);
        Consumer->OnStringScalar(CurrentToken);
        Consumer->OnEndMap();
        return;
    }

    if (!(State == EState::InsideKey && ReadingLength && BytesToRead == 4)) {
        ythrow yexception() << "Premature end of stream";
    }
}

const char* TYamrLenvalParser::Consume(const char* begin, const char* end)
{
    const char* current = begin;
    while (BytesToRead > 0 && current != end) {
        if (ReadingLength) {
            Union.Bytes[4 - BytesToRead] = *current;
        }
        ++current;
        --BytesToRead;
    }
    if (!ReadingLength) {
        CurrentToken.append(begin, current);
    }
    if (BytesToRead != 0) return current;

    if (ReadingLength) {
        ReadingLength = false;
        BytesToRead = Union.Length;
    } else {
        switch (State) {
            case EState::InsideKey:
                Consumer->OnListItem();
                Consumer->OnBeginMap();
                Consumer->OnKeyedItem(Config->Key);
                Consumer->OnStringScalar(CurrentToken);
                State = Config->HasSubkey ?
                    EState::InsideSubkey :
                    EState::InsideValue;
                break;
            case EState::InsideSubkey:
                Consumer->OnKeyedItem(Config->Subkey);
                Consumer->OnStringScalar(CurrentToken);
                State = EState::InsideValue;
                break;
            case EState::InsideValue:
                Consumer->OnKeyedItem(Config->Value);
                Consumer->OnStringScalar(CurrentToken);
                Consumer->OnEndMap();
                State = EState::InsideKey;
                break;
            default:
                YUNREACHABLE();
        }
        CurrentToken.clear();
        ReadingLength = true;
        BytesToRead = 4;
    }
    return current;
}

///////////////////////////////////////////////////////////////////////////////

TAutoPtr<NYTree::IParser> CreateParserForYamr(
    NYTree::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{
    if (!config) {
        config = New<TYamrFormatConfig>();
    }
    if (config->Lenval) {
        return new TYamrLenvalParser(consumer, config);
    } else {
        return new TYamrDelimitedParser(consumer, config);
    }
}

///////////////////////////////////////////////////////////////////////////////

void ParseYamr(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{
    auto parser = CreateParserForYamr(consumer, config);
    Parse(input, consumer, ~parser);
}

void ParseYamr(
    const TStringBuf& data,
    NYTree::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{
    auto parser = CreateParserForYamr(consumer, config);
    parser->Read(data);
    parser->Finish();
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
