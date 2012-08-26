#include "stdafx.h"
#include "yamr_parser.h"
#include "yamr_base_parser.h"

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TYamrDelimitedParser
    : public TYamrBaseParser
{
public:
    TYamrDelimitedParser(
        NYTree::IYsonConsumer* consumer,
        TYamrFormatConfigPtr config)
        : TYamrBaseParser(
              config->FieldSeparator,
              config->RecordSeparator,
              config->HasSubkey)
        , Consumer(consumer)
        , Config(config)
    { }

private:
    NYTree::IYsonConsumer* Consumer;
    TYamrFormatConfigPtr Config;

    void ConsumeKey(const TStringBuf& key)
    {
        Consumer->OnListItem();
        Consumer->OnBeginMap();
        Consumer->OnKeyedItem(Config->Key);
        Consumer->OnStringScalar(key);
    }

    void ConsumeSubkey(const TStringBuf& subkey)
    {
        Consumer->OnKeyedItem(Config->Subkey);
        Consumer->OnStringScalar(subkey);
    }

    void ConsumeValue(const TStringBuf& value)
    {
        Consumer->OnKeyedItem(Config->Value);
        Consumer->OnStringScalar(value);
        Consumer->OnEndMap();
    }
};

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
    const char* ConsumeLength(const char* begin, const char* end);
    const char* ConsumeData(const char* begin, const char* end);


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
    if (ReadingLength) {
        return ConsumeLength(begin, end);
    } else {
        return ConsumeData(begin, end);
    }
}

const char* TYamrLenvalParser::ConsumeLength(const char* begin, const char* end)
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

    ReadingLength = false;
    BytesToRead = Union.Length;
    return current;
}

const char* TYamrLenvalParser::ConsumeData(const char* begin, const char* end)
{
    TStringBuf data;
    const char* current = begin + BytesToRead;

    if (current > end) {
        CurrentToken.append(begin, end);
        BytesToRead -= (end - begin);
        YCHECK(BytesToRead > 0);
        return end;
    }

    if (CurrentToken.empty()) {
        data = TStringBuf(begin, current);
    } else {
        CurrentToken.append(begin, current);
        data = CurrentToken;
    }

    switch (State) {
        case EState::InsideKey:
            Consumer->OnListItem();
            Consumer->OnBeginMap();
            Consumer->OnKeyedItem(Config->Key);
            Consumer->OnStringScalar(data);
            State = Config->HasSubkey ?
                EState::InsideSubkey :
                EState::InsideValue;
            break;
        case EState::InsideSubkey:
            Consumer->OnKeyedItem(Config->Subkey);
            Consumer->OnStringScalar(data);
            State = EState::InsideValue;
            break;
        case EState::InsideValue:
            Consumer->OnKeyedItem(Config->Value);
            Consumer->OnStringScalar(data);
            Consumer->OnEndMap();
            State = EState::InsideKey;
            break;
        default:
            YUNREACHABLE();
    }
    CurrentToken.clear();
    ReadingLength = true;
    BytesToRead = 4;

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
