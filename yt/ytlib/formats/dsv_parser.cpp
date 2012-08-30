#include "stdafx.h"
#include "dsv_parser.h"
#include "dsv_symbols.h"

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TDsvParser
    : public NYTree::IParser
{
public:
    TDsvParser(
        NYTree::IYsonConsumer* consumer,
        TDsvFormatConfigPtr config,
        bool makeRecordProcessing);

    virtual void Read(const TStringBuf& data);
    virtual void Finish();

private:
    NYTree::IYsonConsumer* Consumer;
    TDsvFormatConfigPtr Config;
    bool MakeRecordProcessing;

    const char* Consume(const char* begin, const char* end);
    const char* FindStopPosition(const char* begin, const char* end) const;

    void StartRecordIfNeeded();
    void FinishRecord();

    void ValidatePrefix(const Stroka& prefix) const;

    DECLARE_ENUM(EState,
        (InsidePrefix)
        (InsideKey)
        (InsideValue)
    );
    EState State;

    EState GetStartState() const;

    bool NewRecordStarted;
    bool ExpectingEscapedChar;

    int RecordCount;
    int FieldCount;

    Stroka CurrentToken;
};


////////////////////////////////////////////////////////////////////////////////

TDsvParser::TDsvParser(IYsonConsumer* consumer, TDsvFormatConfigPtr config, bool makeRecordProcessing)
    : Consumer(consumer)
    , Config(config)
    , MakeRecordProcessing(makeRecordProcessing)
    , State(GetStartState())
    , NewRecordStarted(!MakeRecordProcessing)
    , ExpectingEscapedChar(false)
    , RecordCount(1)
    , FieldCount(1)
{
    if (!Config) {
        Config = New<TDsvFormatConfig>();
    }
    InitDsvSymbols(Config);
}

void TDsvParser::Read(const TStringBuf& data)
{
    auto current = data.begin();
    while (current != data.end()) {
        current = Consume(current, data.end());
    }
}

void TDsvParser::Finish()
{
    if (ExpectingEscapedChar) {
        ythrow yexception() << "Dsv record is incomplete, expecting escaped character";
    }
    if (State == EState::InsideKey) {
        if (!CurrentToken.empty()) {
            ythrow yexception() << "Dsv record is incomplete, there is key without value";
        }
    }
    if (!CurrentToken.empty()) {
        StartRecordIfNeeded();
    }
    if (State == EState::InsideValue) {
        Consumer->OnStringScalar(CurrentToken);
    }
    if (State == EState::InsidePrefix && !CurrentToken.empty()) {
        ValidatePrefix(CurrentToken);
    }
    CurrentToken.clear();
    FinishRecord();
}

const char* TDsvParser::Consume(const char* begin, const char* end)
{
    // Processing escaping symbols
    if (!ExpectingEscapedChar && *begin == Config->EscapingSymbol) {
        ExpectingEscapedChar = true;
        return begin + 1;
    }
    if (ExpectingEscapedChar) {
        CurrentToken.append(UnEscapingTable[static_cast<ui8>(*begin)]);
        ExpectingEscapedChar = false;
        return begin + 1;
    }

    // Read until stop symbol
    auto next = FindStopPosition(begin, end);
    CurrentToken.append(begin, next);
    if (next == end || *next == Config->EscapingSymbol) {
        return next;
    }

    // Here, we have finished reading prefix, key or value
    if (State == EState::InsidePrefix) {
        StartRecordIfNeeded();
        ValidatePrefix(CurrentToken);
        State = EState::InsideKey;
    }
    else if (State == EState::InsideKey) {
        StartRecordIfNeeded();
        if (*next == Config->KeyValueSeparator) {
            Consumer->OnKeyedItem(CurrentToken);
            State = EState::InsideValue;
        }
    }
    else if (State == EState::InsideValue) {
        Consumer->OnStringScalar(CurrentToken);
        State = EState::InsideKey;
        FieldCount += 1;
    }
    else {
        YUNREACHABLE();
    }

    CurrentToken.clear();
    if (*next == Config->RecordSeparator) {
        FinishRecord();
    }
    return next + 1;
}

TDsvParser::EState TDsvParser::GetStartState() const
{
    return Config->LinePrefix ? EState::InsidePrefix : EState::InsideKey;
}

void TDsvParser::StartRecordIfNeeded()
{
    if (!NewRecordStarted) {
        Consumer->OnListItem();
        Consumer->OnBeginMap();
        NewRecordStarted = true;
    }
}

void TDsvParser::FinishRecord()
{
    if (MakeRecordProcessing && NewRecordStarted) {
        Consumer->OnEndMap();
        NewRecordStarted = false;
    }
    State = GetStartState();

    RecordCount += 1;
    FieldCount = 1;
}

const char* TDsvParser::FindStopPosition(const char* begin, const char* end) const
{
    bool* IsStopSymbol =
        (State == EState::InsideKey) ?
        IsKeyStopSymbol :
        IsValueStopSymbol;

    auto current = begin;
    for ( ; current < end; ++current) {
        if (IsStopSymbol[static_cast<ui8>(*current)]) {
            return current;
        }
    }
    return end;
}

void TDsvParser::ValidatePrefix(const Stroka& prefix) const
{
    if (prefix != Config->LinePrefix.Get()) {
        ythrow yexception() <<
            Sprintf("Expected %s at the beginning of record, "
                    "found %s (Record: %d, Field: %d)",
                ~Config->LinePrefix.Get().Quote(),
                ~prefix.Quote(),
                RecordCount,
                FieldCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ParseDsv(TInputStream* input, IYsonConsumer* consumer, TDsvFormatConfigPtr config)
{
    auto parser = CreateParserForDsv(consumer, config);
    Parse(input, consumer, parser.Get());
}

void ParseDsv(const TStringBuf& data,
    IYsonConsumer* consumer,
    TDsvFormatConfigPtr config)
{
    auto parser = CreateParserForDsv(consumer, config);
    parser->Read(data);
    parser->Finish();
}

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<NYTree::IParser> CreateParserForDsv(
    NYTree::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config,
    bool makeRecordProcessing)
{
    if (!config) {
        config = New<TDsvFormatConfig>();
    }
    return new TDsvParser(consumer, config, makeRecordProcessing);
}

} // namespace NFormats
} // namespace NYT
