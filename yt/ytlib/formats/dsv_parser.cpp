#include "stdafx.h"
#include "dsv_parser.h"
#include "dsv_symbols.h"

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TDsvParser
    : public IParser
{
public:
    TDsvParser(
        IYsonConsumer* consumer,
        TDsvFormatConfigPtr config,
        bool wrapWithmap);

    virtual void Read(const TStringBuf& data) override;
    virtual void Finish() override;

private:
    IYsonConsumer* Consumer;
    TDsvFormatConfigPtr Config;
    bool WrapWithMap;

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

TDsvParser::TDsvParser(IYsonConsumer* consumer, TDsvFormatConfigPtr config, bool wrapWithMap)
    : Consumer(consumer)
    , Config(config)
    , WrapWithMap(wrapWithMap)
    , State(GetStartState())
    , NewRecordStarted(!WrapWithMap)
    , ExpectingEscapedChar(false)
    , RecordCount(1)
    , FieldCount(1)
{
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
        THROW_ERROR_EXCEPTION("Incomplete escape sequence in DSV");
    }
    if (State == EState::InsideKey) {
        if (!CurrentToken.empty()) {
            THROW_ERROR_EXCEPTION("Missing value in DSV");
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
    // Process escaping symbols.
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
    if (WrapWithMap && NewRecordStarted) {
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
        // TODO(babenko): provide GetPositionInfo
        THROW_ERROR_EXCEPTION("Malformed line prefix in DSV: expected %s, found %s",
            ~Config->LinePrefix.Get().Quote(),
            ~prefix.Quote())
            << TErrorAttribute("record_index", RecordCount)
            << TErrorAttribute("field_index", FieldCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ParseDsv(
    TInputStream* input,
    IYsonConsumer* consumer,
    TDsvFormatConfigPtr config)
{
    auto parser = CreateParserForDsv(consumer, config);
    Parse(input, consumer, parser.Get());
}

void ParseDsv(
    const TStringBuf& data,
    IYsonConsumer* consumer,
    TDsvFormatConfigPtr config)
{
    auto parser = CreateParserForDsv(consumer, config);
    parser->Read(data);
    parser->Finish();
}

TAutoPtr<IParser> CreateParserForDsv(
    IYsonConsumer* consumer,
    TDsvFormatConfigPtr config,
    bool wrapWithMap)
{
    return new TDsvParser(consumer, config, wrapWithMap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
