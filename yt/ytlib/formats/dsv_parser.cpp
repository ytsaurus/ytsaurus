#include "stdafx.h"
#include "dsv_parser.h"

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TDsvParser::TDsvParser(IYsonConsumer* consumer, TDsvFormatConfigPtr config)
    : Consumer(consumer)
    , Config(config)
    , NewRecordStarted(false)
    , ExpectingEscapedChar(false)
{
    if (!Config) {
        Config = New<TDsvFormatConfig>();
    }
    State = GetStartState();

    // TODO(panin): unite with next
    KeyStopSymbols[0] = Config->EscapingSymbol;
    KeyStopSymbols[1] = Config->KeyValueSeparator;
    KeyStopSymbols[2] = Config->RecordSeparator;

    ValueStopSymbols[0] = Config->EscapingSymbol;
    ValueStopSymbols[1] = Config->FieldSeparator;
    ValueStopSymbols[2] = Config->RecordSeparator;
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
    switch(State) {
        case (EState::InsideValue):
            Consumer->OnStringScalar(CurrentToken);
            Consumer->OnEndMap();
            break;
        case (EState::InsidePrefix):
            if (!CurrentToken.empty()) {
                ValidatePrefix(CurrentToken);
                StartRecordIfNeeded();
                Consumer->OnEndMap();
            }
            break;
        case (EState::InsideKey):
            if (!CurrentToken.empty()) {
                ythrow yexception() << Sprintf("Key %s must be followed by value", ~CurrentToken);
            }
            if (NewRecordStarted) {
                Consumer->OnEndMap();
            }
            break;
        default:
            YUNREACHABLE();
    }
}

void TDsvParser::StartRecordIfNeeded()
{
    if (!NewRecordStarted) {
        Consumer->OnListItem();
        Consumer->OnBeginMap();
        NewRecordStarted = true;
    }
}

const char* TDsvParser::Consume(const char* begin, const char* end)
{
    if (!ExpectingEscapedChar && *begin == Config->EscapingSymbol) {
        ExpectingEscapedChar = true;
        ++begin;
        if (begin == end) {
            return begin;
        }
    }

    if (ExpectingEscapedChar) {
        CurrentToken.append(*begin);
        ++begin;
        ExpectingEscapedChar = false;
        if (begin == end) {
            return begin;
        }
    }

    switch (State) {
        case EState::InsidePrefix: {
            auto next = std::find_first_of(
                begin, end,
                ValueStopSymbols, ValueStopSymbols + ARRAY_SIZE(ValueStopSymbols));
            CurrentToken.append(begin, next);
            if (next != end && *next != Config->EscapingSymbol) {
                ValidatePrefix(CurrentToken);
                CurrentToken.clear();
                StartRecordIfNeeded();
                if (*next == Config->RecordSeparator) {
                    Consumer->OnEndMap();
                    NewRecordStarted = false;
                    State = GetStartState();
                } else {
                    State = EState::InsideKey;
                }
                ++next;
            }
            return next;
        }
        case EState::InsideKey: {
            auto next = std::find_first_of(
                begin, end,
                KeyStopSymbols, KeyStopSymbols + ARRAY_SIZE(KeyStopSymbols));
            CurrentToken.append(begin, next);
            if (next != end && *next != Config->EscapingSymbol) {
                StartRecordIfNeeded();
                if (*next == Config->RecordSeparator) {
                    if (!CurrentToken.empty()) {
                        ythrow yexception() <<
                            Sprintf("Key %s must be followed by value", ~CurrentToken);
                    }
                    // TODO(panin): extract method
                    Consumer->OnEndMap();
                    NewRecordStarted = false;
                    State = GetStartState();
                } else {
                    YCHECK(*next == Config->KeyValueSeparator);
                    StartRecordIfNeeded();
                    Consumer->OnKeyedItem(CurrentToken);
                    CurrentToken.clear();
                    State = EState::InsideValue;
                }
                ++next;
            }
            return next;
        }
        case EState::InsideValue: {
            auto next = std::find_first_of(
                begin, end,
                ValueStopSymbols, ValueStopSymbols + ARRAY_SIZE(ValueStopSymbols));
            CurrentToken.append(begin, next);
            if (next != end && *next != Config->EscapingSymbol) {
                Consumer->OnStringScalar(CurrentToken);
                CurrentToken.clear();
                if (*next == Config->RecordSeparator) {
                    Consumer->OnEndMap();
                    NewRecordStarted = false;
                    State = GetStartState();
                } else {
                    State = EState::InsideKey;
                }
                ++next;
            }
            return next;
        }
        default:
            YUNREACHABLE();
    }
}

const char* TDsvParser::FindEndOfValue(const char* begin, const char* end)
{
    auto current = begin;
    for ( ; current != end; ++current) {
        if (*current == Config->FieldSeparator || *current == Config->RecordSeparator) {
            return current;
        }
    }
    return end;
}

TDsvParser::EState TDsvParser::GetStartState()
{
    if (Config->LinePrefix) {
        return EState::InsidePrefix;
    } else {
        return EState::InsideKey;
    }
}

void TDsvParser::ValidatePrefix(const Stroka& prefix)
{
    if (prefix != Config->LinePrefix.Get()) {
        ythrow yexception() <<
            Sprintf("Expected %s at the beginning of line, found %s",
                ~Config->LinePrefix.Get().Quote(),
                ~prefix.Quote());
    }
}

////////////////////////////////////////////////////////////////////////////////

const size_t ParseChunkSize = 1 << 16;

void ParseDsv(TInputStream* input, IYsonConsumer* consumer, TDsvFormatConfigPtr config)
{
    TDsvParser parser(consumer, config);
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

void ParseDsv(const TStringBuf& data,
    IYsonConsumer* consumer,
    TDsvFormatConfigPtr config)
{
    TDsvParser parser(consumer, config);
    parser.Read(data);
    parser.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
