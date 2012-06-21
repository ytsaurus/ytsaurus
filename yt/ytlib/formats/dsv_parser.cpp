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
    , Record(1)
    , Field(1)
{
    if (!Config) {
        Config = New<TDsvFormatConfig>();
    }
    State = GetStartState();

    // TODO(panin): unite with next
    memset(IsKeyStopSymbol, 0, sizeof(IsKeyStopSymbol));
    IsKeyStopSymbol[Config->EscapingSymbol] = true;
    IsKeyStopSymbol[Config->KeyValueSeparator] = true;
    IsKeyStopSymbol[Config->RecordSeparator] = true;

    memset(IsValueStopSymbol, 0, sizeof(IsValueStopSymbol));
    IsValueStopSymbol[Config->EscapingSymbol] = true;
    IsValueStopSymbol[Config->FieldSeparator] = true;
    IsValueStopSymbol[Config->RecordSeparator] = true;
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
                ythrow yexception() <<
                    Sprintf("Key %s must be followed by a value (%s)",
                        ~CurrentToken.Quote(),
                        ~GetPositionInfo());
            }
            if (NewRecordStarted) {
                Consumer->OnEndMap();
            }
            break;
        default:
            YUNREACHABLE();
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
            auto next = FindEndOfValue(begin, end);
            CurrentToken.append(begin, next);
            if (next != end && *next != Config->EscapingSymbol) {
                StartRecordIfNeeded();
                ValidatePrefix(CurrentToken);
                CurrentToken.clear();
                if (*next == Config->RecordSeparator) {
                    EndRecord();
                } else {
                    EndField();
                }
                ++next;
            }
            return next;
        }
        case EState::InsideKey: {
            auto next = FindEndOfKey(begin, end);
            CurrentToken.append(begin, next);
            if (next != end && *next != Config->EscapingSymbol) {
                StartRecordIfNeeded();
                if (*next == Config->RecordSeparator) {
                    if (!CurrentToken.empty()) {
                        ythrow yexception() <<
                            Sprintf("Key %s must be followed by a value (%s)",
                                ~CurrentToken.Quote(),
                                ~GetPositionInfo());
                    }
                    EndRecord();
                } else {
                    YCHECK(*next == Config->KeyValueSeparator);
                    Consumer->OnKeyedItem(CurrentToken);
                    CurrentToken.clear();
                    State = EState::InsideValue;
                }
                ++next;
            }
            return next;
        }
        case EState::InsideValue: {
            auto next = FindEndOfValue(begin, end);
            CurrentToken.append(begin, next);
            if (next != end && *next != Config->EscapingSymbol) {
                Consumer->OnStringScalar(CurrentToken);
                CurrentToken.clear();
                if (*next == Config->RecordSeparator) {
                    EndRecord();
                } else {
                    EndField();
                }
                ++next;
            }
            return next;
        }
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

void TDsvParser::EndRecord()
{
    Consumer->OnEndMap();
    NewRecordStarted = false;
    State = GetStartState();

    ++Record;
    Field = 1;
}

void TDsvParser::EndField()
{
    State = EState::InsideKey;
    ++Field;
}


const char* TDsvParser::FindEndOfValue(const char* begin, const char* end)
{
    auto current = begin;
    for ( ; current != end; ++current) {
        if (IsValueStopSymbol[*current]) {
            return current;
        }
    }
    return end;
}

const char* TDsvParser::FindEndOfKey(const char* begin, const char* end)
{
    auto current = begin;
    for ( ; current != end; ++current) {
        if (IsKeyStopSymbol[*current]) {
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
            Sprintf("Expected %s at the beginning of record, found %s (%s)",
                ~Config->LinePrefix.Get().Quote(),
                ~prefix.Quote(),
                ~GetPositionInfo());
    }
}

Stroka TDsvParser::GetPositionInfo() const
{
    TStringStream stream;
    stream << "Record: " << Record;
    stream << ", Field: " << Field;
    return stream.Str();
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
