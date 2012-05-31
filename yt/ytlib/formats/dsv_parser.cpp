#include "stdafx.h"
#include "dsv_parser.h"

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TDsvParser::TDsvParser(IYsonConsumer* consumer, TDsvFormatConfigPtr config)
    : Consumer(consumer)
    , Config(config)
    , FirstSymbol(true)
{
    if (!Config) {
        Config = New<TDsvFormatConfig>();
    }
    State = GetStartState();
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
    if (State == EState::InsideValue) {
        Consumer->OnStringScalar(CurrentToken);
        Consumer->OnEndMap();
    }
}

const char* TDsvParser::Consume(const char* begin, const char* end)
{
    switch (State) {
        case EState::InsidePrefix: {
            if (FirstSymbol) {
                Consumer->OnListItem();
                Consumer->OnBeginMap();
                FirstSymbol = false;
            }
            auto next = FindEndOfValue(begin, end);
            CurrentToken.append(begin, next);
            if (next != end) {
                if (CurrentToken != Config->LinePrefix.Get()) {
                    ythrow yexception() <<
                        Sprintf("Each line should begin with %s", ~Config->LinePrefix.Get());
                }
                CurrentToken.clear();
                if (*next == Config->RecordSeparator) {
                    Consumer->OnEndMap();
                    FirstSymbol = true;
                    State = GetStartState();
                } else {
                    State = EState::InsideKey;
                }
                ++next;
            }
            return next;
        }
        case EState::InsideKey: {
            if (FirstSymbol) {
                Consumer->OnListItem();
                Consumer->OnBeginMap();
                FirstSymbol = false;
            }
            auto next = std::find(begin, end, Config->KeyValueSeparator);
            CurrentToken.append(begin, next);
            if (next != end) {
                YCHECK(*next == Config->KeyValueSeparator);
                Consumer->OnKeyedItem(CurrentToken);
                CurrentToken.clear();
                State = EState::InsideValue;
                ++next;
            }
            return next;
        }
        case EState::InsideValue: {
            auto next = FindEndOfValue(begin, end);
            CurrentToken.append(begin, next);
            if (next != end) {
                Consumer->OnStringScalar(CurrentToken);
                CurrentToken.clear();
                if (*next == Config->RecordSeparator) {
                    Consumer->OnEndMap();
                    FirstSymbol = true;
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
