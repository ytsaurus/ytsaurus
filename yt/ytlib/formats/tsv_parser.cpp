#include "stdafx.h"
#include "tsv_parser.h"

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTsvParser::TTsvParser(IYsonConsumer* consumer, TTsvFormatConfigPtr config)
    : Consumer(consumer)
    , Config(config)
    , FirstSymbol(true)
    , State(EState::InsideKey)
{
    if (!Config) {
        Config = New<TTsvFormatConfig>();
    }
}

void TTsvParser::Read(const TStringBuf& data)
{
    auto current = data.begin();
    while (current != data.end()) {
        current = Consume(current, data.end());
    }
}

void TTsvParser::Finish()
{
    if (State == EState::InsideValue) {
        Consumer->OnStringScalar(CurrentToken);
        Consumer->OnEndMap();
    }
}

const char* TTsvParser::Consume(const char* begin, const char* end)
{
    switch (State) {
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
                State = EState::InsideKey;
                if (*next == Config->NewLineSeparator) {
                    Consumer->OnEndMap();
                    FirstSymbol = true;
                }
                ++next;
            }
            return next;
        }
        default:
            YUNREACHABLE();
    }
}

const char* TTsvParser::FindEndOfValue(const char* begin, const char* end)
{
    auto current = begin;
    for ( ; current != end; ++current) {
        if (*current == Config->ItemSeparator || *current == Config->NewLineSeparator) {
            return current;
        }
    }
    return end;
}

////////////////////////////////////////////////////////////////////////////////

const size_t ParseChunkSize = 1 << 16;

void ParseTsv(TInputStream* input, IYsonConsumer* consumer, TTsvFormatConfigPtr config)
{
    TTsvParser parser(consumer, config);
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

void ParseTsv(const TStringBuf& data,
    IYsonConsumer* consumer,
    TTsvFormatConfigPtr config)
{
    TTsvParser parser(consumer, config);
    parser.Read(data);
    parser.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
