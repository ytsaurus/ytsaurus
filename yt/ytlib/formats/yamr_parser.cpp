#include "stdafx.h"
#include "yamr_parser.h"

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYamrParser::TYamrParser(IYsonConsumer* consumer, TYamrFormatConfigPtr config)
    : Consumer(consumer)
    , Config(config)
    , State(EState::InsideKey)
{
    if (!Config) {
        Config = New<TYamrFormatConfig>();
    }

    if (Config->Lenval) {
        YUNIMPLEMENTED();
    }

    memset(IsStopSymbol, 0, sizeof(IsStopSymbol));
    IsStopSymbol[Config->RowSeparator] = true;
    IsStopSymbol[Config->FieldSeparator] = true;
}

void TYamrParser::Read(const TStringBuf& data)
{
    auto current = data.begin();
    while (current != data.end()) {
        current = Consume(current, data.end());
    }
}

void TYamrParser::Finish()
{
    if (State == EState::InsideValue) {
        Consumer->OnKeyedItem(Config->Value);
        Consumer->OnStringScalar(CurrentToken);
        Consumer->OnEndMap();
        CurrentToken.clear();
    }
}

const char* TYamrParser::Consume(const char* begin, const char* end)
{
    switch (State) {
        case EState::InsideKey:
        case EState::InsideSubkey: {
            const char* next = FindNextStopSymbol(begin, end);
            CurrentToken.append(begin, next);
            if (next != end) {
                if (*next == Config->RowSeparator) {
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
            const char* next = FindEndOfRow(begin, end);
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

const char* TYamrParser::FindNextStopSymbol(const char* begin, const char* end)
{
    auto current = begin;
    for ( ; current != end; ++current) {
        if (IsStopSymbol[*current]) {
            return current;
        }
    }
    return end;
}

const char* TYamrParser::FindEndOfRow(const char* begin, const char* end)
{
    auto current = begin;
    for ( ; current != end; ++current) {
        if (*current == Config->RowSeparator) {
            return current;
        }
    }
    return end;
}

////////////////////////////////////////////////////////////////////////////////

// TODO(panin): extract method
const size_t ParseChunkSize = 1 << 16;

void ParseYamr(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{
    TYamrParser parser(consumer, config);
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

void ParseYamr(
    const TStringBuf& data,
    NYTree::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{
    TYamrParser parser(consumer, config);
    parser.Read(data);
    parser.Finish();
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
