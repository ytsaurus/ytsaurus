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
        bool newRecordStarted);

    virtual void Read(const TStringBuf& data);
    virtual void Finish();

private:
    NYTree::IYsonConsumer* Consumer;
    TDsvFormatConfigPtr Config;
    bool StartRecord;

    bool NewRecordStarted;
    bool ExpectingEscapedChar;

    Stroka CurrentToken;

    const char* Consume(const char* begin, const char* end);
    const char* FindEndOfValue(const char* begin, const char* end);
    const char* FindEndOfKey(const char* begin, const char* end);

    void StartRecordIfNeeded();
    void EndRecord();
    void EndField();

    void ValidatePrefix(const Stroka& prefix);

    int Record;
    int Field;
    Stroka GetPositionInfo() const;

    DECLARE_ENUM(EState,
        (InsidePrefix)
        (InsideKey)
        (InsideValue)
    );
    EState State;
    EState GetStartState();
};


////////////////////////////////////////////////////////////////////////////////

TDsvParser::TDsvParser(IYsonConsumer* consumer, TDsvFormatConfigPtr config, bool newRecordStarted)
    : Consumer(consumer)
    , Config(config)
    , StartRecord(newRecordStarted)
    , NewRecordStarted(newRecordStarted)
    , ExpectingEscapedChar(false)
    , Record(1)
    , Field(1)
{
    if (!Config) {
        Config = New<TDsvFormatConfig>();
    }
    State = GetStartState();
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
        CurrentToken.append(UnEscapingTable[static_cast<ui8>(*begin)]);
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
                if (*next == Config->KeyValueSeparator) {
                    Consumer->OnKeyedItem(CurrentToken);
                    CurrentToken.clear();
                    State = EState::InsideValue;
                } else {
                    CurrentToken.clear();
                    if (*next == Config->RecordSeparator) {
                        EndRecord();
                    } else {
                        EndField();
                    }
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
        if (IsValueStopSymbol[static_cast<ui8>(*current)]) {
            return current;
        }
    }
    return end;
}

const char* TDsvParser::FindEndOfKey(const char* begin, const char* end)
{
    auto current = begin;
    for ( ; current != end; ++current) {
        if (IsKeyStopSymbol[static_cast<ui8>(*current)]) {
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
    bool newRecordStarted)
{
    if (!config) {
        config = New<TDsvFormatConfig>();
    }
    return new TDsvParser(consumer, config, newRecordStarted);
}

} // namespace NFormats
} // namespace NYT
