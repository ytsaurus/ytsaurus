#include "yamr_base_parser.h"

#include <core/misc/error.h>
#include <core/misc/string.h>
#include <core/misc/format.h>

#include <core/yson/consumer.h>

#include <ytlib/table_client/public.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYamrConsumerBase::TYamrConsumerBase(NYson::IYsonConsumer* consumer)
    : Consumer(consumer)
{ }

void TYamrConsumerBase::SwitchTable(i64 tableIndex)
{
    static const Stroka key = FormatEnum(NTableClient::EControlAttribute(
        NTableClient::EControlAttribute::TableIndex));
    Consumer->OnListItem();
    Consumer->OnBeginAttributes();
    Consumer->OnKeyedItem(key);
    Consumer->OnInt64Scalar(tableIndex);
    Consumer->OnEndAttributes();
    Consumer->OnEntity();
}

////////////////////////////////////////////////////////////////////////////////

TYamrDelimitedBaseParser::TYamrDelimitedBaseParser(
    IYamrConsumerPtr consumer,
    bool hasSubkey,
    char fieldSeparator,
    char recordSeparator,
    bool enableKeyEscaping,
    bool enableValueEscaping,
    char escapingSymbol)
    : Consumer(consumer)
    , FieldSeparator(fieldSeparator)
    , RecordSeparator(recordSeparator)
    , EscapingSymbol(escapingSymbol)
    , ExpectingEscapedChar(false)
    , HasSubkey(hasSubkey)
    , Offset(0)
    , Record(1)
    , BufferPosition(0)
    , Table(
        fieldSeparator,
        recordSeparator,
        enableKeyEscaping,
        enableValueEscaping,
        escapingSymbol,
        false)
{ }

void TYamrDelimitedBaseParser::Read(const TStringBuf& data)
{
    auto current = data.begin();
    auto end = data.end();
    while (current != end) {
        current = Consume(current, end);
    }
}

void TYamrDelimitedBaseParser::Finish()
{
    if (ExpectingEscapedChar) {
        ThrowIncorrectFormat();
    }
    if (State == EState::InsideKey && !CurrentToken.empty()) {
        ThrowIncorrectFormat();
    }
    if (State == EState::InsideSubkey) {
        ProcessSubkey(CurrentToken);
        ProcessValue("");
    }
    if (State == EState::InsideValue) {
        ProcessValue(CurrentToken);
    }
}

Stroka TYamrDelimitedBaseParser::GetDebugInfo() const
{
    Stroka context;
    const char* last = ContextBuffer + BufferPosition;
    if (Offset >= ContextBufferSize) {
        context.append(last, ContextBuffer + ContextBufferSize);
    }
    context.append(ContextBuffer, last);
    return Format("Offset: %v, Record: %v, State: %v, Context: %Qv",
            Offset,
            Record,
            State,
            context);
}

void TYamrDelimitedBaseParser::ProcessTableSwitch(const TStringBuf& tableIndex)
{
    YASSERT(!ExpectingEscapedChar);
    YASSERT(State == EState::InsideKey);
    i64 value;
    try {
         value = FromString<i64>(tableIndex);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Invalid output table switch in YAMR: %Qv", tableIndex);
    }
    Consumer->SwitchTable(value);
}

void TYamrDelimitedBaseParser::ProcessKey(const TStringBuf& key)
{
    YASSERT(!ExpectingEscapedChar);
    YASSERT(State == EState::InsideKey);
    Consumer->ConsumeKey(key);
    State = HasSubkey ? EState::InsideSubkey : EState::InsideValue;
}

void TYamrDelimitedBaseParser::ProcessSubkey(const TStringBuf& subkey)
{
    YASSERT(!ExpectingEscapedChar);
    YASSERT(State == EState::InsideSubkey);
    Consumer->ConsumeSubkey(subkey);
    State = EState::InsideValue;
}

void TYamrDelimitedBaseParser::ProcessSubkeyBadFormat(const TStringBuf& subkey)
{
    YASSERT(!ExpectingEscapedChar);
    YASSERT(State == EState::InsideSubkey);
    Consumer->ConsumeSubkey(subkey);
    Consumer->ConsumeValue("");
    State = EState::InsideKey;
}

void TYamrDelimitedBaseParser::ProcessValue(const TStringBuf& value)
{
    YASSERT(!ExpectingEscapedChar);
    YASSERT(State == EState::InsideValue);
    Consumer->ConsumeValue(value);
    State = EState::InsideKey;
    Record += 1;
}

const char* TYamrDelimitedBaseParser::ProcessToken(
    void (TYamrDelimitedBaseParser::*processor)(const TStringBuf& value),
    const char* begin,
    const char* next)
{
    if (CurrentToken.empty()) {
        (this->*processor)(TStringBuf(begin, next));
    } else {
        CurrentToken.append(begin, next);
        (this->*processor)(CurrentToken);
        CurrentToken.clear();
    }

    OnRangeConsumed(next, next + 1);
    return next + 1;
}

const char* TYamrDelimitedBaseParser::FindNext(const char* begin, const char* end, const TLookupTable& lookupTable)
{
    const char* next = lookupTable.FindNext(begin, end);
    OnRangeConsumed(begin, next);
    return next;
}

const char* TYamrDelimitedBaseParser::Consume(const char* begin, const char* end)
{
    if (ExpectingEscapedChar) {
        // Read and unescape.
        CurrentToken.append(Table.Escapes.Backward[static_cast<ui8>(*begin)]);
        ExpectingEscapedChar = false;
        OnRangeConsumed(begin, begin + 1);
        return begin + 1;
    }

    YASSERT(!ExpectingEscapedChar);

    const char* next = FindNext(begin, end, State == EState::InsideValue ? Table.ValueStops : Table.KeyStops);
    if (next == end) {
        CurrentToken.append(begin, next);
        if (CurrentToken.length() > NTableClient::MaxRowWeightLimit) {
            THROW_ERROR_EXCEPTION(
                "Current token is too large (%d > %" PRId64 ")",
                CurrentToken.length(),
                NTableClient::MaxRowWeightLimit);
        }
        return end;
    }

    if (*next == EscapingSymbol) {
        CurrentToken.append(begin, next);
        OnRangeConsumed(next, next + 1);
        ExpectingEscapedChar = true;
        return next + 1;
    }

    switch (State) {
    case EState::InsideKey:
        if (*next == RecordSeparator) {
            return ProcessToken(&TYamrDelimitedBaseParser::ProcessTableSwitch, begin, next);
        }

        if (*next == FieldSeparator) {
            return ProcessToken(&TYamrDelimitedBaseParser::ProcessKey, begin, next);
        }
        break;

    case EState::InsideSubkey:
        if (*next == FieldSeparator) {
            return ProcessToken(&TYamrDelimitedBaseParser::ProcessSubkey, begin, next);
        }

        if (*next == RecordSeparator) {
            // Look yamr_parser_yt.cpp: IncompleteRows() for details.
            return ProcessToken(&TYamrDelimitedBaseParser::ProcessSubkeyBadFormat, begin, next);
        }
        break;

    case EState::InsideValue:
        if (*next == RecordSeparator) {
            return ProcessToken(&TYamrDelimitedBaseParser::ProcessValue, begin, next);
        }
        break;

    };

    ThrowIncorrectFormat();
    // To supress warnings.
    YUNREACHABLE();
}

void TYamrDelimitedBaseParser::ThrowIncorrectFormat() const
{
    THROW_ERROR_EXCEPTION("Unexpected symbol in YAMR row: expected %s, found %s (%s)",
        ~Stroka(FieldSeparator).Quote(),
        ~Stroka(RecordSeparator).Quote(),
        ~GetDebugInfo());
}

void TYamrDelimitedBaseParser::OnRangeConsumed(const char* begin, const char* end)
{
    Offset += end - begin;
    auto current = std::max(begin, end - ContextBufferSize);
    for ( ; current < end; ++current) {
        AppendToContextBuffer(*current);
    }
}

void TYamrDelimitedBaseParser::AppendToContextBuffer(char symbol)
{
    ContextBuffer[BufferPosition] = symbol;
    ++BufferPosition;
    if (BufferPosition >= ContextBufferSize) {
        BufferPosition -= ContextBufferSize;
    }
}


////////////////////////////////////////////////////////////////////////////////

TYamrLenvalBaseParser::TYamrLenvalBaseParser(
    IYamrConsumerPtr consumer,
    bool hasSubkey)
        : Consumer(consumer)
        , HasSubkey(hasSubkey)
        , ReadingLength(true)
        , BytesToRead(4)
        , State(EState::InsideKey)
{ }

void TYamrLenvalBaseParser::Read(const TStringBuf& data)
{
    auto current = data.begin();
    while (current != data.end()) {
        current = Consume(current, data.end());
    }
}

void TYamrLenvalBaseParser::Finish()
{
    if (State == EState::InsideValue && !ReadingLength && BytesToRead == 0) {
        Consumer->ConsumeValue(CurrentToken);
        return;
    }

    if (!(State == EState::InsideKey && ReadingLength && BytesToRead == 4)) {
        THROW_ERROR_EXCEPTION("Premature end of stream");
    }
}

const char* TYamrLenvalBaseParser::Consume(const char* begin, const char* end)
{
    if (ReadingLength) {
        return ConsumeLength(begin, end);
    } else {
        return ConsumeData(begin, end);
    }
}

const char* TYamrLenvalBaseParser::ConsumeInt(const char* begin, const char* end)
{
    const char* current = begin;
    while (BytesToRead != 0 && current != end) {
        Union.Bytes[4 - BytesToRead] = *current;
        ++current;
        --BytesToRead;
    }
    return current;
}

const char* TYamrLenvalBaseParser::ConsumeLength(const char* begin, const char* end)
{
    YASSERT(ReadingLength);
    const char* next = ConsumeInt(begin, end);

    if (BytesToRead == 0) {
        ReadingLength = false;
        BytesToRead = Union.Value;
    }

    if (BytesToRead == static_cast<ui32>(-1)) {
        if (State == EState::InsideKey) {
            BytesToRead = 4;
            State = EState::InsideTableSwitch;
        } else {
            THROW_ERROR_EXCEPTION("Unexpected table switch instruction (State: %v)", State);
        }
    }

    if (BytesToRead > NTableClient::MaxRowWeightLimit) {
        THROW_ERROR_EXCEPTION(
            "Lenval length is too large (%v > %v)",
            BytesToRead,
            NTableClient::MaxRowWeightLimit);
    }

    return next;
}

const char* TYamrLenvalBaseParser::ConsumeData(const char* begin, const char* end)
{
    if (State == EState::InsideTableSwitch) {
        YASSERT(CurrentToken.empty());
        const char* next = ConsumeInt(begin, end);

        if (BytesToRead == 0) {
            Consumer->SwitchTable(static_cast<i64>(Union.Value));
            State = EState::InsideKey;
            ReadingLength = true;
            BytesToRead = 4;
        }

        return next;
    }

    // Consume ordinary string tokens.
    TStringBuf data;
    const char* current = begin + BytesToRead;

    if (current > end) {
        CurrentToken.append(begin, end);
        BytesToRead -= (end - begin);
        YASSERT(BytesToRead > 0);
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
            Consumer->ConsumeKey(data);
            State = HasSubkey ? EState::InsideSubkey : EState::InsideValue;
            break;
        case EState::InsideSubkey:
            Consumer->ConsumeSubkey(data);
            State = EState::InsideValue;
            break;
        case EState::InsideValue:
            Consumer->ConsumeValue(data);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
