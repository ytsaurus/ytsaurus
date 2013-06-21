#include "yamr_base_parser.h"

#include <ytlib/misc/error.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYamrDelimitedBaseParser::TYamrDelimitedBaseParser(
    IYamrConsumerPtr consumer,
    bool hasSubkey,
    char fieldSeparator,
    char recordSeparator,
    bool enableKeyEscaping,
    bool enableValueEscaping,
    char escapingSymbol,
    bool escapeCarriageReturn)
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
        escapeCarriageReturn,
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
    return Sprintf("Offset: %" PRId64 ", Record: %" PRId64 ", State: %s, Context: %s",
            Offset,
            Record,
            ~State.ToString(),
            ~context.Quote());
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

void TYamrDelimitedBaseParser::ProcessValue(const TStringBuf& value)
{
    YASSERT(!ExpectingEscapedChar);
    YASSERT(State == EState::InsideValue);
    Consumer->ConsumeValue(value);
    State = EState::InsideKey;
    Record += 1;
}


const char* TYamrDelimitedBaseParser::Consume(const char* begin, const char* end)
{
    // Try parse whole record (it usually works faster)
    if (!ExpectingEscapedChar && State == EState::InsideKey && CurrentToken.empty()) {
        const char* next = TryConsumeRecord(begin, end);
        if (next != NULL) {
            return next;
        }
    }

    // Read symbol if we are expecting escaped symbol
    if (ExpectingEscapedChar) {
        CurrentToken.append(Table.Escapes.Backward[static_cast<ui8>(*begin)]);
        ExpectingEscapedChar = false;
        OnRangeConsumed(begin, begin + 1);
        return begin + 1;
    }

    YASSERT(!ExpectingEscapedChar);

    // There is no whole record yet
    const char* next =
        (State == EState::InsideValue)
        ? Table.ValueStops.FindNext(begin, end)
        : Table.KeyStops.FindNext(begin, end);

    OnRangeConsumed(begin, next);
    CurrentToken.append(begin, next);
    if (next == end) {
        return end;
    }
    OnRangeConsumed(next, next + 1);

    if (*next == EscapingSymbol) {
        ExpectingEscapedChar = true;
        return next + 1;
    }

    if (State == EState::InsideKey) {
        if (*next == RecordSeparator) {
            ThrowIncorrectFormat();
        }
        ProcessKey(CurrentToken);
    } else if (State == EState::InsideSubkey) {
        YASSERT(HasSubkey);
        ProcessSubkey(CurrentToken);
        if (*next == RecordSeparator) {
            ProcessValue("");
        }
    } else { // State == EState::InsideValue
        ProcessValue(CurrentToken);
    }
    CurrentToken.clear();
    return next + 1;
}

const char* TYamrDelimitedBaseParser::TryConsumeRecord(const char* begin, const char* end)
{
    const char* endOfKey = Table.KeyStops.FindNext(begin, end);
    const char* endOfSubkey =
        HasSubkey
        ? Table.KeyStops.FindNext(std::min(endOfKey + 1, end), end)
        : endOfKey;
    const char* endOfValue = Table.ValueStops.FindNext(std::min(endOfSubkey + 1, end), end);

    if (endOfValue == end) { // There is no whole record yet.
        return NULL;
    }

    if (*endOfKey == EscapingSymbol ||
        *endOfSubkey == EscapingSymbol ||
        *endOfValue == EscapingSymbol) // We have escaped symbols, so we need state machine
    {
        return NULL;
    }

    OnRangeConsumed(begin, endOfKey + 1);

    if (*endOfKey == RecordSeparator) { // There is no tabulation in record. It is incorrect case.
        ThrowIncorrectFormat();
    }
    if (*endOfSubkey == RecordSeparator) { // The case of empty value without proper tabulation
        endOfValue = endOfSubkey;
    }

    ProcessKey(TStringBuf(begin, endOfKey));
    if (HasSubkey) {
        ProcessSubkey(TStringBuf(endOfKey + 1, endOfSubkey));
    }
    const char* beginOfValue = std::min(endOfSubkey + 1, endOfValue);
    ProcessValue(TStringBuf(beginOfValue, endOfValue));
    OnRangeConsumed(endOfKey + 1, endOfValue + 1); // consume with separator

    return endOfValue + 1;
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

const char* TYamrLenvalBaseParser::ConsumeLength(const char* begin, const char* end)
{
    const char* current = begin;
    while (BytesToRead != 0 && current != end) {
        if (ReadingLength) {
            Union.Bytes[4 - BytesToRead] = *current;
        }
        ++current;
        --BytesToRead;
    }

    if (!ReadingLength) {
        CurrentToken.append(begin, current);
    }

    if (BytesToRead != 0) {
        return current;
    }

    if (Uniton.Length == static_cast<ui32>(-1)) {
        THROW_ERROR_EXCEPTION("Output table selectors in YAMR stream are not supported");
    }

    if (Union.Length > MaxFieldLength) {
        THROW_ERROR_EXCEPTION("Field is too long: length % " PRId64 ", limit %" PRId64,
            static_cast<i64>(Union.Length),
            MaxFieldLength);
    }

    ReadingLength = false;
    BytesToRead = Union.Length;
    return current;
}

const char* TYamrLenvalBaseParser::ConsumeData(const char* begin, const char* end)
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
