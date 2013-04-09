#include "yamr_base_parser.h"

#include <ytlib/misc/error.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYamrBaseParser::TYamrBaseParser(
    char fieldSeparator,
    char recordSeparator,
    bool enableKeyEscaping,
    bool enableValueEscaping,
    char escapingSymbol,
    bool hasSubkey,
    bool escapeCarriageReturn)
    : FieldSeparator(fieldSeparator)
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

void TYamrBaseParser::Read(const TStringBuf& data)
{
    auto current = data.begin();
    auto end = data.end();
    while (current != end) {
        current = Consume(current, end);
    }
}

void TYamrBaseParser::Finish()
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

Stroka TYamrBaseParser::GetDebugInfo() const
{
    Stroka context;
    const char* last = ContextBuffer + BufferPosition;
    if (Offset >= BufferSize) {
        context.append(last, ContextBuffer + BufferSize);
    }
    context.append(ContextBuffer, last);
    return Sprintf("Offset: %" PRId64 ", Record: %" PRId64 ", State: %s, Context: %s",
            Offset,
            Record,
            ~State.ToString(),
            ~context.Quote());
}

void TYamrBaseParser::ProcessKey(const TStringBuf& key)
{
    YASSERT(!ExpectingEscapedChar);
    YASSERT(State == EState::InsideKey);
    ConsumeKey(key);
    State = HasSubkey ? EState::InsideSubkey : EState::InsideValue;
}

void TYamrBaseParser::ProcessSubkey(const TStringBuf& subkey)
{
    YASSERT(!ExpectingEscapedChar);
    YASSERT(State == EState::InsideSubkey);
    ConsumeSubkey(subkey);
    State = EState::InsideValue;
}

void TYamrBaseParser::ProcessValue(const TStringBuf& value)
{
    YASSERT(!ExpectingEscapedChar);
    YASSERT(State == EState::InsideValue);
    ConsumeValue(value);
    State = EState::InsideKey;
    Record += 1;
}


const char* TYamrBaseParser::Consume(const char* begin, const char* end)
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

const char* TYamrBaseParser::TryConsumeRecord(const char* begin, const char* end)
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

void TYamrBaseParser::ThrowIncorrectFormat() const
{
    THROW_ERROR_EXCEPTION("Unexpected symbol in YAMR row: expected %s, found: %s (%s)",
        ~Stroka(FieldSeparator).Quote(),
        ~Stroka(RecordSeparator).Quote(),
        ~GetDebugInfo());
}

void TYamrBaseParser::OnRangeConsumed(const char* begin, const char* end)
{
    Offset += end - begin;
    auto current = std::max(begin, end - BufferSize);
    for ( ; current < end; ++current) {
        AppendToContextBuffer(*current);
    }
}

void TYamrBaseParser::AppendToContextBuffer(char symbol)
{
    ContextBuffer[BufferPosition] = symbol;
    ++BufferPosition;
    if (BufferPosition >= BufferSize) {
        BufferPosition -= BufferSize;
    }
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
