#include "yamr_base_parser.h"

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYamrBaseParser::TYamrBaseParser(
    char fieldSeparator,
    char recordSeparator,
    bool hasSubkey)
    : FieldSeparator(fieldSeparator)
    , RecordSeparator(recordSeparator)
    , HasSubkey(hasSubkey)
{
    memset(IsStopSymbol, 0, sizeof(IsStopSymbol));
    IsStopSymbol[RecordSeparator] = true;
    IsStopSymbol[FieldSeparator] = true;
}
    
void TYamrBaseParser::Read(const TStringBuf& data)
{
    auto current = data.begin();
    while (current != data.end()) {
        current = Consume(current, data.end());
    }
}

void TYamrBaseParser::Finish()
{
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

void TYamrBaseParser::ProcessKey(const TStringBuf& key)
{
    YASSERT(State == EState::InsideKey);
    ConsumeKey(key);
    State = HasSubkey ? EState::InsideSubkey : EState::InsideValue;
}

void TYamrBaseParser::ProcessSubkey(const TStringBuf& subkey)
{
    YASSERT(State == EState::InsideSubkey);
    ConsumeSubkey(subkey);
    State = EState::InsideValue;
}

void TYamrBaseParser::ProcessValue(const TStringBuf& value)
{
    YASSERT(State == EState::InsideValue);
    ConsumeValue(value);
    State = EState::InsideKey;
}


const char* TYamrBaseParser::Consume(const char* begin, const char* end)
{
    // Try parse whole record (it usually works faster) 
    if (State == EState::InsideKey && CurrentToken.empty()) {
        const char* next = TryConsumeRecord(begin, end);
        if (next != NULL) {
            return next;
        }
    }

    // There is no whole record yet
    const char* next = FindNextStopSymbol(begin, end, State);
    CurrentToken.append(begin, next);
    if (next == end) {
        return end;
    }
    if (State == EState::InsideKey) {
        if (*next == RecordSeparator) {
            ThrowIncorrectFormat();
        }
        ProcessKey(CurrentToken);
    }
    else if (State == EState::InsideSubkey) {
        YASSERT(HasSubkey);
        ProcessSubkey(CurrentToken);
        if (*next == RecordSeparator) {
            ProcessValue("");
        }
    }
    else { // State == EState::InsideValue
        ProcessValue(CurrentToken);
    }
    CurrentToken = "";
    return next + 1;
}

const char* TYamrBaseParser::FindNextStopSymbol(const char* begin, const char* end, EState currentState)
{
    if (currentState == EState::InsideValue) {
        IsStopSymbol[FieldSeparator] = false;
    }
    else {
        IsStopSymbol[FieldSeparator] = true;
    }
    auto current = begin;
    for ( ; current < end; ++current) {
        if (IsStopSymbol[static_cast<ui8>(*current)]) {
            return current;
        }
    }
    return end;
}

const char* TYamrBaseParser::TryConsumeRecord(const char* begin, const char* end)
{
    const char* endOfKey = FindNextStopSymbol(begin, end, EState::InsideKey);
    const char* endOfSubkey = 
        HasSubkey ?
        FindNextStopSymbol(endOfKey + 1, end, EState::InsideSubkey) :
        endOfKey;
    const char* endOfValue = FindNextStopSymbol(endOfSubkey + 1, end, EState::InsideValue);
    if (endOfValue == end) { // There is no whole record yet.
        return NULL;
    }
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

    return endOfValue + 1;
}
    
void TYamrBaseParser::ThrowIncorrectFormat() const
{
    ythrow yexception() <<
        "Unexpected eoln symbol during parsing yamr record. "
        "You may forget output value.";
}


////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
