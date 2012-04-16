#include "stdafx.h"
#include "yson_consumer.h"
#include "yson_reader.h"
#include "yson_format.h"

#include <ytlib/misc/serialize.h>

#include <util/string/escape.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TYsonReaderBase::TYsonReaderBase(IYsonConsumer* consumer, TInputStream* stream)
    : Consumer(consumer)
    , Stream(stream)
{
    YASSERT(consumer);
    YASSERT(stream);
    Reset();
}

Stroka TYsonReaderBase::GetPositionInfo()
{
    return Sprintf("(Line: %d, Position: %d, Offset: %d)",
        LineIndex,
        Position,
        Offset);
}

void TYsonReaderBase::Reset()
{
    Lookahead = NoLookahead;
    LineIndex = 1;
    Position = 1;
    Offset = 0;
}

int TYsonReaderBase::ReadChar(bool binaryInput)
{
    if (Lookahead == NoLookahead) {
        PeekChar();
    }

    int result = Lookahead;
    Lookahead = NoLookahead;

    ++Offset;
    if (!binaryInput && result == '\n') {
        ++LineIndex;
        Position = 1;
    } else {
        ++Position;
    }

    return result;
}

Stroka TYsonReaderBase::ReadChars(int charCount, bool binaryInput)
{
    Stroka result;
    result.reserve(charCount);
    for (int i = 0; i < charCount; ++i) {
        int ch = ReadChar(binaryInput);
        if (ch == Eos) {
            ythrow yexception() << Sprintf("Premature end-of-stream while reading byte %d out of %d %s",
                i + 1,
                charCount,
                ~GetPositionInfo());
        }
        result.push_back(static_cast<char>(ch));
    }
    return result;
}

void TYsonReaderBase::ExpectChar(char expectedCh)
{
    int readCh = ReadChar();
    if (readCh == Eos) {
        ythrow yexception() << Sprintf("Premature end-of-stream expecting %s in YSON %s",
            ~Stroka(expectedCh).Quote(),
            ~GetPositionInfo());
    }
    if (static_cast<char>(readCh) != expectedCh) {
        ythrow yexception() << Sprintf("Found %s while expecting %s in YSON %s",
            ~Stroka(static_cast<char>(readCh)).Quote(),
            ~Stroka(expectedCh).Quote(),
            ~GetPositionInfo());
    }
}

int TYsonReaderBase::PeekChar()
{
    if (Lookahead != NoLookahead) {
        return Lookahead;
    }

    char ch;
    if (Stream->ReadChar(ch)) {
        Lookahead = ch;
    } else {
        Lookahead = Eos;
    }

    return Lookahead;
}

bool TYsonReaderBase::IsLetter(int ch)
{
    return (('a' <= ch && ch <= 'z') ||
            ('A' <= ch && ch <= 'Z'));
}

bool TYsonReaderBase::IsDigit(int ch)
{
    return ('0' <= ch && ch <= '9');
}

bool TYsonReaderBase::IsWhitespace(int ch)
{
    return
        ch == '\n' ||
        ch == '\r' ||
        ch == '\t' ||
        ch == ' ';
}

void TYsonReaderBase::SkipWhitespaces()
{
    while (IsWhitespace(PeekChar())) {
        ReadChar();
    }
}

Stroka TYsonReaderBase::ReadString()
{
    int ch = PeekChar();
    switch (ch) {
        case StringMarker:
            return ReadBinaryString();
        case '"':
            return ReadQuoteStartingString();
        case Eos:
            ythrow yexception() << Sprintf("Premature end-of-stream while expecting string literal in YSON %s",
                ~GetPositionInfo());
        default:
            if (!(IsLetter(ch) || ch == '_')) {
                ythrow yexception() << Sprintf("Expecting string literal but found %s in YSON %s",
                ~Stroka(static_cast<char>(ch)).Quote(),
                ~GetPositionInfo());
            }
            return ReadLetterStartingString();
    }
}

Stroka TYsonReaderBase::ReadQuoteStartingString()
{
    ExpectChar('"');

    Stroka result;
    int trailingSlashesCount = 0;
    while (true) {
        int ch = ReadChar();
        if (ch == Eos) {
            ythrow yexception() << Sprintf("Premature end-of-stream while reading string literal in YSON %s",
                ~GetPositionInfo());
        }
        if (ch == '"' && trailingSlashesCount % 2 == 0) {
            break;
        }

        if (ch == '\\') {
            ++trailingSlashesCount;
        } else {
            trailingSlashesCount = 0;
        }

        result.append(static_cast<char>(ch));
    }
    return UnescapeC(result);
}

Stroka TYsonReaderBase::ReadLetterStartingString()
{
    Stroka result;
    while (true) {
        int ch = PeekChar();
        if (!(IsLetter(ch) ||
              ch == '_' ||
              (IsDigit(ch) && !result.Empty())))
              break;
        ReadChar();
        result.append(static_cast<char>(ch));
    }
    return UnescapeC(result);
}

Stroka TYsonReaderBase::ReadBinaryString()
{
    ExpectChar(StringMarker);
    YASSERT(Lookahead == NoLookahead);

    i32 length;
    int bytesRead = ReadVarInt32(Stream, &length);
    Position += bytesRead;
    Offset += bytesRead;

    return ReadChars(length, true);
}

Stroka TYsonReaderBase::ReadNumeric()
{
    Stroka result;
    while (true) {
        int ch = PeekChar();
        if (!(IsDigit(ch) ||
              ch == '+' ||
              ch == '-' ||
              ch == '.' ||
              ch == 'e' || ch == 'E'))
            break;
        ReadChar();
        result.append(static_cast<char>(ch));
    }
    if (result.Empty()) {
        ythrow yexception() << Sprintf("Premature end-of-stream while parsing numeric literal in YSON %s",
            ~GetPositionInfo());
    }
    return result;
}

void TYsonReaderBase::ParseAny(int depth)
{
    SkipWhitespaces();
    int ch = PeekChar();
    switch (ch) {
        case BeginListSymbol:
            ParseList(depth);
            break;

        case BeginMapSymbol:
            ParseMap(depth);
            break;

        case BeginAttributesSymbol:
            ParseEntity(depth);
            break;

        case StringMarker:
            ParseString(depth);
            break;

        case IntegerMarker:
            ParseBinaryInteger(depth);
            break;

        case DoubleMarker:
            ParseBinaryDouble(depth);
            break;

        case Eos:
            ythrow yexception() << Sprintf("Premature end-of-stream in YSON %s",
                ~GetPositionInfo());

        default:
            if (IsDigit(ch) ||
                ch == '+' ||
                ch == '-')
            {
                ParseNumeric(depth);
            } else if (IsLetter(ch) ||
                       ch == '_' ||
                       ch == '"')
            {
                ParseString(depth);
            } else {
                ythrow yexception() << Sprintf("Unexpected character %s in YSON %s",
                    ~Stroka(static_cast<char>(ch)).Quote(),
                    ~GetPositionInfo());
            }
            break;
    }
}

bool TYsonReaderBase::HasAttributes(int depth)
{
    UNUSED(depth);
    SkipWhitespaces();
    return PeekChar() == BeginAttributesSymbol;
}

void TYsonReaderBase::ParseAttributesItem(int depth)
{
    SkipWhitespaces();
    Stroka name = ReadString();
    if (name.Empty()) {
        ythrow yexception() << Sprintf("Empty attribute name in YSON %s",
            ~GetPositionInfo());
    }
    SkipWhitespaces();
    ExpectChar(KeyValueSeparator);
    Consumer->OnAttributesItem(name);
    ParseAny(depth + 1);
}

void TYsonReaderBase::ParseAttributes(int depth)
{
    SkipWhitespaces();
    if (PeekChar() != BeginAttributesSymbol)
        return;
    YVERIFY(ReadChar() == BeginAttributesSymbol);
    Consumer->OnBeginAttributes();
    while (true) {
        SkipWhitespaces();
        if (PeekChar() == EndAttributesSymbol)
            break;
        ParseAttributesItem(depth);
        SkipWhitespaces();
        if (PeekChar() == EndAttributesSymbol)
            break;
        ExpectChar(ItemSeparator);
    }
    YVERIFY(ReadChar() == EndAttributesSymbol);
    Consumer->OnEndAttributes();
}

void TYsonReaderBase::ParseListItem(int depth)
{
    Consumer->OnListItem();
    ParseAny(depth + 1);
}

void TYsonReaderBase::ParseList(int depth)
{
    YVERIFY(ReadChar() == BeginListSymbol);
    Consumer->OnBeginList();
    while (true) {
        SkipWhitespaces();
        if (PeekChar() == EndListSymbol)
            break;
        ParseListItem(depth);
        SkipWhitespaces();
        if (PeekChar() == EndListSymbol)
            break;
        ExpectChar(ItemSeparator);
    }
    YVERIFY(ReadChar() == EndListSymbol);

    if (HasAttributes(depth)) {
        Consumer->OnEndList(true);
        ParseAttributes(depth);
    } else {
        Consumer->OnEndList(false);
    }
}

void TYsonReaderBase::ParseMapItem(int depth)
{
    SkipWhitespaces();
    Stroka name = ReadString();
    if (name.Empty()) {
        ythrow yexception() << Sprintf("Empty map item name in YSON %s",
            ~GetPositionInfo());
    }
    SkipWhitespaces();
    ExpectChar(KeyValueSeparator);
    Consumer->OnMapItem(name);
    ParseAny(depth + 1);
}

void TYsonReaderBase::ParseMap(int depth)
{
    YVERIFY(ReadChar() == BeginMapSymbol);
    Consumer->OnBeginMap();
    while (true) {
        SkipWhitespaces();
        if (PeekChar() == EndMapSymbol)
            break;
        ParseMapItem(depth);
        SkipWhitespaces();
        if (PeekChar() == EndMapSymbol)
            break;
        ExpectChar(ItemSeparator);
    }
    YVERIFY(ReadChar() == EndMapSymbol);

    if (HasAttributes(depth)) {
        Consumer->OnEndMap(true);
        ParseAttributes(depth);
    } else {
        Consumer->OnEndMap(false);
    }
}

void TYsonReaderBase::ParseEntity(int depth)
{
    Consumer->OnEntity(true);
    ParseAttributes(depth);
}

void TYsonReaderBase::ParseString(int depth)
{
    Stroka value = ReadString();
    if (HasAttributes(depth)) {
        Consumer->OnStringScalar(value, true);
        ParseAttributes(depth);
    } else {
        Consumer->OnStringScalar(value, false);
    }
}

bool TYsonReaderBase::SeemsInteger(const Stroka& str)
{
    for (int i = 0; i < static_cast<int>(str.length()); ++i) {
        char ch = str[i];
        if (ch == '.' || ch == 'e' || ch == 'E')
            return false;
    }
    return true;
}

void TYsonReaderBase::ParseNumeric(int depth)
{
    Stroka str = ReadNumeric();
    if (SeemsInteger(str)) {
        i64 value;
        try {
            value = FromString<i64>(str);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Failed to parse \"Integer\" literal %s in YSON %s",
                ~str.Quote(),
                ~GetPositionInfo());
        }

        if (HasAttributes(depth)) {
            Consumer->OnIntegerScalar(value, true);
            ParseAttributes(depth);
        } else {
            Consumer->OnIntegerScalar(value, false);
        }
    } else {
        double value;
        try {
            value = FromString<double>(str);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Failed to parse \"Double\" literal %s in YSON %s",
                ~str.Quote(),
                ~GetPositionInfo());
        }

        if (HasAttributes(depth)) {
            Consumer->OnDoubleScalar(value, true);
            ParseAttributes(depth);
        } else {
            Consumer->OnDoubleScalar(value, false);
        }
    }
}

void TYsonReaderBase::ParseBinaryInteger(int depth)
{
    ExpectChar(IntegerMarker);
    YASSERT(Lookahead == NoLookahead);

    i64 value;
    int bytesRead = ReadVarInt64(Stream, &value);
    Position += bytesRead;
    Offset += bytesRead;

    if (HasAttributes(depth)) {
        Consumer->OnIntegerScalar(value, true);
        ParseAttributes(depth);
    } else {
        Consumer->OnIntegerScalar(value, false);
    }
}

void TYsonReaderBase::ParseBinaryDouble(int depth)
{
    ExpectChar(DoubleMarker);
    YASSERT(Lookahead == NoLookahead);

    double value;
    int bytesToRead = static_cast<int>(sizeof(double));
    Stream->Read(&value, bytesToRead);
    Position += bytesToRead;
    Offset += bytesToRead;

    if (HasAttributes(depth)) {
        Consumer->OnDoubleScalar(value, true);
        ParseAttributes(depth);
    } else {
        Consumer->OnDoubleScalar(value, false);
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonReader::TYsonReader(
    IYsonConsumer* consumer,
    TInputStream* stream )
    : TYsonReaderBase(consumer, stream)
{ }

void TYsonReader::Read()
{
    ParseAny(0);
    int ch = ReadChar();
    if (ch != Eos) {
        ythrow yexception() << Sprintf("Unexpected symbol %s while expecting end-of-file in YSON %s",
            ~Stroka(static_cast<char>(ch)).Quote(),
            ~GetPositionInfo());
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonFragmentReader::TYsonFragmentReader(
    IYsonConsumer* consumer,
    TInputStream* stream)
    : TYsonReaderBase(consumer, stream)
{ }

bool TYsonFragmentReader::HasNext()
{
    SkipWhitespaces();
    return PeekChar() != Eos;
}

void TYsonFragmentReader::ReadNext()
{
    ParseAny(0);
}

bool TYsonFragmentReader::HasAttributes(int depth)
{
    if (depth == 0) {
        return false;
    }
    return TYsonReaderBase::HasAttributes(depth);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
