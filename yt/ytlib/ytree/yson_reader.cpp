#include "stdafx.h"
#include "common.h"

#include "yson_reader.h"
#include "yson_format.h"

#include <ytlib/misc/assert.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/actions/action_util.h>

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
              IsDigit(ch) && !result.Empty()))
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

void TYsonReaderBase::ParseAny()
{
    SkipWhitespaces();
    int ch = PeekChar();
    switch (ch) {
        case BeginListSymbol:
            ParseList();
            break;

        case BeginMapSymbol:
            ParseMap();
            break;

        case BeginAttributesSymbol:
            ParseEntity();
            break;

        case StringMarker:
            ParseString();
            break;

        case Int64Marker:
            ParseBinaryInt64();
            break;

        case DoubleMarker:
            ParseBinaryDouble();
            break;

        case Eos:
            ythrow yexception() << Sprintf("Premature end-of-stream in YSON %s",
                ~GetPositionInfo());

        default:
            if (IsDigit(ch) ||
                ch == '+' ||
                ch == '-')
            {
                ParseNumeric();
            } else if (IsLetter(ch) ||
                       ch == '_' ||
                       ch == '"')
            {
                ParseString();
            } else {
                ythrow yexception() << Sprintf("Unexpected character %s in YSON %s",
                    ~Stroka(static_cast<char>(ch)).Quote(),
                    ~GetPositionInfo());
            }
            break;
    }
}

bool TYsonReaderBase::HasAttributes()
{
    SkipWhitespaces();
    return PeekChar() == BeginAttributesSymbol;
}

void TYsonReaderBase::ParseAttributesItem()
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
    ParseAny();
}

void TYsonReaderBase::ParseAttributes()
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
        ParseAttributesItem();
        SkipWhitespaces();
        if (PeekChar() == EndAttributesSymbol)
            break;
        ExpectChar(ItemSeparator);
    }
    YVERIFY(ReadChar() == EndAttributesSymbol);
    Consumer->OnEndAttributes();
}

void TYsonReaderBase::ParseListItem()
{
    Consumer->OnListItem();
    ParseAny();
}

void TYsonReaderBase::ParseList()
{
    YVERIFY(ReadChar() == BeginListSymbol);
    Consumer->OnBeginList();
    while (true) {
        SkipWhitespaces();
        if (PeekChar() == EndListSymbol)
            break;
        ParseListItem();
        SkipWhitespaces();
        if (PeekChar() == EndListSymbol)
            break;
        ExpectChar(ItemSeparator);
    }
    YVERIFY(ReadChar() == EndListSymbol);

    if (HasAttributes()) {
        Consumer->OnEndList(true);
        ParseAttributes();
    } else {
        Consumer->OnEndList(false);
    }
}

void TYsonReaderBase::ParseMapItem()
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
    ParseAny();
}

void TYsonReaderBase::ParseMap()
{
    YVERIFY(ReadChar() == BeginMapSymbol);
    Consumer->OnBeginMap();
    while (true) {
        SkipWhitespaces();
        if (PeekChar() == EndMapSymbol)
            break;
        ParseMapItem();
        SkipWhitespaces();
        if (PeekChar() == EndMapSymbol)
            break;
        ExpectChar(ItemSeparator);
    }
    YVERIFY(ReadChar() == EndMapSymbol);

    if (HasAttributes()) {
        Consumer->OnEndMap(true);
        ParseAttributes();
    } else {
        Consumer->OnEndMap(false);
    }
}

void TYsonReaderBase::ParseEntity()
{
    Consumer->OnEntity(true);
    ParseAttributes();
}

void TYsonReaderBase::ParseString()
{
    Stroka value = ReadString();
    if (HasAttributes()) {
        Consumer->OnStringScalar(value, true);
        ParseAttributes();
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

void TYsonReaderBase::ParseNumeric()
{
    Stroka str = ReadNumeric();
    if (SeemsInteger(str)) {
        i64 value;
        try {
            value = FromString<i64>(str);
        } catch (...) {
            ythrow yexception() << Sprintf("Failed to parse \"Int64\" literal %s in YSON %s",
                ~str.Quote(),
                ~GetPositionInfo());
        }

        if (HasAttributes()) {
            Consumer->OnInt64Scalar(value, true);
            ParseAttributes();
        } else {
            Consumer->OnInt64Scalar(value, false);
        }
    } else {
        double value;
        try {
            value = FromString<double>(str);
        } catch (...) {
            ythrow yexception() << Sprintf("Failed to parse \"Double\" literal %s in YSON %s",
                ~str.Quote(),
                ~GetPositionInfo());
        }

        if (HasAttributes()) {
            Consumer->OnDoubleScalar(value, true);
            ParseAttributes();
        } else {
            Consumer->OnDoubleScalar(value, false);
        }
    }
}

void TYsonReaderBase::ParseBinaryInt64()
{
    ExpectChar(Int64Marker);
    YASSERT(Lookahead == NoLookahead);

    i64 value;
    int bytesRead = ReadVarInt64(Stream, &value);
    Position += bytesRead;
    Offset += bytesRead;

    if (HasAttributes()) {
        Consumer->OnInt64Scalar(value, true);
        ParseAttributes();
    } else {
        Consumer->OnInt64Scalar(value, false);
    }
}

void TYsonReaderBase::ParseBinaryDouble()
{
    ExpectChar(DoubleMarker);
    YASSERT(Lookahead == NoLookahead);

    double value;
    int bytesToRead = static_cast<int>(sizeof(double));
    Stream->Read(&value, bytesToRead);
    Position += bytesToRead;
    Offset += bytesToRead;

    if (HasAttributes()) {
        Consumer->OnDoubleScalar(value, true);
        ParseAttributes();
    } else {
        Consumer->OnDoubleScalar(value, false);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TYsonReader::Read()
{
    ParseAny();
    int ch = ReadChar();
    if (ch != Eos) {
        ythrow yexception() << Sprintf("Unexpected symbol %s while expecting end-of-file in YSON %s",
            ~Stroka(static_cast<char>(ch)).Quote(),
            ~GetPositionInfo());
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TYsonFragmentReader::HasNext()
{
    SkipWhitespaces();
    return PeekChar() != Eos;
}

void TYsonFragmentReader::ReadNext()
{
    ParseAny();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
