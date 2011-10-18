#include "common.h"

#include "yson_reader.h"
#include "yson_format.h"

#include "../misc/assert.h"
#include "../misc/serialize.h"
#include "../actions/action_util.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TYsonReader::TYsonReader(IYsonConsumer* events)
    : Events(events)
{
    Reset();
}

void TYsonReader::Read(TInputStream* stream)
{
    try {
        Stream = stream;
        ParseAny();
        int ch = ReadChar();
        if (ch != Eos) {
            ythrow yexception() << Sprintf("Unexpected symbol %s in YSON %s",
                ~Stroka(static_cast<char>(ch)).Quote(),
                ~GetPositionInfo());
        }
    } catch (...) {
        Reset();
        throw;
    }
}

Stroka TYsonReader::GetPositionInfo()
{
    return Sprintf("(Line: %d, Position: %d, Offset: %d)",
        LineIndex,
        Position,
        Offset);
}

void TYsonReader::Reset()
{
    Stream = NULL;
    Lookahead = NoLookahead;
    LineIndex = 1;
    Position = 1;
    Offset = 0;
}

int TYsonReader::ReadChar(bool binaryInput)
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

Stroka TYsonReader::ReadChars(int charCount, bool binaryInput)
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


void TYsonReader::ExpectChar(char expectedCh)
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

int TYsonReader::PeekChar()
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

bool TYsonReader::IsWhitespace(int ch)
{
    return
        ch == '\n' ||
        ch == '\r' ||
        ch == '\t' ||
        ch == ' ';
}

void TYsonReader::SkipWhitespaces()
{
    while (IsWhitespace(PeekChar())) {
        ReadChar();
    }
}

Stroka TYsonReader::ReadString()
{
    Stroka result;
    if (PeekChar() == '"') {
        YVERIFY(ReadChar() == '"');
        while (true) {
            int ch = ReadChar();
            if (ch == Eos) {
                ythrow yexception() << Sprintf("Premature end-of-stream while parsing string literal in YSON %s",
                    ~GetPositionInfo());
            }
            if (ch == '"')
                break;
            result.append(static_cast<char>(ch));
        }
    } else {
        while (true) {
            int ch = PeekChar();
            if (!(ch >= 'a' && ch <= 'z' ||
                  ch >= 'A' && ch <= 'Z' ||
                  ch == '_' ||
                  ch >= '0' && ch <= '9' && !result.Empty()))
                  break;
            ReadChar();
            result.append(static_cast<char>(ch));
        }
    }
    return result;
}

Stroka TYsonReader::ReadNumeric()
{
    Stroka result;
    while (true) {
        int ch = PeekChar();
        if (!(ch >= '0' && ch <= '9' ||
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

void TYsonReader::ParseAny()
{
    SkipWhitespaces();
    int ch = PeekChar();
    switch (ch) {
        case '[':
            ParseList();
            ParseAttributes();
            break;

        case '{':
            ParseMap();
            ParseAttributes();
            break;

        case '<':
            ParseEntity();
            ParseAttributes();
            break;

        case StringMarker:
            ParseBinaryString();
            ParseAttributes();
            break;

        case Int64Marker:
            ParseBinaryInt64();
            ParseAttributes();
            break;

        case DoubleMarker:
            ParseBinaryDouble();
            ParseAttributes();
            break;

        default:
            if (ch >= '0' && ch <= '9' ||
                ch == '+' ||
                ch == '-')
            {
                ParseNumeric();
                ParseAttributes();
            } else if (ch >= 'a' && ch <= 'z' ||
                       ch >= 'A' && ch <= 'Z' ||
                       ch == '_' ||
                       ch == '"')
            {
                ParseString();
                ParseAttributes();
            } else {
                ythrow yexception() << Sprintf("Unexpected character %s in YSON %s",
                    ~Stroka(static_cast<char>(ch)).Quote(),
                    ~GetPositionInfo());
            }
            break;
    }
}

void TYsonReader::ParseAttributesItem()
{
    SkipWhitespaces();
    Stroka name = ReadString();
    if (name.Empty()) {
        ythrow yexception() << Sprintf("Empty attribute name in YSON %s",
            ~GetPositionInfo());
    }
    SkipWhitespaces();
    ExpectChar(KeyValueSeparator);
    Events->OnAttributesItem(name);
    ParseAny();
}

void TYsonReader::ParseAttributes()
{
    SkipWhitespaces();
    if (PeekChar() != '<')
        return;
    YVERIFY(ReadChar() == '<');
    Events->OnBeginAttributes();
    while (true) {
        SkipWhitespaces();
        if (PeekChar() == '>')
            break;
        ParseAttributesItem();
        if (PeekChar() == '>')
            break;
        ExpectChar(MapItemSeparator);
    }
    YVERIFY(ReadChar() == '>');
    Events->OnEndAttributes();
}

void TYsonReader::ParseListItem(int index)
{
    Events->OnListItem(index);
    ParseAny();
}

void TYsonReader::ParseList()
{
    YVERIFY(ReadChar() == '[');
    Events->OnBeginList();
    for (int index = 0; true; ++index) {
        SkipWhitespaces();
        if (PeekChar() == ']')
            break;
        ParseListItem(index);
        if (PeekChar() == ']')
            break;
        ExpectChar(ListItemSeparator);
    }
    YVERIFY(ReadChar() == ']');
    Events->OnEndList();
}

void TYsonReader::ParseMapItem()
{
    SkipWhitespaces();
    Stroka name = ReadString();
    if (name.Empty()) {
        ythrow yexception() << Sprintf("Empty map item name in YSON %s",
            ~GetPositionInfo());
    }
    SkipWhitespaces();
    ExpectChar(KeyValueSeparator);
    Events->OnMapItem(name);
    ParseAny();
}

void TYsonReader::ParseMap()
{
    YVERIFY(ReadChar() == '{');
    Events->OnBeginMap();
    while (true) {
        SkipWhitespaces();
        if (PeekChar() == '}')
            break;
        ParseMapItem();
        if (PeekChar() == '}')
            break;
        ExpectChar(MapItemSeparator);
    }
    YVERIFY(ReadChar() == '}');
    Events->OnEndMap();
}

void TYsonReader::ParseEntity()
{
    Events->OnEntityScalar();
}

void TYsonReader::ParseString()
{
    Stroka value = ReadString();
    Events->OnStringScalar(value);
}

bool TYsonReader::SeemsInteger(const Stroka& str)
{
    for (int i = 0; i < static_cast<int>(str.length()); ++i) {
        char ch = str[i];
        if (ch == '.' || ch == 'e' || ch == 'E')
            return false;
    }
    return true;
}

void TYsonReader::ParseNumeric()
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
        Events->OnInt64Scalar(value);
    } else {
        double value;
        try {
            value = FromString<double>(str);
        } catch (...) {
            ythrow yexception() << Sprintf("Failed to parse \"Double\" literal %s in YSON %s",
                ~str.Quote(),
                ~GetPositionInfo());
        }
        Events->OnDoubleScalar(value);
    }
}

void TYsonReader::ParseBinaryString()
{
    ExpectChar(StringMarker);
    YASSERT(Lookahead == NoLookahead);

    i32 length;
    int bytesRead = ReadVarInt32(&length, Stream);
    Position += bytesRead;
    Offset += bytesRead;

    Stroka result = ReadChars(length, true);

    Events->OnStringScalar(result);
}

void TYsonReader::ParseBinaryInt64()
{
    ExpectChar(Int64Marker);
    YASSERT(Lookahead == NoLookahead);

    i64 value;
    int bytesRead = ReadVarInt64(&value, Stream);
    Position += bytesRead;
    Offset += bytesRead;

    Events->OnInt64Scalar(value);
}

void TYsonReader::ParseBinaryDouble()
{
    ExpectChar(DoubleMarker);
    YASSERT(Lookahead == NoLookahead);

    double value;
    int bytesToRead = static_cast<int>(sizeof(double));
    Stream->Read(&value, bytesToRead);
    Position += bytesToRead;
    Offset += bytesToRead;

    Events->OnDoubleScalar(value);
}

TYsonProducer::TPtr TYsonReader::GetProducer(TInputStream* stream)
{
    return FromMethod(&TYsonReader::GetProducerThunk, stream);
}

void TYsonReader::GetProducerThunk(IYsonConsumer* consumer, TInputStream* stream)
{
    TYsonReader reader(consumer);
    reader.Read(stream);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
