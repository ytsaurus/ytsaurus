#include "common.h"

#include "yson_reader.h"
#include "yson_format.h"

#include "../misc/assert.h"

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
        Events->BeginTree();
        ParseAny();
        int ch = ReadChar();
        if (ch != Eos) {
            ythrow yexception() << Sprintf("Unexpected symbol %s in YSON",
                ~Stroka(static_cast<char>(ch)).Quote());
        }
        Events->EndTree();
    } catch (...) {
        Reset();
        throw;
    }
}

void TYsonReader::Reset()
{
    Stream = NULL;
    Lookahead = NoLookahead;
}

int TYsonReader::ReadChar()
{
    if (Lookahead == NoLookahead) {
        PeekChar();
    }

    int result = Lookahead;
    Lookahead = NoLookahead;
    return result;
}

void TYsonReader::ExpectChar(char expectedCh)
{
    int readCh = ReadChar();
    if (readCh == Eos) {
        // TODO:
        ythrow yexception() << Sprintf("Premature end-of-stream expecting %s in YSON",
            ~Stroka(expectedCh).Quote());
    }
    if (static_cast<char>(readCh) != expectedCh) {
        // TODO:
        ythrow yexception() << Sprintf("Found %s while expecting %s in YSON",
            ~Stroka(static_cast<char>(readCh)).Quote(),
            ~Stroka(expectedCh).Quote());
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
                // TODO:
                ythrow yexception() << Sprintf("Premature end-of-stream while parsing \"String\" literal in YSON");
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

Stroka TYsonReader::ReadNumericLike()
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
        // TODO:
        ythrow yexception() << Sprintf("Premature end-of-stream while parsing \"Numeric\" literal in YSON");
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
                // TODO:
                ythrow yexception() << Sprintf("Unexpected %s in YSON",
                    ~Stroka(static_cast<char>(ch)).Quote());
            }
            break;
    }
}

void TYsonReader::ParseAttributesItem()
{
    SkipWhitespaces();
    Stroka name = ReadString();
    if (name.Empty()) {
        // TODO:
        ythrow yexception() << Sprintf("Empty attribute name in YSON");
    }
    SkipWhitespaces();
    ExpectChar(MapItemSeparator);
    Events->AttributesItem(name);
    ParseAny();
}

void TYsonReader::ParseAttributes()
{
    SkipWhitespaces();
    if (PeekChar() != '<')
        return;
    YVERIFY(ReadChar() == '<');
    Events->BeginAttributes();
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
    Events->EndAttributes();
}

void TYsonReader::ParseListItem(int index)
{
    Events->ListItem(index);
    ParseAny();
}

void TYsonReader::ParseList()
{
    YVERIFY(ReadChar() == '[');
    Events->BeginList();
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
    Events->EndList();
}

void TYsonReader::ParseMapItem()
{
    SkipWhitespaces();
    Stroka name = ReadString();
    if (name.Empty()) {
        // TODO:
        ythrow yexception() << Sprintf("Empty map item name in YSON");
    }
    SkipWhitespaces();
    ExpectChar(KeyValueSeparator);
    Events->MapItem(name);
    ParseAny();
}

void TYsonReader::ParseMap()
{
    YVERIFY(ReadChar() == '{');
    Events->BeginMap();
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
    Events->EndMap();
}

void TYsonReader::ParseEntity()
{
    Events->EntityScalar();
}

void TYsonReader::ParseString()
{
    Stroka value = ReadString();
    Events->StringScalar(value);
}

bool TYsonReader::IsIntegerLike(const Stroka& str)
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
    Stroka str = ReadNumericLike();
    if (IsIntegerLike(str)) {
        try {
            i64 value = FromString<i64>(str);
            Events->Int64Scalar(value);
        } catch (...) {
            // TODO:
            ythrow yexception() << Sprintf("Failed to parse \"Int64\" literal %s in YSON",
                ~str.Quote());
        }
    } else {
        try {
            double value = FromString<double>(str);
            Events->DoubleScalar(value);
        } catch (...) {
            // TODO:
            ythrow yexception() << Sprintf("Failed to parse \"Double\" literal %s in YSON",
                ~str.Quote());
        }
    }
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

