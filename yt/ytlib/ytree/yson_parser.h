#pragma once

#include "common.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser
{
public:
    TYsonParser(IYsonEvents* events)
        : Events(events)
    {
        Reset();
    }

    void Parse(TInputStream* stream)
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

private:
    static const int Eos = -1;
    static const int NoLookahead = -2;

    IYsonEvents* Events;
    TInputStream* Stream;
    int Lookahead;

    void Reset()
    {
        Stream = NULL;
        Lookahead = NoLookahead;
    }

    int ReadChar()
    {
        if (Lookahead == NoLookahead) {
            PeekChar();
        }

        int result = Lookahead;
        Lookahead = NoLookahead;
        return result;
    }

    void ExpectChar(char expectedCh)
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

    int PeekChar()
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

    static bool IsWhitespace(int ch)
    {
        return
            ch == '\n' ||
            ch == '\r' ||
            ch == '\t' ||
            ch == ' ';
    }

    void SkipWhitespaces()
    {
        while (IsWhitespace(PeekChar())) {
            ReadChar();
        }
    }

    Stroka ReadString()
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

    Stroka ReadNumericLike()
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

    void ParseAny()
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

    void ParseAttributesItem()
    {
        SkipWhitespaces();
        Stroka name = ReadString();
        if (name.Empty()) {
            // TODO:
            ythrow yexception() << Sprintf("Empty attribute name in YSON");
        }
        SkipWhitespaces();
        ExpectChar(':');
        Events->AttributesItem(name);
        ParseAny();
    }

    void ParseAttributes()
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
            ExpectChar(',');
        }
        YVERIFY(ReadChar() == '>');
        Events->EndAttributes();
    }

    void ParseListItem(int index)
    {
        Events->ListItem(index);
        ParseAny();
    }

    void ParseList()
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
            ExpectChar(',');
        }
        YVERIFY(ReadChar() == ']');
        Events->EndList();
    }

    void ParseMapItem()
    {
        SkipWhitespaces();
        Stroka name = ReadString();
        if (name.Empty()) {
            // TODO:
            ythrow yexception() << Sprintf("Empty map item name in YSON");
        }
        SkipWhitespaces();
        ExpectChar(':');
        Events->MapItem(name);
        ParseAny();
    }

    void ParseMap()
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
            ExpectChar(',');
        }
        YVERIFY(ReadChar() == '}');
        Events->EndMap();
    }

    void ParseEntity()
    {
        Events->EntityValue();
    }

    void ParseString()
    {
        Stroka value = ReadString();
        Events->StringValue(value);
    }

    static bool IsIntegerLike(const Stroka& str)
    {
        for (int i = 0; i < static_cast<int>(str.length()); ++i) {
            char ch = str[i];
            if (ch == '.' || ch == 'e' || ch == 'E')
                return false;
        }
        return true;
    }

    void ParseNumeric()
    {
        Stroka str = ReadNumericLike();
        if (IsIntegerLike(str)) {
            try {
                i64 value = FromString<i64>(str);
                Events->Int64Value(value);
            } catch (...) {
                // TODO:
                ythrow yexception() << Sprintf("Failed to parse \"Int64\" literal %s in YSON",
                    ~str.Quote());
            }
        } else {
            try {
                double value = FromString<double>(str);
                Events->DoubleValue(value);
            } catch (...) {
                // TODO:
                ythrow yexception() << Sprintf("Failed to parse \"Double\" literal %s in YSON",
                    ~str.Quote());
            }
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

