#pragma once

#include "common.h"
#include "yson_events.h"


namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonReaderBase
    : private TNonCopyable
{
protected:
    TYsonReaderBase(IYsonConsumer* consumer, TInputStream* stream);

    static const int Eos = -1;
    static const int NoLookahead = -2;

    IYsonConsumer* Consumer;
    TInputStream* Stream;
    int Lookahead;

    int LineIndex;
    int Position;
    int Offset;
    Stroka GetPositionInfo();

    void Reset();

    int ReadChar(bool binaryInput = false);
    Stroka ReadChars(int charCount, bool binaryInput = false);

    void ExpectChar(char expectedCh);

    int PeekChar();

    static bool IsLetter(int ch);
    static bool IsDigit(int ch);
    static bool IsWhitespace(int ch);
    void SkipWhitespaces();

    Stroka ReadString();
    Stroka ReadQuoteStartingString();
    Stroka ReadLetterStartingString();
    Stroka ReadBinaryString();

    Stroka ReadNumeric();

    void ParseAny();

    bool HasAttributes();
    void ParseAttributesItem();
    void ParseAttributes();

    void ParseListItem();
    void ParseList();

    void ParseMapItem();
    void ParseMap();

    void ParseEntity();

    void ParseString();

    void ParseBinaryInt64();
    void ParseBinaryDouble();

    static bool SeemsInteger(const Stroka& str);
    void ParseNumeric();

};

////////////////////////////////////////////////////////////////////////////////

class TYsonReader
    : public TYsonReaderBase
{
public:
    TYsonReader(IYsonConsumer* consumer, TInputStream* stream)
        : TYsonReaderBase(consumer, stream)
    { }

    void Read();
};

////////////////////////////////////////////////////////////////////////////////

class TYsonFragmentReader
    : public TYsonReaderBase
{
public:
    TYsonFragmentReader(IYsonConsumer* consumer, TInputStream* stream)
        : TYsonReaderBase(consumer, stream)
    { }

    bool HasNext();
    void ReadNext();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

