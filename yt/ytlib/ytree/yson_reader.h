#pragma once

#include "public.h"

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

    void ParseAny(int depth);

    virtual bool HasAttributes(int depth);
    void ParseAttributesItem(int depth);
    void ParseAttributes(int depth);

    void ParseListItem(int depth);
    void ParseList(int depth);

    void ParseMapItem(int depth);
    void ParseMap(int depth);

    void ParseEntity(int depth);

    void ParseString(int depth);

    void ParseBinaryInt64(int depth);
    void ParseBinaryDouble(int depth);

    static bool SeemsInteger(const Stroka& str);
    void ParseNumeric(int depth);

};

////////////////////////////////////////////////////////////////////////////////

class TYsonReader
    : public TYsonReaderBase
{
public:
    TYsonReader(IYsonConsumer* consumer, TInputStream* stream);

    void Read();
};

////////////////////////////////////////////////////////////////////////////////

class TYsonFragmentReader
    : public TYsonReaderBase
{
public:
    TYsonFragmentReader(IYsonConsumer* consumer, TInputStream* stream);

    bool HasNext();
    void ReadNext();

protected:
    virtual bool HasAttributes(int depth);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

