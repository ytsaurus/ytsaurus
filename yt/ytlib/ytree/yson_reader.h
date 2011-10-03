#pragma once

#include "common.h"
#include "yson_events.h"


namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonReader
    : private TNonCopyable
{
public:
    TYsonReader(IYsonConsumer* events);

    void Read(TInputStream* stream);

private:
    static const int Eos = -1;
    static const int NoLookahead = -2;

    IYsonConsumer* Events;
    TInputStream* Stream;
    int Lookahead;

    void Reset();

    int ReadChar();

    void ExpectChar(char expectedCh);

    int PeekChar();

    static bool IsWhitespace(int ch);

    void SkipWhitespaces();

    Stroka ReadString();

    Stroka ReadNumericLike();

    void ParseAny();

    void ParseAttributesItem();
    void ParseAttributes();

    void ParseListItem(int index);
    void ParseList();

    void ParseMapItem();
    void ParseMap();

    void ParseEntity();

    void ParseString();

    static bool IsIntegerLike(const Stroka& str);

    void ParseNumeric();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

