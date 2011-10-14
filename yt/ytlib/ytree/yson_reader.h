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
    TYsonReader(IYsonConsumer* consumer);

    void Read(TInputStream* stream);

    static TYsonProducer::TPtr GetProducer(TInputStream* stream);

private:
    static const int Eos = -1;
    static const int NoLookahead = -2;

    IYsonConsumer* Events;
    TInputStream* Stream;
    int Lookahead;

    int LineIndex;
    int Position;

    static void GetProducerThunk(IYsonConsumer* consumer, TInputStream* stream);

    void Reset();

    int ReadChar(bool binary = false);
    void ReadChars(int charCount, ui8* buffer);

    void ExpectChar(char expectedCh);

    int PeekChar();

    static bool IsWhitespace(int ch);
    void SkipWhitespaces();

    Stroka ReadString();
    Stroka ReadNumeric();

    void ParseAny();

    void ParseAttributesItem();
    void ParseAttributes();

    void ParseListItem(int index);
    void ParseList();

    void ParseMapItem();
    void ParseMap();

    void ParseEntity();

    void ParseString();

    void ParseBinaryString();
    void ParseBinaryInt64();
    void ParseBinaryDouble();

    static bool SeemsInteger(const Stroka& str);
    void ParseNumeric();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

