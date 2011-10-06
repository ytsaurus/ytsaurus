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
    void ReadChars(int charCount, char* buffer);

    template<class T>
    T ReadRaw()
    {
        int charCount = sizeof(T);
        char buffer[charCount];
        ReadChars(charCount, buffer);
        return *reinterpret_cast<T*>(buffer);
    }

    Stroka ReadChars(int charsCount);

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

