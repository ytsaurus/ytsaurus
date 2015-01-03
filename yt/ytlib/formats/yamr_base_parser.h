#pragma once

#include "parser.h"
#include "yamr_table.h"

#include <core/ytree/attributes.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct IYamrConsumer
    : public virtual TRefCounted
{
    virtual void ConsumeKey(const TStringBuf& key) = 0;
    virtual void ConsumeSubkey(const TStringBuf& subkey) = 0;
    virtual void ConsumeValue(const TStringBuf& value) = 0;
    virtual void SwitchTable(i64 tableIndex) = 0;
};

DEFINE_REFCOUNTED_TYPE(IYamrConsumer)

////////////////////////////////////////////////////////////////////////////////

class TYamrConsumerBase
    : public IYamrConsumer
{
public:
    explicit TYamrConsumerBase(NYson::IYsonConsumer* consumer);
    virtual void SwitchTable(i64 tableIndex) override;

protected:
    NYson::IYsonConsumer* Consumer;

};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EYamrDelimitedBaseParserState,
    (InsideKey)
    (InsideSubkey)
    (InsideValue)
);

class TYamrDelimitedBaseParser
    : public IParser
{
public:
    TYamrDelimitedBaseParser(
        IYamrConsumerPtr consumer,
        bool hasSubkey,
        char fieldSeparator,
        char recordSeparator,
        bool enableKeyEscaping,
        bool enableValueEscaping,
        char escapingSymbol);

    virtual void Read(const TStringBuf& data) override;
    virtual void Finish() override;

private:
    using EState = EYamrDelimitedBaseParserState;

    const char* Consume(const char* begin, const char* end);

    void ProcessKey(const TStringBuf& key);
    void ProcessSubkey(const TStringBuf& subkey);
    void ProcessSubkeyBadFormat(const TStringBuf& subkey);
    void ProcessValue(const TStringBuf& value);
    void ProcessTableSwitch(const TStringBuf& tableIndex);

    const char* ProcessToken(
        void (TYamrDelimitedBaseParser::*processor)(const TStringBuf& value),
        const char* begin,
        const char* next);

    const char* FindNext(const char* begin, const char* end, const TLookupTable& lookupTable);


    void ThrowIncorrectFormat() const;

    void OnRangeConsumed(const char* begin, const char* end);
    void AppendToContextBuffer(char symbol);

    Stroka GetContext() const;
    std::unique_ptr<NYTree::IAttributeDictionary> GetDebugInfo() const;


    IYamrConsumerPtr Consumer;

    EState State;

    char FieldSeparator;
    char RecordSeparator;
    char EscapingSymbol;
    bool ExpectingEscapedChar;
    bool HasSubkey;

    Stroka CurrentToken;

    // Diagnostic Info
    i64 Offset;
    i64 Record;
    i32 BufferPosition;

    static const int ContextBufferSize = 64;
    char ContextBuffer[ContextBufferSize];

    TYamrTable Table;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EYamrLenvalBaseParserState,
    (InsideTableSwitch)
    (InsideKey)
    (InsideSubkey)
    (InsideValue)
);

class TYamrLenvalBaseParser
    : public IParser
{
public:
    TYamrLenvalBaseParser(
        IYamrConsumerPtr consumer,
        bool hasSubkey);

    virtual void Read(const TStringBuf& data) override;
    virtual void Finish() override;

private:
    using EState = EYamrLenvalBaseParserState;

    const char* Consume(const char* begin, const char* end);
    const char* ConsumeInt(const char* begin, const char* end);
    const char* ConsumeLength(const char* begin, const char* end);
    const char* ConsumeData(const char* begin, const char* end);

    IYamrConsumerPtr Consumer;

    bool HasSubkey;

    Stroka CurrentToken;

    union {
        ui32 Value;
        char Bytes[4];
    } Union;

    bool ReadingLength;
    ui32 BytesToRead;

    EState State;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
