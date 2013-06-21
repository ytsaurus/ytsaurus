#pragma once

#include "parser.h"
#include "yamr_table.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct IYamrConsumer
    : public TRefCounted
{
    virtual void ConsumeKey(const TStringBuf& key) = 0;
    virtual void ConsumeSubkey(const TStringBuf& subkey) = 0;
    virtual void ConsumeValue(const TStringBuf& value) = 0;
};

////////////////////////////////////////////////////////////////////////////////

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
        char escapingSymbol,
        bool escapeCarriageReturn);

    virtual void Read(const TStringBuf& data) override;
    virtual void Finish() override;

private:
    const char* Consume(const char* begin, const char* end);

    // returns pointer to next fragment or NULL if record is not fully present in [begin, end)
    const char* TryConsumeRecord(const char* begin, const char *end);

    void ProcessKey(const TStringBuf& key);
    void ProcessSubkey(const TStringBuf& subkey);
    void ProcessValue(const TStringBuf& value);

    void ThrowIncorrectFormat() const;

    void OnRangeConsumed(const char* begin, const char* end);
    void AppendToContextBuffer(char symbol);

    Stroka GetDebugInfo() const;

    IYamrConsumerPtr Consumer;
    
    DECLARE_ENUM(EState,
        (InsideKey)
        (InsideSubkey)
        (InsideValue)
    );
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
    Stroka GetDebugInfo() const;

    const char* Consume(const char* begin, const char* end);
    const char* ConsumeLength(const char* begin, const char* end);
    const char* ConsumeData(const char* begin, const char* end);

    IYamrConsumerPtr Consumer;

    bool HasSubkey;

    Stroka CurrentToken;

    union {
        ui32 Length;
        char Bytes[4];
    } Union;

    bool ReadingLength;
    ui32 BytesToRead;

    DECLARE_ENUM(EState,
        (InsideKey)
        (InsideSubkey)
        (InsideValue)
    );

    EState State;

    static const i64 MaxFieldLength = (i64) 16 * 1024 * 1024;
    
    static const int ContextSize = 16;
    char ContextBuffer[ContextSize];

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
