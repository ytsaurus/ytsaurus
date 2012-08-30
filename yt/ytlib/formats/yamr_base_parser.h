#pragma once

#include <ytlib/ytree/parser.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TYamrBaseParser
    : public NYTree::IParser
{
public:
    TYamrBaseParser(
        char fieldSeparator,
        char recordSeparator,
        bool hasSubkey);

    virtual void Read(const TStringBuf& data) override;
    virtual void Finish() override;

private:
    DECLARE_ENUM(EState,
        (InsideKey)
        (InsideSubkey)
        (InsideValue)
    );
    EState State;
    
    char FieldSeparator;
    char RecordSeparator;
    bool HasSubkey;

    Stroka CurrentToken;
    
    bool IsStopSymbol[256];

    const char* Consume(const char* begin, const char* end);
    const char* FindNextStopSymbol(const char* begin, const char* end, EState state);

    // returns pointer to next fragment or NULL if record is not fully present in [begin, end)
    const char* TryConsumeRecord(const char* begin, const char *end);
    
    void ProcessKey(const TStringBuf& key);
    void ProcessSubkey(const TStringBuf& subkey);
    void ProcessValue(const TStringBuf& value);

    virtual void ConsumeKey(const TStringBuf& key) = 0;
    virtual void ConsumeSubkey(const TStringBuf& subkey) = 0;
    virtual void ConsumeValue(const TStringBuf& value) = 0;

    void ThrowIncorrectFormat() const;
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
