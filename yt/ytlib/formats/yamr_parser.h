#pragma once

#include "public.h"
#include "config.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TYamrParser
    : public NYTree::IParser
{
public:
    TYamrParser(
        NYTree::IYsonConsumer* consumer,
        TYamrFormatConfigPtr config = NULL);

    virtual void Read(const TStringBuf& data) OVERRIDE;
    virtual void Finish() OVERRIDE;

private:
    NYTree::IYsonConsumer* Consumer;
    TYamrFormatConfigPtr Config;

    Stroka CurrentToken;
    Stroka Key;

    const char* Consume(const char* begin, const char* end);
    const char* FindNextStopSymbol(const char* begin, const char* end);
    const char* FindEndOfRow(const char* begin, const char* end);

    bool IsStopSymbol[256];

    DECLARE_ENUM(EState,
        (InsideKey)
        (InsideSubkey)
        (InsideValue)
    );
    EState State;
};

////////////////////////////////////////////////////////////////////////////////

void ParseYamr(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config = NULL);

void ParseYamr(
    const TStringBuf& data,
    NYTree::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
