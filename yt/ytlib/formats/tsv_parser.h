#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/ytree/parser.h>
#include <ytlib/ytree/yson_consumer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TTsvParser
    : public NYTree::IParser
{
public:
    explicit TTsvParser(NYTree::IYsonConsumer* consumer, TTsvFormatConfigPtr config = New<TTsvFormatConfig>());

    virtual void Read(const TStringBuf& data);
    virtual void Finish();

private:
    NYTree::IYsonConsumer* Consumer;
    TTsvFormatConfigPtr Config;

    bool FirstSymbol;

    Stroka CurrentToken;

    DECLARE_ENUM(EState,
        (InsideKey)
        (InsideValue)
    );
    EState State;

    const char* Consume(const char* begin, const char* end);
    const char* FindEndOfValue(const char* begin, const char* end);
};

////////////////////////////////////////////////////////////////////////////////

void ParseTsv(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TTsvFormatConfigPtr config = NULL);

void ParseTsv(
    const TStringBuf& data,
    NYTree::IYsonConsumer* consumer,
    TTsvFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
