#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/ytree/yson_consumer.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TTsvParser
{
public:
    explicit TTsvParser(NYTree::IYsonConsumer* consumer, TTsvFormatConfigPtr config = NULL);

    void Read(const TStringBuf& data);
    void Finish();

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
            
void ParseYson(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TTsvFormatConfigPtr config = NULL);

void ParseTsv(
    const TStringBuf& data,
    NYTree::IYsonConsumer* consumer,
    TTsvFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
