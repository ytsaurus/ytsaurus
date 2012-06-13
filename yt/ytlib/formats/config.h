#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TDsvFormatConfig
    : public TYsonSerializable
{
    char RecordSeparator;
    char KeyValueSeparator;
    char FieldSeparator;
    TNullable<Stroka> LinePrefix;

    bool EnableEscaping;
    char EscapingSymbol;

    TDsvFormatConfig()
    {
        Register("record_separator", RecordSeparator)
            .Default('\n');
        Register("key_value_separator", KeyValueSeparator)
            .Default('=');
        Register("field_separator", FieldSeparator)
            .Default('\t');
        Register("line_prefix", LinePrefix)
            .Default();
        Register("enable_escaping", EnableEscaping)
            .Default(true);
        Register("escaping_symbol", EscapingSymbol)
            .Default('\\');
    }
};

struct TJsonFormatConfig
    : public TYsonSerializable
{
    bool Pretty;

    TJsonFormatConfig()
    {
        Register("pretty", Pretty)
            .Default(false);
    }
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
