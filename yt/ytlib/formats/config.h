#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TDsvFormatConfig
    : public TConfigurable
{
    char RecordSeparator;
    char KeyValueSeparator;
    char FieldSeparator;
    TNullable<Stroka> LinePrefix;
    char EscapingSymbol;

    TDsvFormatConfig()
    {
        Register("record_separator", RecordSeparator).Default('\n');
        Register("key_value_separator", KeyValueSeparator).Default('=');
        Register("field_separator", FieldSeparator).Default('\t');
        Register("line_prefix", LinePrefix).Default();
        Register("escaping_symbol", EscapingSymbol).Default('\\');
    }
};

struct TJsonFormatConfig
    : public TConfigurable
{
    bool Pretty;

    TJsonFormatConfig()
    {
        Register("pretty", Pretty).Default(false);
    }
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
