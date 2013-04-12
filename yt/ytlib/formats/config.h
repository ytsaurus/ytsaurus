#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TDsvFormatConfig
    : public TYsonSerializable
{
public:
    char RecordSeparator;
    char KeyValueSeparator;
    char FieldSeparator;
    TNullable<Stroka> LinePrefix;

    bool EnableEscaping;
    bool EscapeCarriageReturn;
    char EscapingSymbol;

    bool WithAttributes;
    Stroka AttributesPrefix;

    // Escaping rules (EscapingSymbol is '\\')
    //  * '\0' ---> "\0"
    //  * '\n' ---> "\n"
    //  * '\t' ---> "\t"
    //  * 'X'  ---> "\X" if X not in ['\0', '\n', '\t']

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
        Register("escape_carriage_return", EscapeCarriageReturn)
            .Default(false);
        Register("escaping_symbol", EscapingSymbol)
            .Default('\\');
        Register("with_attributes", WithAttributes)
            .Default(true);
        Register("attributes_prefix", AttributesPrefix)
            .Default("@");
    }
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EJsonFormat,
    (Text)
    (Pretty)
);

DECLARE_ENUM(EJsonAttributesMode,
    (Always)
    (Never)
    (OnDemand)
);

class TJsonFormatConfig
    : public TYsonSerializable
{
public:
    EJsonFormat Format;
    EJsonAttributesMode AttributesMode;

    TJsonFormatConfig()
    {
        Register("format", Format)
            .Default(EJsonFormat::Text);
        Register("attributes_mode", AttributesMode)
            .Default(EJsonAttributesMode::OnDemand);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYamrFormatConfig
    : public TYsonSerializable
{
public:
    bool HasSubkey;

    Stroka Key;
    Stroka Subkey;
    Stroka Value;

    bool Lenval;

    // Delimited specific options
    char FieldSeparator;
    char RecordSeparator;
    
    // Escaping options
    bool EnableEscaping;
    bool EscapeCarriageReturn;
    char EscapingSymbol;

    // make sense only in writer
    bool EnableTableIndex;

    TYamrFormatConfig()
    {
        Register("has_subkey", HasSubkey)
            .Default(false);
        Register("key", Key)
            .Default("key");
        Register("subkey", Subkey)
            .Default("subkey");
        Register("value", Value)
            .Default("value");
        Register("lenval", Lenval)
            .Default(false);
        Register("fs", FieldSeparator)
            .Default('\t');
        Register("rs", RecordSeparator)
            .Default('\n');
        Register("enable_table_index", EnableTableIndex)
            .Default(false);
        Register("enable_escaping", EnableEscaping)
            .Default(false);
        Register("escape_carriage_return", EscapeCarriageReturn)
            .Default(false);
        Register("escaping_symbol", EscapingSymbol)
            .Default('\\');
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYamredDsvFormatConfig
    : public TDsvFormatConfig
{
public:
    bool HasSubkey;
    char YamrKeysSeparator;

    std::vector<Stroka> KeyColumnNames;
    std::vector<Stroka> SubkeyColumnNames;

    TYamredDsvFormatConfig()
    {
        Register("has_subkey", HasSubkey)
            .Default(false);
        Register("key_column_names", KeyColumnNames);
        Register("subkey_column_names", SubkeyColumnNames)
            .Default();
        Register("yamr_keys_separator", YamrKeysSeparator)
            .Default(' ');
    }
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
