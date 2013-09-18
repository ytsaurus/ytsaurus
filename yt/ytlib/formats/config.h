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

    // Only supported for tabular data
    TNullable<Stroka> LinePrefix;

    bool EnableEscaping;
    char EscapingSymbol;

    // Escaping rules (EscapingSymbol is '\\')
    //  * '\0' ---> "\0"
    //  * '\n' ---> "\n"
    //  * '\t' ---> "\t"
    //  * 'X'  ---> "\X" if X not in ['\0', '\n', '\t']

    bool EnableTableIndex;
    Stroka TableIndexColumn;

    TDsvFormatConfig()
    {
        RegisterParameter("record_separator", RecordSeparator)
            .Default('\n');
        RegisterParameter("key_value_separator", KeyValueSeparator)
            .Default('=');
        RegisterParameter("field_separator", FieldSeparator)
            .Default('\t');
        RegisterParameter("line_prefix", LinePrefix)
            .Default();
        RegisterParameter("enable_escaping", EnableEscaping)
            .Default(true);
        RegisterParameter("escaping_symbol", EscapingSymbol)
            .Default('\\');
        RegisterParameter("enable_table_index", EnableTableIndex)
            .Default(false);
        RegisterParameter("table_index_column", TableIndexColumn)
            .Default("@table_index")
            .NonEmpty();
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
        RegisterParameter("format", Format)
            .Default(EJsonFormat::Text);
        RegisterParameter("attributes_mode", AttributesMode)
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
    char EscapingSymbol;

    // Makes sense only in writer
    bool EnableTableIndex;

    TYamrFormatConfig()
    {
        RegisterParameter("has_subkey", HasSubkey)
            .Default(false);
        RegisterParameter("key", Key)
            .Default("key");
        RegisterParameter("subkey", Subkey)
            .Default("subkey");
        RegisterParameter("value", Value)
            .Default("value");
        RegisterParameter("lenval", Lenval)
            .Default(false);
        RegisterParameter("fs", FieldSeparator)
            .Default('\t');
        RegisterParameter("rs", RecordSeparator)
            .Default('\n');
        RegisterParameter("enable_table_index", EnableTableIndex)
            .Default(false);
        RegisterParameter("enable_escaping", EnableEscaping)
            .Default(false);
        RegisterParameter("escaping_symbol", EscapingSymbol)
            .Default('\\');
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYamredDsvFormatConfig
    : public TDsvFormatConfig
{
public:
    bool HasSubkey;
    bool Lenval;
    char YamrKeysSeparator;

    std::vector<Stroka> KeyColumnNames;
    std::vector<Stroka> SubkeyColumnNames;

    TYamredDsvFormatConfig()
    {
        RegisterParameter("has_subkey", HasSubkey)
            .Default(false);
        RegisterParameter("lenval", Lenval)
            .Default(false);
        RegisterParameter("key_column_names", KeyColumnNames);
        RegisterParameter("subkey_column_names", SubkeyColumnNames)
            .Default();
        RegisterParameter("yamr_keys_separator", YamrKeysSeparator)
            .Default(' ');

        RegisterValidator([&] () {
            yhash_set<Stroka> names;

            FOREACH(const auto& name, KeyColumnNames) {
                if (!names.insert(name).second) {
                    THROW_ERROR_EXCEPTION(
                        "Duplicate column name encountered in \"key_column_names\": %s",
                        ~name.Quote());
                }
            }

            FOREACH(const auto& name, SubkeyColumnNames) {
                if (!names.insert(name).second) {
                    THROW_ERROR_EXCEPTION(
                        "Duplicate column name encountered in \"subkey_column_names\": %s",
                        ~name.Quote());
                }
            }
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
