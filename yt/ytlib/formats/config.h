#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>
#include <ytlib/table_client/public.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TYsonFormatConfig
    : public NYTree::TYsonSerializable
{
public:
    NYson::EYsonFormat Format;
    bool BooleanAsString;

    TYsonFormatConfig()
    {
        RegisterParameter("format", Format)
            .Default(NYson::EYsonFormat::Binary);
        RegisterParameter("boolean_as_string", BooleanAsString)
            .Default(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDsvFormatConfig
    : public NYTree::TYsonSerializable
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
    : public NYTree::TYsonSerializable
{
public:
    EJsonFormat Format;
    EJsonAttributesMode AttributesMode;
    bool EncodeUtf8;
    i64 MemoryLimit;

    TNullable<int> StringLengthLimit;

    bool BooleanAsString;

    TJsonFormatConfig()
    {
        RegisterParameter("format", Format)
            .Default(EJsonFormat::Text);
        RegisterParameter("attributes_mode", AttributesMode)
            .Default(EJsonAttributesMode::OnDemand);
        RegisterParameter("encode_utf8", EncodeUtf8)
            .Default(true);
        RegisterParameter("string_length_limit", StringLengthLimit)
            .Default(Null);
        RegisterParameter("boolean_as_string", BooleanAsString)
            .Default(true);

        MemoryLimit = NTableClient::MaxRowWeightLimit;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYamrFormatConfig
    : public NYTree::TYsonSerializable
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

            for (const auto& name : KeyColumnNames) {
                if (!names.insert(name).second) {
                    THROW_ERROR_EXCEPTION(
                        "Duplicate column name encountered in \"key_column_names\": %s",
                        ~name.Quote());
                }
            }

            for (const auto& name : SubkeyColumnNames) {
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

class TSchemafulDsvFormatConfig
    : public NYTree::TYsonSerializable
{
public:
    char RecordSeparator;
    char FieldSeparator;

    bool EnableTableIndex;

    bool EnableEscaping;
    char EscapingSymbol;

    std::vector<Stroka> Columns;

    DECLARE_ENUM(EMissingValueMode,
        (SkipRow)
        (Fail)
        (PrintSentinel)
    );

    EMissingValueMode MissingValueMode;
    Stroka MissingValueSentinel;


    TSchemafulDsvFormatConfig()
    {
        RegisterParameter("record_separator", RecordSeparator)
            .Default('\n');
        RegisterParameter("field_separator", FieldSeparator)
            .Default('\t');

        RegisterParameter("enable_table_index", EnableTableIndex)
            .Default(false);

        RegisterParameter("enable_escaping", EnableEscaping)
            .Default(true);
        RegisterParameter("escaping_symbol", EscapingSymbol)
            .Default('\\');

        RegisterParameter("columns", Columns)
            .Default();

        RegisterParameter("missing_value_mode", MissingValueMode)
            .Default(EMissingValueMode::SkipRow);

        RegisterParameter("missing_value_sentinel", MissingValueSentinel)
            .Default("");

        RegisterValidator([&] () {
            yhash_set<Stroka> names;

            for (const auto& name : Columns) {
                if (!names.insert(name).second) {
                    THROW_ERROR_EXCEPTION(
                        "Duplicate column name encountered in \"columns\": %s",
                        ~name.Quote());
                }
            }
            if (Columns.empty()) {
                THROW_ERROR_EXCEPTION("Columns should be non-empty");
            }
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
