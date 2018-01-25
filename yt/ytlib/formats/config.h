#pragma once

#include "public.h"

#include <yt/ytlib/table_client/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TControlAttributesConfig
    : public NTableClient::TChunkReaderOptions
{
public:
    bool EnableKeySwitch;

    TControlAttributesConfig()
    {
        RegisterParameter("enable_key_switch", EnableKeySwitch)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TControlAttributesConfig)

////////////////////////////////////////////////////////////////////////////////

class TYsonFormatConfig
    : public NTableClient::TTypeConversionConfig
{
public:
    NYson::EYsonFormat Format;
    bool BooleanAsString;

    //! Only works for tabular data.
    bool SkipNullValues;

    TYsonFormatConfig()
    {
        RegisterParameter("format", Format)
            .Default(NYson::EYsonFormat::Binary);
        RegisterParameter("boolean_as_string", BooleanAsString)
            .Default(false);
        RegisterParameter("skip_null_values", SkipNullValues)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TYsonFormatConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJsonFormat,
    (Text)
    (Pretty)
);

DEFINE_ENUM(EJsonAttributesMode,
    (Always)
    (Never)
    (OnDemand)
);

class TJsonFormatConfig
    : public NTableClient::TTypeConversionConfig
{
public:
    EJsonFormat Format;
    EJsonAttributesMode AttributesMode;
    bool Plain;
    bool EncodeUtf8;
    i64 MemoryLimit;

    TNullable<int> StringLengthLimit;

    bool BooleanAsString;

    bool Stringify;
    bool AnnotateWithTypes;

    bool SupportInfinity;

    // Size of buffer used read out input stream in parser.
    // NB: in case of parsing long string yajl holds in memory whole string prefix and copy it on every parse call.
    // Therefore parsing long strings works faster with larger buffer.
    int BufferSize;

    //! Only works for tabular data.
    bool SkipNullValues;

    TJsonFormatConfig()
    {
        RegisterParameter("format", Format)
            .Default(EJsonFormat::Text);
        RegisterParameter("attributes_mode", AttributesMode)
            .Default(EJsonAttributesMode::OnDemand);
        RegisterParameter("plain", Plain)
            .Default(false);
        RegisterParameter("encode_utf8", EncodeUtf8)
            .Default(true);
        RegisterParameter("string_length_limit", StringLengthLimit)
            .Default();
        RegisterParameter("boolean_as_string", BooleanAsString)
            .Default(false);
        RegisterParameter("stringify", Stringify)
            .Default(false);
        RegisterParameter("annotate_with_types", AnnotateWithTypes)
            .Default(false);
        RegisterParameter("support_infinity", SupportInfinity)
            .Default(false);
        RegisterParameter("buffer_size", BufferSize)
            .Default(16 * 1024 * 1024);
        RegisterParameter("skip_null_values", SkipNullValues)
            .Default(false);

        // NB: yajl can consume two times more memory than row size.
        MemoryLimit = 2 * NTableClient::MaxRowWeightLimit;
    }
};

DEFINE_REFCOUNTED_TYPE(TJsonFormatConfig)

////////////////////////////////////////////////////////////////////////////////
// Readers for Yamr and Dsv share lots of methods and functionality                          //
// and dependency diagram has the following shape:                                           //
//                                                                                           //
//                    TTableFormatConfigBase --------------------------.                     //
//                      /                 \                             \                    //
//                     /                   \                             \                   //
//       TYamrFormatConfigBase        TDsvFormatConfigBase                \                  //
//            /        \                   /            \                  \                 //
//           /          \                 /              \                  \                //
// TYamrFormatConfig   TYamredDsvFormatConfig   TDsvFormatConfig  TSchemafulDsvFormatConfig  //
//                                                                                           //
// All fields are declared in Base classes, all parameters are                               //
// registered in derived classes.                                                            //

class TTableFormatConfigBase
    : public NTableClient::TTypeConversionConfig
{
public:
    char RecordSeparator;
    char FieldSeparator;

    // Escaping rules (EscapingSymbol is '\\')
    //  * '\0' ---> "\0"
    //  * '\n' ---> "\n"
    //  * '\t' ---> "\t"
    //  * 'X'  ---> "\X" if X not in ['\0', '\n', '\t']
    bool EnableEscaping;
    char EscapingSymbol;

    bool EnableTableIndex;
};

DEFINE_REFCOUNTED_TYPE(TTableFormatConfigBase)

////////////////////////////////////////////////////////////////////////////////

class TYamrFormatConfigBase
    : public virtual TTableFormatConfigBase
{
public:
    bool HasSubkey;
    bool Lenval;
};

DEFINE_REFCOUNTED_TYPE(TYamrFormatConfigBase)

////////////////////////////////////////////////////////////////////////////////

class TDsvFormatConfigBase
    : public virtual TTableFormatConfigBase
{
public:
    char KeyValueSeparator;

    // Only supported for tabular data
    TNullable<TString> LinePrefix;
};

DEFINE_REFCOUNTED_TYPE(TDsvFormatConfigBase)

////////////////////////////////////////////////////////////////////////////////

class TYamrFormatConfig
    : public TYamrFormatConfigBase
{
public:
    TString Key;
    TString Subkey;
    TString Value;

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

DEFINE_REFCOUNTED_TYPE(TYamrFormatConfig)

////////////////////////////////////////////////////////////////////////////////

class TDsvFormatConfig
    : public TDsvFormatConfigBase
{
public:

    TString TableIndexColumn;

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

DEFINE_REFCOUNTED_TYPE(TDsvFormatConfig)

////////////////////////////////////////////////////////////////////////////////

class TYamredDsvFormatConfig
    : public TYamrFormatConfigBase
    , public TDsvFormatConfigBase
{
public:
    char YamrKeysSeparator;

    std::vector<TString> KeyColumnNames;
    std::vector<TString> SubkeyColumnNames;

    TYamredDsvFormatConfig()
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
            THashSet<TString> names;

            for (const auto& name : KeyColumnNames) {
                if (!names.insert(name).second) {
                    THROW_ERROR_EXCEPTION("Duplicate column %Qv found in \"key_column_names\"",
                        name);
                }
            }

            for (const auto& name : SubkeyColumnNames) {
                if (!names.insert(name).second) {
                    THROW_ERROR_EXCEPTION("Duplicate column %Qv found in \"subkey_column_names\"",
                        name);
                }
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TYamredDsvFormatConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMissingSchemafulDsvValueMode,
    (SkipRow)
    (Fail)
    (PrintSentinel)
);

class TSchemafulDsvFormatConfig
    : public TTableFormatConfigBase
{
public:
    TNullable<std::vector<TString>> Columns;

    EMissingSchemafulDsvValueMode MissingValueMode;
    TString MissingValueSentinel;

    TNullable<bool> EnableColumnNamesHeader;

    const std::vector<TString>& GetColumnsOrThrow() const
    {
        if (!Columns) {
            THROW_ERROR_EXCEPTION("Missing \"columns\" attribute in schemaful DSV format");
        }
        return *Columns;
    }

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
            .Default(EMissingSchemafulDsvValueMode::Fail);

        RegisterParameter("missing_value_sentinel", MissingValueSentinel)
            .Default("");

        RegisterParameter("enable_column_names_header", EnableColumnNamesHeader)
            .Default();

        RegisterValidator([&] () {
            if (Columns) {
                THashSet<TString> names;
                for (const auto& name : *Columns) {
                    if (!names.insert(name).second) {
                        THROW_ERROR_EXCEPTION("Duplicate column name %Qv in schemaful DSV configuration",
                            name);
                    }
                }
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TSchemafulDsvFormatConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProtobufType,
    (Double)
    (Float)

    (Int64)
    (Uint64)
    (Sint64)
    (Fixed64)
    (Sfixed64)

    (Int32)
    (Uint32)
    (Sint32)
    (Fixed32)
    (Sfixed32)

    (Bool)
    (String)
    (Bytes)

    (EnumInt)
    (EnumString)

    (Message)
);

class TProtobufColumnConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Name;
    EProtobufType ProtoType;
    ui64 FieldNumber;
    TNullable<TString> EnumerationName;

    TProtobufColumnConfig()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter("proto_type", ProtoType);
        RegisterParameter("field_number", FieldNumber);
        RegisterParameter("enumeration_name", EnumerationName)
            .Default();
    }
};
DEFINE_REFCOUNTED_TYPE(TProtobufColumnConfig)

class TProtobufTableConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<TProtobufColumnConfigPtr> Columns;

    TProtobufTableConfig()
    {
        RegisterParameter("columns", Columns);
    }
};
DEFINE_REFCOUNTED_TYPE(TProtobufTableConfig)

DEFINE_ENUM(ENestedMessagesMode,
    (Protobuf)
    (Yson)
);

class TProtobufFormatConfig
    : public NYTree::TYsonSerializable
{
public:
    TString FileDescriptorSet; // deprecated
    std::vector<int> FileIndices; // deprecated
    std::vector<int> MessageIndices; // deprecated
    bool EnumsAsStrings; // deprecated
    ENestedMessagesMode NestedMessagesMode; // deprecated

    std::vector<TProtobufTableConfigPtr> Tables;
    NYTree::IMapNodePtr Enumerations;

    TProtobufFormatConfig()
    {
        RegisterParameter("file_descriptor_set", FileDescriptorSet)
            .Default();
        RegisterParameter("file_indices", FileIndices)
            .Default();
        RegisterParameter("message_indices", MessageIndices)
            .Default();
        RegisterParameter("nested_messages_mode", NestedMessagesMode)
            .Default(ENestedMessagesMode::Protobuf);
        RegisterParameter("enums_as_strings", EnumsAsStrings)
            .Default();

        RegisterParameter("tables", Tables)
            .Default();
        RegisterParameter("enumerations", Enumerations)
            .Default();

        RegisterValidator([&] {
            Validate();
        });
    }

private:
    void Validate();
};

DEFINE_REFCOUNTED_TYPE(TProtobufFormatConfig)

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWebJsonFormatConfig
    : public NYTree::TYsonSerializable
{
public:
    int RowLimit;
    int ColumnLimit;
    int StringLikeLengthLimit;

    TSchemalessWebJsonFormatConfig()
    {
        RegisterParameter("row_limit", RowLimit)
            .Default(100)
            .GreaterThan(0);
        RegisterParameter("column_limit", ColumnLimit)
            .Default(50)
            .GreaterThan(0);
        RegisterParameter("string_like_length_limit", StringLikeLengthLimit)
            .Default(4 * 1024)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TSchemalessWebJsonFormatConfig)

////////////////////////////////////////////////////////////////////////////////

class TSkiffFormatConfig
    : public NYTree::TYsonSerializable
{
public:
    NYTree::IMapNodePtr SkiffSchemaRegistry;
    NYTree::IListNodePtr TableSkiffSchemas;

    TSkiffFormatConfig()
    {
        RegisterParameter("skiff_schema_registry", SkiffSchemaRegistry)
            .Default();
        RegisterParameter("table_skiff_schemas", TableSkiffSchemas);
    }
};

DEFINE_REFCOUNTED_TYPE(TSkiffFormatConfig)

} // namespace NFormats
} // namespace NYT
