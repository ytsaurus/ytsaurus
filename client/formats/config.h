#pragma once

#include "public.h"

#include <yt/client/table_client/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NFormats {

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
    EComplexTypeMode ComplexTypeMode;

    //! Only works for tabular data.
    bool SkipNullValues;

    TYsonFormatConfig()
    {
        RegisterParameter("format", Format)
            .Default(NYson::EYsonFormat::Binary);
        RegisterParameter("boolean_as_string", BooleanAsString)
            .Default(false);
        RegisterParameter("complex_type_mode", ComplexTypeMode)
            .Default(EComplexTypeMode::Named);
        RegisterParameter("skip_null_values", SkipNullValues)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TYsonFormatConfig)

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
    bool EnableEom;
};

DEFINE_REFCOUNTED_TYPE(TYamrFormatConfigBase)

////////////////////////////////////////////////////////////////////////////////

class TDsvFormatConfigBase
    : public virtual TTableFormatConfigBase
{
public:
    char KeyValueSeparator;

    // Only supported for tabular data
    std::optional<TString> LinePrefix;
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
        RegisterParameter("enable_eom", EnableEom)
            .Default(false);

        RegisterPreprocessor([&] {
            if (EnableEom && !Lenval) {
                THROW_ERROR_EXCEPTION("EOM marker is not supported in YAMR text mode");
            }
        });
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
        RegisterParameter("enable_eom", EnableEom)
            .Default(false);

        RegisterPreprocessor([&] {
            if (EnableEom && !Lenval) {
                THROW_ERROR_EXCEPTION("EOM marker is not supported in YAMR text mode");
            }
        });

        RegisterPostprocessor([&] {
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
    std::optional<std::vector<TString>> Columns;

    EMissingSchemafulDsvValueMode MissingValueMode;
    TString MissingValueSentinel;

    std::optional<bool> EnableColumnNamesHeader;

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

        RegisterPostprocessor([&] {
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

    // Same as 'bytes'.
    (Message)

    // Protobuf type must be message.
    // It corresponds to a complex type.
    (StructuredMessage)

    // Protobuf type must be string.
    // Maps to any type (not necessarily "any" type) in table row.
    (Any)

    // Protobuf type must be string containing valid YSON map.
    // Each entry (|key|, |value|) of this map will correspond
    // a separate |TUnversionedValue| under name |key|.
    // NOTE: Not allowed inside complex types.
    (OtherColumns)
);

class TProtobufColumnConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Name;
    EProtobufType ProtoType;
    ui64 FieldNumber;
    bool Repeated;
    bool Packed;
    std::vector<TProtobufColumnConfigPtr> Fields;
    std::optional<TString> EnumerationName;

    TProtobufColumnConfig()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter("proto_type", ProtoType);
        RegisterParameter("field_number", FieldNumber);
        RegisterParameter("repeated", Repeated)
            .Default(false);
        RegisterParameter("packed", Packed)
            .Default(false);
        RegisterParameter("fields", Fields)
            .Default();
        RegisterParameter("enumeration_name", EnumerationName)
            .Default();
    }
};
DEFINE_REFCOUNTED_TYPE(TProtobufColumnConfig)

////////////////////////////////////////////////////////////////////////////////

class TProtobufTableConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<TProtobufColumnConfigPtr> Columns;

    TProtobufTableConfig()
    {
        RegisterParameter("columns", Columns);

        RegisterPostprocessor([&] {
            bool hasOtherColumns = false;
            for (const auto& column: Columns) {
                if (column->ProtoType == EProtobufType::OtherColumns) {
                    if (hasOtherColumns) {
                        THROW_ERROR_EXCEPTION("Multiple \"other_columns\" in protobuf config are not allowed");
                    }
                    hasOtherColumns = true;
                }
            }
        });
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

    EComplexTypeMode ComplexTypeMode;

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

        RegisterParameter("complex_type_mode", ComplexTypeMode)
            .Default(EComplexTypeMode::Named);
    }

private:
    void Validate();
};

DEFINE_REFCOUNTED_TYPE(TProtobufFormatConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWebJsonValueFormat,
    // Values are stringified and (de-)serialized together with their types in form
    // |"column_name": {"$type": "double", "$value": "3.141592"}|.
    // Strings with length exceeding |FieldWeightLimit| are truncated and meta-attribute
    // "$incomplete" with value |true| is added to the representation, i.e.
    // |"column_name": {"$type": "string", "$incomplete": true, "$value": "Some very long st"}|
    (Schemaless)

    // Values are stringified and (de-)serialized in form
    // |<column_name>: [ <value>,  <stringified-type-index>]|, i.e. |"column_name": ["3.141592", "3"]|.
    // Type indices point to type registry stored under "yql_type_registry" key.
    // Non-UTF-8 strings are Base64-encoded and enclosed in a map with "b64" and "val" keys:
    //    | "column_name": {"val": "aqw==", "b64": true} |.
    // Strings and lists can be truncated, in which case they are enclosed in a map with "inc" and "val" keys:
    //    | "column_name": {"val": ["12", "13"], "inc": true} |
    // Wrapping in an additional map can occur on any depth.
    // Both "inc" and "b64" keys may appear in such maps.
    (Yql)
);

class TWebJsonFormatConfig
    : public NYTree::TYsonSerializable
{
public:
    int MaxSelectedColumnCount;
    int FieldWeightLimit;
    int StringWeightLimit;
    int MaxAllColumnNamesCount;
    std::optional<std::vector<TString>> ColumnNames;
    EWebJsonValueFormat ValueFormat;

    // Intentionally do not reveal following options to user.
    bool SkipSystemColumns = true;

    TWebJsonFormatConfig()
    {
        RegisterParameter("max_selected_column_count", MaxSelectedColumnCount)
            .Default(50)
            .GreaterThanOrEqual(0);
        RegisterParameter("field_weight_limit", FieldWeightLimit)
            .Default(1_KB)
            .GreaterThanOrEqual(0);
        RegisterParameter("string_weight_limit", StringWeightLimit)
            .Default(200)
            .GreaterThanOrEqual(0);
        RegisterParameter("max_all_column_names_count", MaxAllColumnNamesCount)
            .Default(2000)
            .GreaterThanOrEqual(0);
        RegisterParameter("column_names", ColumnNames)
            .Default();
        RegisterParameter("value_format", ValueFormat)
            .Default(EWebJsonValueFormat::Schemaless);
    }
};

DEFINE_REFCOUNTED_TYPE(TWebJsonFormatConfig)

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
