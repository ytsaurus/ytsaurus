#include "config.h"

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

void TControlAttributesConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_key_switch", &TThis::EnableKeySwitch)
        .Default(false);

    registrar.Parameter("enable_end_of_stream", &TThis::EnableEndOfStream)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TYsonFormatConfig::TYsonFormatConfig()
{
    RegisterParameter("format", Format)
        .Default(NYson::EYsonFormat::Binary);
    RegisterParameter("complex_type_mode", ComplexTypeMode)
        .Default(EComplexTypeMode::Named);
    RegisterParameter("decimal_mode", DecimalMode)
        .Default(EDecimalMode::Binary);
    RegisterParameter("time_mode", TimeMode)
        .Default(ETimeMode::Binary);
    RegisterParameter("uuid_mode", UuidMode)
        .Default(EUuidMode::Binary);
    RegisterParameter("skip_null_values", SkipNullValues)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TYamrFormatConfig::TYamrFormatConfig()
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

////////////////////////////////////////////////////////////////////////////////

TDsvFormatConfig::TDsvFormatConfig()
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
    RegisterParameter("skip_unsupported_types", SkipUnsupportedTypes)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TYamredDsvFormatConfig::TYamredDsvFormatConfig()
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
    RegisterParameter("skip_unsupported_types_in_value", SkipUnsupportedTypesInValue)
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

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString>& TSchemafulDsvFormatConfig::GetColumnsOrThrow() const
{
    if (!Columns) {
        THROW_ERROR_EXCEPTION("Missing \"columns\" attribute in schemaful DSV format");
    }
    return *Columns;
}

TSchemafulDsvFormatConfig::TSchemafulDsvFormatConfig()
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

////////////////////////////////////////////////////////////////////////////////

TProtobufTypeConfig::TProtobufTypeConfig()
{
    RegisterParameter("proto_type", ProtoType);
    RegisterParameter("fields", Fields)
        .Default();
    RegisterParameter("enumeration_name", EnumerationName)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TProtobufColumnConfig::TProtobufColumnConfig()
{
    RegisterParameter("name", Name)
        .NonEmpty();
    RegisterParameter("field_number", FieldNumber)
        .Optional();
    RegisterParameter("repeated", Repeated)
        .Default(false);
    RegisterParameter("packed", Packed)
        .Default(false);

    RegisterParameter("type", Type)
        .Default();

    RegisterParameter("proto_type", ProtoType)
        .Default();
    RegisterParameter("fields", Fields)
        .Default();
    RegisterParameter("enumeration_name", EnumerationName)
        .Default();

    RegisterPostprocessor([&] {
        CustomPostprocess();
    });
}

void TProtobufColumnConfig::CustomPostprocess()
{
    if (Packed && !Repeated) {
        THROW_ERROR_EXCEPTION("Field %Qv is marked \"packed\" but is not marked \"repeated\"",
            Name);
    }

    if (!Type) {
        Type = New<TProtobufTypeConfig>();
        if (!ProtoType) {
            THROW_ERROR_EXCEPTION("One of \"type\" and \"proto_type\" must be specified");
        }
        Type->ProtoType = *ProtoType;
        Type->Fields = std::move(Fields);
        Type->EnumerationName = EnumerationName;
    }

    if (!FieldNumber && Type->ProtoType != EProtobufType::Oneof) {
        THROW_ERROR_EXCEPTION("\"field_number\" is required for type %Qlv",
            Type->ProtoType);
    }
}

////////////////////////////////////////////////////////////////////////////////

TProtobufTableConfig::TProtobufTableConfig()
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

TProtobufFormatConfig::TProtobufFormatConfig()
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

    RegisterParameter("file_descriptor_set_text", FileDescriptorSetText)
        .Default();
    RegisterParameter("type_names", TypeNames)
        .Default();

    RegisterParameter("complex_type_mode", ComplexTypeMode)
        .Default(EComplexTypeMode::Named);
    RegisterParameter("decimal_mode", DecimalMode)
        .Default(EDecimalMode::Binary);
    RegisterParameter("time_mode", TimeMode)
        .Default(ETimeMode::Binary);
    RegisterParameter("uuid_mode", UuidMode)
        .Default(EUuidMode::Binary);
}

////////////////////////////////////////////////////////////////////////////////

TWebJsonFormatConfig::TWebJsonFormatConfig()
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

////////////////////////////////////////////////////////////////////////////////

TSkiffFormatConfig::TSkiffFormatConfig()
{
    RegisterParameter("skiff_schema_registry", SkiffSchemaRegistry)
        .Default();
    RegisterParameter("table_skiff_schemas", TableSkiffSchemas);

    RegisterParameter("override_intermediate_table_schema", OverrideIntermediateTableSchema)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
