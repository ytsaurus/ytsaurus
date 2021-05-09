#include "protobuf.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/string_builder.h>

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <google/protobuf/wire_format_lite.h>

namespace NYT::NFormats {

using ::google::protobuf::Descriptor;
using ::google::protobuf::FileDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::DescriptorPool;
using ::google::protobuf::internal::WireFormatLite;

using namespace NYT::NTableClient;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

TEnumerationDescription::TEnumerationDescription(const TString& name)
    : Name_(name)
{ }

const TString& TEnumerationDescription::GetEnumerationName() const
{
    return Name_;
}

const TString& TEnumerationDescription::GetValueName(i32 value) const
{
    auto it = ValueToName_.find(value);
    if (it == ValueToName_.end()) {
        THROW_ERROR_EXCEPTION("Invalid value for enum")
            << TErrorAttribute("enum_name", GetEnumerationName())
            << TErrorAttribute("value", value);
    }
    return it->second;
}

i32 TEnumerationDescription::GetValue(TStringBuf valueName) const
{
    auto it = NameToValue_.find(valueName);
    if (it == NameToValue_.end()) {
        THROW_ERROR_EXCEPTION("Invalid value for enum")
            << TErrorAttribute("enum_name", GetEnumerationName())
            << TErrorAttribute("value", valueName);
    }
    return it->second;
}

void TEnumerationDescription::Add(TString name, i32 value)
{
    if (NameToValue_.find(name) != NameToValue_.end()) {
        THROW_ERROR_EXCEPTION("Enumeration %v already has value %v",
            Name_,
            name);
    }
    if (ValueToName_.find(value) != ValueToName_.end()) {
        THROW_ERROR_EXCEPTION("Enumeration %v already has value %v",
            Name_,
            value);
    }
    NameToValue_.emplace(name, value);
    ValueToName_.emplace(value, std::move(name));
}

////////////////////////////////////////////////////////////////////////////////

class TProtoErrorCollector
    : public DescriptorPool::ErrorCollector
{
public:
    virtual void AddError(
        const TString& fileName,
        const TString& elementName,
        const Message* /*descriptor*/,
        DescriptorPool::ErrorCollector::ErrorLocation /*location*/,
        const TString& message) override
    {
        THROW_ERROR_EXCEPTION("Error while building protobuf descriptors: %v", message)
            << TErrorAttribute("file_name", fileName)
            << TErrorAttribute("element_name", elementName);
    }
};

////////////////////////////////////////////////////////////////////////////////

static TEnumerationDescription CreateEnumerationMap(
    const TString& enumName,
    const IMapNodePtr& enumerationConfig)
{
    TEnumerationDescription result(enumName);
    for (const auto& enumValue : enumerationConfig->GetChildren()) {
        const auto& name = enumValue.first;
        const auto& valueNode = enumValue.second;
        switch (valueNode->GetType()) {
            case ENodeType::Uint64:
                result.Add(name, valueNode->GetValue<ui64>());
                break;
            case ENodeType::Int64:
                result.Add(name, valueNode->GetValue<i64>());
                break;
            default:
                THROW_ERROR_EXCEPTION("Invalid specification of %Qv enumeration; enumeration value expected type Int64 or Uint64 actual type: %v",
                    enumName,
                    valueNode->GetType());
        }
    }
    return result;
}

static std::optional<WireFormatLite::FieldType> ConvertFromInternalProtobufType(EProtobufType type)
{
    auto fieldType = [=] () -> std::optional<::google::protobuf::FieldDescriptor::Type>{
        using namespace ::google::protobuf;
        switch (type) {
            case EProtobufType::Double:
                return FieldDescriptor::TYPE_DOUBLE;
            case EProtobufType::Float:
                return FieldDescriptor::TYPE_FLOAT;

            case EProtobufType::Int64:
                return FieldDescriptor::TYPE_INT64;
            case EProtobufType::Uint64:
                return FieldDescriptor::TYPE_UINT64;
            case EProtobufType::Sint64:
                return FieldDescriptor::TYPE_SINT64;
            case EProtobufType::Fixed64:
                return FieldDescriptor::TYPE_FIXED64;
            case EProtobufType::Sfixed64:
                return FieldDescriptor::TYPE_SFIXED64;

            case EProtobufType::Int32:
                return FieldDescriptor::TYPE_INT32;
            case EProtobufType::Uint32:
                return FieldDescriptor::TYPE_UINT32;
            case EProtobufType::Sint32:
                return FieldDescriptor::TYPE_SINT32;
            case EProtobufType::Fixed32:
                return FieldDescriptor::TYPE_FIXED32;
            case EProtobufType::Sfixed32:
                return FieldDescriptor::TYPE_SFIXED32;

            case EProtobufType::Bool:
                return FieldDescriptor::TYPE_BOOL;
            case EProtobufType::String:
                return FieldDescriptor::TYPE_STRING;
            case EProtobufType::Bytes:
                return FieldDescriptor::TYPE_BYTES;

            case EProtobufType::EnumInt:
            case EProtobufType::EnumString:
                return FieldDescriptor::TYPE_ENUM;

            case EProtobufType::Message:
            case EProtobufType::StructuredMessage:
                return FieldDescriptor::TYPE_MESSAGE;

            case EProtobufType::Any:
            case EProtobufType::OtherColumns:
                return FieldDescriptor::TYPE_BYTES;

            case EProtobufType::Oneof:
                return std::nullopt;
        }
        YT_ABORT();
    }();
    if (fieldType) {
        return static_cast<WireFormatLite::FieldType>(*fieldType);
    }
    return std::nullopt;
}

static std::optional<EProtobufType> ConvertToInternalProtobufType(
    ::google::protobuf::FieldDescriptor::Type type,
    bool enumsAsStrings)
{
    using namespace ::google::protobuf;
    switch (type) {
        case FieldDescriptor::TYPE_DOUBLE:
            return EProtobufType::Double;
        case FieldDescriptor::TYPE_FLOAT:
            return EProtobufType::Float;
        case FieldDescriptor::TYPE_INT64:
            return EProtobufType::Int64;
        case FieldDescriptor::TYPE_UINT64:
            return EProtobufType::Uint64;
        case FieldDescriptor::TYPE_INT32:
            return EProtobufType::Int32;
        case FieldDescriptor::TYPE_FIXED64:
            return EProtobufType::Fixed64;
        case FieldDescriptor::TYPE_FIXED32:
            return EProtobufType::Fixed32;
        case FieldDescriptor::TYPE_BOOL:
            return EProtobufType::Bool;
        case FieldDescriptor::TYPE_STRING:
            return EProtobufType::String;
        case FieldDescriptor::TYPE_GROUP:
            return std::nullopt;
        case FieldDescriptor::TYPE_MESSAGE:
            return EProtobufType::Message;
        case FieldDescriptor::TYPE_BYTES:
            return EProtobufType::Bytes;
        case FieldDescriptor::TYPE_UINT32:
            return EProtobufType::Uint32;
        case FieldDescriptor::TYPE_ENUM:
            return enumsAsStrings ? EProtobufType::EnumString : EProtobufType::EnumInt;
        case FieldDescriptor::TYPE_SFIXED32:
            return EProtobufType::Sfixed32;
        case FieldDescriptor::TYPE_SFIXED64:
            return EProtobufType::Sfixed64;
        case FieldDescriptor::TYPE_SINT32:
            return EProtobufType::Sint32;
        case FieldDescriptor::TYPE_SINT64:
            return EProtobufType::Sint64;
    }
    return std::nullopt;
}

static TEnumerationDescription ConvertToEnumMap(const ::google::protobuf::EnumDescriptor& enumDescriptor)
{
    TEnumerationDescription result(enumDescriptor.full_name());
    for (int i = 0; i < enumDescriptor.value_count(); ++i) {
        auto valueDescriptor = enumDescriptor.value(i);
        YT_VERIFY(valueDescriptor);
        result.Add(valueDescriptor->name(), valueDescriptor->number());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

ui32 TProtobufFieldDescriptionBase::GetFieldNumber() const
{
    return WireFormatLite::GetTagFieldNumber(WireTag);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_ENUM(EIntegerSignednessKind,
    (SignedInteger)
    (UnsignedInteger)
    (Other)
);

} // namespace

void ValidateSimpleType(
    EProtobufType protobufType,
    NTableClient::ESimpleLogicalValueType logicalType)
{
    if (logicalType == ESimpleLogicalValueType::Any) {
        return;
    }

    using EKind = EIntegerSignednessKind;

    auto getLogicalTypeKind = [] (ESimpleLogicalValueType type) {
        switch (type) {
            case ESimpleLogicalValueType::Int8:
            case ESimpleLogicalValueType::Int16:
            case ESimpleLogicalValueType::Int32:
            case ESimpleLogicalValueType::Int64:
                return EKind::SignedInteger;
            case ESimpleLogicalValueType::Uint8:
            case ESimpleLogicalValueType::Uint16:
            case ESimpleLogicalValueType::Uint32:
            case ESimpleLogicalValueType::Uint64:
                return EKind::UnsignedInteger;
            default:
                return EKind::Other;
        }
    };

    auto getProtobufTypeKind = [] (EProtobufType type) {
        switch (type) {
            case EProtobufType::Fixed64:
            case EProtobufType::Uint64:
            case EProtobufType::Uint32:
            case EProtobufType::Fixed32:
                return EKind::UnsignedInteger;

            case EProtobufType::Int64:
            case EProtobufType::Sint64:
            case EProtobufType::Sfixed64:
            case EProtobufType::Sfixed32:
            case EProtobufType::Sint32:
            case EProtobufType::Int32:
            case EProtobufType::EnumInt:
                return EKind::SignedInteger;

            default:
                return EKind::Other;
        }
    };

    auto throwMismatchError = [&] (TStringBuf message) {
        THROW_ERROR_EXCEPTION("Simple logical type %Qlv and protobuf type %Qlv mismatch: %v",
            logicalType,
            protobufType,
            message);
    };

    auto validateLogicalType = [&] (auto... expectedTypes) {
        if ((... && (logicalType != expectedTypes))) {
            auto typeNameList = std::vector<TString>{FormatEnum(expectedTypes)...};
            throwMismatchError(Format("expected logical type to be one of %v", typeNameList));
        }
    };

    switch (protobufType) {
        case EProtobufType::String:
            validateLogicalType(ESimpleLogicalValueType::String, ESimpleLogicalValueType::Utf8);
            return;

        case EProtobufType::Bytes:
        case EProtobufType::Message:
            validateLogicalType(ESimpleLogicalValueType::String);
            return;

        case EProtobufType::Fixed64:
        case EProtobufType::Uint64:
        case EProtobufType::Uint32:
        case EProtobufType::Fixed32:
        case EProtobufType::Int64:
        case EProtobufType::Sint64:
        case EProtobufType::Sfixed64:
        case EProtobufType::Sfixed32:
        case EProtobufType::Sint32:
        case EProtobufType::Int32: {
            auto logicalTypeKind = getLogicalTypeKind(logicalType);
            if (logicalTypeKind == EIntegerSignednessKind::Other) {
                throwMismatchError("integer protobuf type can match only integer type in schema");
            }
            if (logicalTypeKind != getProtobufTypeKind(protobufType)) {
                throwMismatchError("signedness of both types must be the same");
            }
            return;
        }

        case EProtobufType::Float:
            validateLogicalType(ESimpleLogicalValueType::Float, ESimpleLogicalValueType::Double);
            return;

        case EProtobufType::Double:
            validateLogicalType(ESimpleLogicalValueType::Double);
            return;

        case EProtobufType::Bool:
            validateLogicalType(ESimpleLogicalValueType::Boolean);
            return;

        case EProtobufType::EnumInt:
            if (getLogicalTypeKind(logicalType) != EKind::SignedInteger) {
                throwMismatchError("logical type must be signed integer type");
            }
            return;
        case EProtobufType::EnumString:
            if (logicalType != ESimpleLogicalValueType::String &&
                getLogicalTypeKind(logicalType) != EKind::SignedInteger)
            {
                throwMismatchError("logical type must be either signed integer type or string");
            }
            return;

        case EProtobufType::Any:
            // No check here: every type would be OK.
            return;

        case EProtobufType::StructuredMessage:
        case EProtobufType::OtherColumns:
        case EProtobufType::Oneof:
            throwMismatchError("protobuf type cannot match any simple type");
    }
    YT_ABORT();
}

static bool CanBePacked(EProtobufType type)
{
    auto wireFieldType = ConvertFromInternalProtobufType(type);
    if (!wireFieldType) {
        return false;
    }
    auto wireType = WireFormatLite::WireTypeForFieldType(*wireFieldType);
    return
        wireType == WireFormatLite::WireType::WIRETYPE_FIXED32 ||
        wireType == WireFormatLite::WireType::WIRETYPE_FIXED64 ||
        wireType == WireFormatLite::WireType::WIRETYPE_VARINT;
}

void TProtobufFormatDescriptionBase::Init(
    const TProtobufFormatConfigPtr& config,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas)
{
    const bool tablesSpecified = !config->Tables.empty();
    const bool fileDescriptorSetSpecified = !config->FileDescriptorSet.empty();
    if (tablesSpecified && fileDescriptorSetSpecified) {
        THROW_ERROR_EXCEPTION(R"("tables" and "file_descriptor_set" must not be used together in protobuf format)");
    } else if (tablesSpecified) {
        InitFromProtobufSchema(config, schemas);
    } else if (fileDescriptorSetSpecified) {
        InitFromFileDescriptors(config);
    } else {
        THROW_ERROR_EXCEPTION(R"("tables" attribute is not specified in protobuf format)");
    }
}

void TProtobufFormatDescriptionBase::InitFromFileDescriptors(const TProtobufFormatConfigPtr& config)
{
    if (config->FileIndices.empty()) {
        THROW_ERROR_EXCEPTION(R"("file_indices" attribute must be non empty in protobuf format)");
    }
    if (config->MessageIndices.size() != config->FileIndices.size()) {
        THROW_ERROR_EXCEPTION(R"("message_indices" and "file_indices" must be of same size in protobuf format)");
    }

    ::google::protobuf::FileDescriptorSet fileDescriptorSet;
    if (!fileDescriptorSet.ParseFromString(config->FileDescriptorSet)) {
        THROW_ERROR_EXCEPTION(R"(Error parsing "file_descriptor_set" in protobuf config)");
    }

    std::vector<const FileDescriptor*> fileDescriptors;
    TProtoErrorCollector errorCollector;
    DescriptorPool descriptorPool;
    for (const auto& fileDescriptorProto : fileDescriptorSet.file()) {
        auto* file = descriptorPool.BuildFileCollectingErrors(
            fileDescriptorProto,
            &errorCollector);

        if (!file) {
            THROW_ERROR_EXCEPTION("Error building file %v",
                fileDescriptorProto.name());
        }

        fileDescriptors.push_back(file);
    }

    const bool enumsAsStrings = config->EnumsAsStrings;
    std::vector<const Descriptor*> messageDescriptors;
    for (size_t i = 0; i < config->FileIndices.size(); ++i) {
        if (config->FileIndices[i] >= static_cast<int>(fileDescriptors.size())) {
            THROW_ERROR_EXCEPTION("File index is out of bound")
                << TErrorAttribute("file_index", config->FileIndices[i])
                << TErrorAttribute("file_count", fileDescriptors.size());
        }
        auto* fileDescriptor = fileDescriptors[config->FileIndices[i]];

        if (config->MessageIndices[i] >= fileDescriptor->message_type_count()) {
            THROW_ERROR_EXCEPTION("Message index is out of bound")
                << TErrorAttribute("message_index", config->MessageIndices[i])
                << TErrorAttribute("message_count", fileDescriptor->message_type_count());
        }
        auto* messageDescriptor = fileDescriptor->message_type(config->MessageIndices[i]);

        AddTable();

        // messageDescriptor -> columns
        int fieldCount = messageDescriptor->field_count();
        for (int fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex) {
            const auto* fieldDescriptor = messageDescriptor->field(fieldIndex);

            auto columnName = fieldDescriptor->options().GetExtension(column_name);
            if (columnName.empty()) {
                columnName = fieldDescriptor->options().GetExtension(key_column_name);
                if (columnName.empty()) {
                    columnName = fieldDescriptor->name();
                }
            }

            auto optionalType = ConvertToInternalProtobufType(fieldDescriptor->type(), enumsAsStrings);
            if (!optionalType) {
                continue;
            }

            auto columnConfig = New<TProtobufColumnConfig>();
            columnConfig->Name = columnName;
            columnConfig->ProtoType = *optionalType;
            columnConfig->FieldNumber = fieldDescriptor->number();

            if (columnConfig->ProtoType == EProtobufType::EnumString || columnConfig->ProtoType == EProtobufType::EnumInt) {
                auto enumDescriptor = fieldDescriptor->enum_type();
                YT_VERIFY(enumDescriptor);
                auto enumName = enumDescriptor->full_name();

                auto it = EnumerationDescriptionMap_.find(enumName);
                if (it == EnumerationDescriptionMap_.end()) {
                    YT_VERIFY(EnumerationDescriptionMap_.emplace(enumName, ConvertToEnumMap(*enumDescriptor)).second);
                }

                columnConfig->EnumerationName = enumName;
            }

            auto field = AddField(
                /* tableIndex */ i,
                /* parent */ nullptr,
                fieldIndex,
                columnConfig);

            if (columnConfig->EnumerationName && !field->EnumerationDescription) {
                field->EnumerationDescription = &EnumerationDescriptionMap_.at(*columnConfig->EnumerationName);
            }
        }
    }
}

[[noreturn]] void ThrowSchemaMismatch(
    TStringBuf message,
    const TComplexTypeFieldDescriptor& descriptor,
    const TProtobufColumnConfigPtr& protoConfig)
{
    THROW_ERROR_EXCEPTION("Table schema and protobuf format config mismatch at %v: %v",
        descriptor.GetDescription(),
        message)
        << TErrorAttribute("type_in_schema", ToString(*descriptor.GetType()))
        << TErrorAttribute("protobuf_config", protoConfig);
}

void TProtobufFormatDescriptionBase::InitFromProtobufSchema(
    const TProtobufFormatConfigPtr& config,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas)
{
    if (config->Enumerations) {
        const auto& enumerationConfigMap = config->Enumerations;
        for (const auto& [name, field] : enumerationConfigMap->GetChildren()) {
            if (field->GetType() != ENodeType::Map) {
                THROW_ERROR_EXCEPTION(R"(Invalid enumeration specification type: expected "map", found %Qlv)",
                    field->GetType());
            }
            const auto& enumerationConfig = field->AsMap();
            EnumerationDescriptionMap_.emplace(name, CreateEnumerationMap(name, enumerationConfig));
        }
    }

    const auto& tableConfigs = config->Tables;
    if (tableConfigs.size() < schemas.size()) {
        THROW_ERROR_EXCEPTION("Number of schemas is greater than number of tables in protobuf config: %v > %v",
            schemas.size(),
            tableConfigs.size());
    }

    for (size_t tableIndex = 0; tableIndex != schemas.size(); ++tableIndex) {
        AddTable();
        int fieldIndex = 0;
        const auto& tableConfig = tableConfigs[tableIndex];
        const auto& tableSchema = schemas[tableIndex];
        for (const auto& columnConfig : tableConfig->Columns) {
            auto columnSchema = tableSchema->FindColumn(columnConfig->Name);
            TLogicalTypePtr logicalType = columnSchema ? columnSchema->LogicalType() : nullptr;
            if (columnConfig->ProtoType == EProtobufType::OtherColumns) {
                if (columnConfig->Repeated) {
                    THROW_ERROR_EXCEPTION("Protobuf field %Qv of type %Qlv can not be repeated",
                        columnConfig->Name,
                        EProtobufType::OtherColumns);
                }
                if (logicalType) {
                    THROW_ERROR_EXCEPTION("Protobuf field %Qv of type %Qlv should not match actual column in schema",
                        columnConfig->Name,
                        EProtobufType::OtherColumns);
                }
            }

            bool needSchema = columnConfig->Repeated
                || columnConfig->ProtoType == EProtobufType::StructuredMessage
                || columnConfig->ProtoType == EProtobufType::Oneof;
            if (!logicalType && needSchema) {
                if (tableSchema->GetColumnCount() > 0) {
                    // Ignore missing column to facilitate schema evolution.
                    IgnoreField(tableIndex, /* parent */ nullptr, columnConfig);
                    continue;
                }
                THROW_ERROR_EXCEPTION(
                    "Field %Qv of type %Qlv requires a corresponding schematized column",
                    columnConfig->Name,
                    columnConfig->ProtoType);
            }

            auto field = AddField(tableIndex, /* parent */ nullptr, fieldIndex, columnConfig);
            ++fieldIndex;
            if (logicalType) {
                YT_VERIFY(columnSchema);
                Traverse(
                    tableIndex,
                    field,
                    columnConfig,
                    TComplexTypeFieldDescriptor(*columnSchema));
            }
        }
    }
}

TProtobufFieldDescriptionBase* TProtobufFormatDescriptionBase::InitField(
    TProtobufFieldDescriptionBase* field,
    int structFieldIndex,
    const TProtobufColumnConfigPtr& columnConfig)
{
    field->Name = columnConfig->Name;
    field->Type = columnConfig->ProtoType;
    field->Repeated = columnConfig->Repeated;
    field->Packed = columnConfig->Packed;
    field->StructFieldIndex = structFieldIndex;

    if (auto wireFieldType = ConvertFromInternalProtobufType(field->Type)) {
        YT_VERIFY(columnConfig->FieldNumber);
        field->TagSize = WireFormatLite::TagSize(*columnConfig->FieldNumber, *wireFieldType);
        WireFormatLite::WireType wireType;
        if (columnConfig->Packed) {
            wireType = WireFormatLite::WireType::WIRETYPE_LENGTH_DELIMITED;
        } else {
            wireType = WireFormatLite::WireTypeForFieldType(*wireFieldType);
        }
        field->WireTag = WireFormatLite::MakeTag(*columnConfig->FieldNumber, wireType);
    }

    if (field->Type == EProtobufType::EnumString) {
        if (!columnConfig->EnumerationName) {
            THROW_ERROR_EXCEPTION("Invalid format: \"enumeration_name\" for column or field %Qv is not specified",
                columnConfig->Name);
        }
        auto it = EnumerationDescriptionMap_.find(*columnConfig->EnumerationName);
        if (it == EnumerationDescriptionMap_.end()) {
            THROW_ERROR_EXCEPTION("Invalid format: cannot find enumeration with name %Qv",
                *columnConfig->EnumerationName);
        }
        field->EnumerationDescription = &it->second;
    }

    return field;
}

void TProtobufFormatDescriptionBase::Traverse(
    int tableIndex,
    TProtobufFieldDescriptionBase* field,
    const TProtobufColumnConfigPtr& columnConfig,
    TComplexTypeFieldDescriptor descriptor)
{
    if (columnConfig->ProtoType == EProtobufType::OtherColumns) {
        ThrowSchemaMismatch(
            "protobuf field of type \"other_columns\" is not allowed inside complex types",
            descriptor,
            columnConfig);
    }

    if (columnConfig->Packed && !columnConfig->Repeated) {
        THROW_ERROR_EXCEPTION("Field %Qv is marked \"packed\" but is not marked \"repeated\"",
            columnConfig->Name);
    }

    if (field->Packed && !CanBePacked(field->Type)) {
        ThrowSchemaMismatch(
            Format("packed protobuf field must have primitive numeric type, got %Qlv", field->Type),
            descriptor,
            columnConfig);
    }

    if (descriptor.GetType()->GetMetatype() == ELogicalMetatype::Optional) {
        descriptor = descriptor.OptionalElement();
        field->Optional = true;
    } else {
        field->Optional = false;
    }

    if (field->Repeated) {
        if (descriptor.GetType()->GetMetatype() == ELogicalMetatype::Dict) {
            // Do nothing, this case will be processed in the following switch.
        } else if (descriptor.GetType()->GetMetatype() == ELogicalMetatype::List) {
            descriptor = descriptor.ListElement();
        } else {
            ThrowSchemaMismatch(
                Format("repeated field must correspond to list or dict, got %Qlv", descriptor.GetType()->GetMetatype()),
                descriptor,
                columnConfig);
        }
    }

    const auto& logicalType = descriptor.GetType();
    YT_VERIFY(logicalType);

    // list<optional<any>> is allowed.
    if (field->Repeated &&
        field->Type == EProtobufType::Any &&
        logicalType->GetMetatype() == ELogicalMetatype::Optional &&
        logicalType->GetElement()->GetMetatype() == ELogicalMetatype::Simple &&
        logicalType->GetElement()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Any)
    {
        return;
    }

    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Struct:
            if (columnConfig->ProtoType != EProtobufType::StructuredMessage) {
                ThrowSchemaMismatch(
                    Format("expected \"structured_message\" protobuf type, got %Qlv", columnConfig->ProtoType),
                    descriptor,
                    columnConfig);
            }
            TraverseStruct(tableIndex, field, columnConfig, descriptor);
            break;
        case ELogicalMetatype::VariantStruct:
            if (columnConfig->ProtoType != EProtobufType::Oneof) {
                ThrowSchemaMismatch(
                    Format("expected \"oneof\" protobuf type, got %Qlv", columnConfig->ProtoType),
                    descriptor,
                    columnConfig);
            }
            TraverseStruct(tableIndex, field, columnConfig, descriptor);
            break;
        case ELogicalMetatype::Dict:
            TraverseDict(tableIndex, field, columnConfig, descriptor);
            break;
        case ELogicalMetatype::Simple:
            ValidateSimpleType(field->Type, logicalType->AsSimpleTypeRef().GetElement());
            break;
        default:
            ThrowSchemaMismatch(
                Format("unexpected logical metatype %Qlv", logicalType->GetMetatype()),
                descriptor,
                columnConfig);

    }
}

void TProtobufFormatDescriptionBase::TraverseStruct(
    int tableIndex,
    TProtobufFieldDescriptionBase* parent,
    const TProtobufColumnConfigPtr& columnConfig,
    NTableClient::TComplexTypeFieldDescriptor descriptor)
{
    YT_VERIFY(
        columnConfig->ProtoType == EProtobufType::StructuredMessage ||
        columnConfig->ProtoType == EProtobufType::Oneof);

    const auto isOneof = (columnConfig->ProtoType == EProtobufType::Oneof);

    THashMap<TStringBuf, TProtobufColumnConfigPtr> nameToConfig;
    for (const auto& config : columnConfig->Fields) {
        auto inserted = nameToConfig.emplace(config->Name, config).second;
        if (!inserted) {
            ThrowSchemaMismatch(
                Format("multiple fields with same name (%Qv) are forbidden", config->Name),
                descriptor,
                columnConfig);
        }
    }

    auto processFieldMissingFromConfig = [&] (const TStructField& structField) {
        if (isOneof) {
            // It is OK, we will ignore this alternative when writing.
            return;
        }
        if (structField.Type->GetMetatype() != ELogicalMetatype::Optional && !NonOptionalMissingFieldsAllowed()) {
            ThrowSchemaMismatch(
                Format("non-optional field %Qv in schema is missing from protobuf config", structField.Name),
                descriptor,
                columnConfig);
        }
    };

    const auto& structFields = descriptor.GetType()->GetFields();
    if (!isOneof) {
        parent->StructFieldCount = static_cast<int>(structFields.size());
    }
    for (int fieldIndex = 0; fieldIndex != static_cast<int>(structFields.size()); ++fieldIndex) {
        const auto& structField = structFields[fieldIndex];
        auto configIt = nameToConfig.find(structField.Name);
        if (configIt == nameToConfig.end()) {
            processFieldMissingFromConfig(structField);
            continue;
        }
        auto childDescriptor = isOneof
            ? descriptor.VariantStructField(fieldIndex)
            : descriptor.StructField(fieldIndex);
        if (isOneof && childDescriptor.GetType()->IsNullable()) {
            THROW_ERROR_EXCEPTION("Optional variant field %Qv can not match oneof field in protobuf format",
                childDescriptor.GetDescription());
        }
        auto child = AddField(tableIndex, parent, fieldIndex, configIt->second);
        Traverse(tableIndex, child, configIt->second, childDescriptor);
        nameToConfig.erase(configIt);
    }

    for (const auto& [name, childConfig] : nameToConfig) {
        IgnoreField(tableIndex, parent, childConfig);
    }
}

void TProtobufFormatDescriptionBase::TraverseDict(
    int tableIndex,
    TProtobufFieldDescriptionBase* field,
    const TProtobufColumnConfigPtr& columnConfig,
    TComplexTypeFieldDescriptor descriptor)
{
    YT_VERIFY(field->Repeated);
    if (field->Type != EProtobufType::StructuredMessage) {
        ThrowSchemaMismatch(
            Format("expected protobuf field of type %Qlv to match %Qlv type in schema",
                EProtobufType::StructuredMessage,
                ELogicalMetatype::Dict),
            descriptor,
            columnConfig);
    }
    const auto& fields = columnConfig->Fields;
    if (fields.size() != 2 ||
        fields[0]->Name != "key" ||
        fields[1]->Name != "value")
    {
        ThrowSchemaMismatch(
            Format(
                "expected protobuf message with exactly fields \"key\" and \"value\" "
                "to match %Qlv type in schema",
                ELogicalMetatype::Dict),
            descriptor,
            columnConfig);
    }
    field->StructFieldCount = 2;
    const auto& keyConfig = fields[0];
    const auto& valueConfig = fields[1];
    auto keyField = AddField(tableIndex, field, /* fieldIndex */ 0, keyConfig);
    Traverse(tableIndex, keyField, keyConfig, descriptor.DictKey());
    auto valueField = AddField(tableIndex, field, /* fieldIndex */ 1, valueConfig);
    Traverse(tableIndex, valueField, valueConfig, descriptor.DictValue());
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
static T& ResizeAndGetElement(std::vector<T>& vector, int index, const T& fill = {})
{
    if (index >= static_cast<int>(vector.size())) {
        vector.resize(index + 1, fill);
    }
    return vector[index];
}

TProtobufWriterFieldDescription* TProtobufWriterFieldDescription::AddChild(int fieldIndex)
{
    if (Type == EProtobufType::Oneof) {
        ResizeAndGetElement(AlternativeToChildIndex_, fieldIndex, InvalidChildIndex) = Children.size();
    }
    Children.push_back(std::make_unique<TProtobufWriterFieldDescription>());
    return Children.back().get();
}

const TProtobufWriterFieldDescription* TProtobufWriterFieldDescription::FindAlternative(int alternativeIndex) const
{
    if (alternativeIndex >= static_cast<int>(AlternativeToChildIndex_.size())) {
        return nullptr;
    }
    if (AlternativeToChildIndex_[alternativeIndex] == InvalidChildIndex) {
        return nullptr;
    }
    return Children[AlternativeToChildIndex_[alternativeIndex]].get();
}

bool TProtobufWriterFormatDescription::NonOptionalMissingFieldsAllowed() const
{
    return true;
}

void TProtobufWriterFormatDescription::AddTable()
{
    Tables_.emplace_back();
}

void TProtobufWriterFormatDescription::IgnoreField(
    int /*tableIndex*/,
    TProtobufFieldDescriptionBase* /*parent*/,
    const TProtobufColumnConfigPtr& /*columnConfig*/)
{ }

TProtobufFieldDescriptionBase* TProtobufWriterFormatDescription::AddField(
    int tableIndex,
    TProtobufFieldDescriptionBase* parent,
    int fieldIndex,
    const TProtobufColumnConfigPtr& columnConfig)
{
    TProtobufWriterFieldDescription* child;
    if (parent) {
        auto typedParent = static_cast<TProtobufWriterFieldDescription*>(parent);
        child = typedParent->AddChild(fieldIndex);
    } else {
        YT_VERIFY(tableIndex < static_cast<int>(Tables_.size()));
        auto& table = Tables_[tableIndex];
        auto [it, inserted] = table.Columns.emplace(columnConfig->Name, TProtobufWriterFieldDescription{});
        if (!inserted) {
            THROW_ERROR_EXCEPTION("Multiple fields with same column name %Qv are forbidden in protobuf format",
                columnConfig->Name);
        }
        child = &it->second;
        if (columnConfig->ProtoType == EProtobufType::OtherColumns) {
            table.OtherColumnsField = child;
        }
    }

    return InitField(child, fieldIndex, columnConfig);
}

const TProtobufWriterFormatDescription::TTableDescription&
TProtobufWriterFormatDescription::GetTableDescription(int tableIndex) const
{
    if (Y_UNLIKELY(tableIndex >= std::ssize(Tables_))) {
        THROW_ERROR_EXCEPTION("Table with index %v is missing in format description",
            tableIndex);
    }
    return Tables_[tableIndex];
}

const TProtobufWriterFieldDescription* TProtobufWriterFormatDescription::FindField(
    int tableIndex,
    int fieldIndex,
    const TNameTablePtr& nameTable) const
{
    const auto& table = GetTableDescription(tableIndex);
    auto currentSize = static_cast<int>(table.FieldIndexToDescription.size());
    if (currentSize <= fieldIndex) {
        table.FieldIndexToDescription.reserve(fieldIndex + 1);
        for (int i = currentSize; i <= fieldIndex; ++i) {
            YT_ASSERT(static_cast<int>(table.FieldIndexToDescription.size()) == i);
            auto fieldName = nameTable->GetName(i);
            auto it = table.Columns.find(fieldName);
            if (it == table.Columns.end()) {
                table.FieldIndexToDescription.push_back(nullptr);
            } else {
                table.FieldIndexToDescription.emplace_back(&it->second);
            }
        }
    }
    return table.FieldIndexToDescription[fieldIndex];
}

int TProtobufWriterFormatDescription::GetTableCount() const
{
    return Tables_.size();
}

const TProtobufWriterFieldDescription* TProtobufWriterFormatDescription::FindOtherColumnsField(int tableIndex) const
{
    return GetTableDescription(tableIndex).OtherColumnsField;
}

////////////////////////////////////////////////////////////////////////////////

TProtobufParserFieldDescription* TProtobufParserFieldDescription::AddChild(int fieldNumber)
{
    SetChildIndex(fieldNumber, Children.size());
    Children.push_back(std::make_unique<TProtobufParserFieldDescription>());
    return Children.back().get();
}

void TProtobufParserFieldDescription::IgnoreChild(int fieldNumber)
{
    SetChildIndex(fieldNumber, IgnoredChildIndex);
}

void TProtobufParserFieldDescription::SetChildIndex(int fieldNumber, int childIndex)
{
    bool isNew = false;
    if (fieldNumber < MaxFieldNumberVectorSize) {
        auto& childIndexRef = ResizeAndGetElement(FieldNumberToChildIndexVector_, fieldNumber, InvalidChildIndex);
        isNew = (childIndexRef == InvalidChildIndex);
        childIndexRef = childIndex;
    } else {
        isNew = FieldNumberToChildIndexMap_.emplace(fieldNumber, childIndex).second;
    }
    if (!isNew) {
        THROW_ERROR_EXCEPTION("Invalid protobuf format: duplicate field number %v (child of %Qv)",
            fieldNumber,
            GetDebugString());
    }
}

std::optional<int> TProtobufParserFieldDescription::FieldNumberToChildIndex(int fieldNumber) const
{
    int index;
    if (fieldNumber < std::ssize(FieldNumberToChildIndexVector_)) {
        index = FieldNumberToChildIndexVector_[fieldNumber];
        if (Y_UNLIKELY(index == InvalidChildIndex)) {
            THROW_ERROR_EXCEPTION("Unexpected field number %v while parsing %Qv",
                fieldNumber,
                GetDebugString());
        }
    } else {
        auto it = FieldNumberToChildIndexMap_.find(fieldNumber);
        if (Y_UNLIKELY(it == FieldNumberToChildIndexMap_.end())) {
            THROW_ERROR_EXCEPTION("Unexpected field number %v while parsing %Qv",
                fieldNumber,
                GetDebugString());
        }
        index = it->second;
    }
    if (index == IgnoredChildIndex) {
        return {};
    }
    return index;
}

TString TProtobufParserFieldDescription::GetDebugString() const
{
    std::vector<const TProtobufParserFieldDescription*> reversePath;
    auto description = this;
    while (description) {
        reversePath.push_back(description);
        description = description->Parent;
    }

    TStringBuilder pathBuilder;
    for (auto it = reversePath.crbegin(); it != reversePath.crend(); ++it) {
        pathBuilder.AppendString((*it)->Name);
        if (std::next(it) != reversePath.crend()) {
            pathBuilder.AppendChar('.');
        }
    }
    return pathBuilder.Flush();
}

TProtobufParserFormatDescription::TProtobufParserFormatDescription()
{
    RootDescription_.Name = "<root>";
}

const TProtobufParserFieldDescription& TProtobufParserFormatDescription::GetRootDescription() const
{
    return RootDescription_;
}

std::vector<ui16> TProtobufParserFormatDescription::CreateRootChildColumnIds(const TNameTablePtr& nameTable) const
{
    std::vector<ui16> ids;
    ids.reserve(RootDescription_.Children.size());
    for (const auto& child: RootDescription_.Children) {
        auto name = child->Name;
        if (child->IsOneofAlternative()) {
            name = child->Parent->Name;
        }
        ids.push_back(static_cast<ui16>(nameTable->GetIdOrRegisterName(name)));
    }
    return ids;
}

bool TProtobufParserFormatDescription::NonOptionalMissingFieldsAllowed() const
{
    return false;
}

void TProtobufParserFormatDescription::AddTable()
{ }

void TProtobufParserFormatDescription::IgnoreField(
    int tableIndex,
    TProtobufFieldDescriptionBase* parent,
    const TProtobufColumnConfigPtr& columnConfig)
{
    YT_VERIFY(tableIndex == 0);
    auto actualParent = ResolveField(parent);
    if (actualParent->Type == EProtobufType::Oneof) {
        actualParent = actualParent->Parent;
    }
    if (columnConfig->FieldNumber) {
        actualParent->IgnoreChild(*columnConfig->FieldNumber);
    }
}

TProtobufFieldDescriptionBase* TProtobufParserFormatDescription::AddField(
    int tableIndex,
    TProtobufFieldDescriptionBase* parent,
    int fieldIndex,
    const TProtobufColumnConfigPtr& columnConfig)
{
    YT_VERIFY(tableIndex == 0);
    auto actualParent = ResolveField(parent);
    TProtobufParserFieldDescription* child;
    auto structFieldIndex = fieldIndex;
    if (columnConfig->ProtoType == EProtobufType::Oneof) {
        if (actualParent->Type == EProtobufType::Oneof) {
            THROW_ERROR_EXCEPTION("Invalid protobuf format: oneof group %Qv cannot have a oneof child %Qv",
                actualParent->GetDebugString(),
                columnConfig->Name);
        }
        OneofDescriptions_.push_back(std::make_unique<TProtobufParserFieldDescription>());
        child = OneofDescriptions_.back().get();
    } else if (actualParent->Type == EProtobufType::Oneof) {
        YT_VERIFY(columnConfig->FieldNumber);
        child = actualParent->Parent->AddChild(*columnConfig->FieldNumber);
        child->AlternativeIndex = fieldIndex;
        structFieldIndex = actualParent->StructFieldIndex;
    } else {
        YT_VERIFY(columnConfig->FieldNumber);
        child = actualParent->AddChild(*columnConfig->FieldNumber);
    }
    child->Parent = actualParent;
    return InitField(child, structFieldIndex, columnConfig);
}

TProtobufParserFieldDescription* TProtobufParserFormatDescription::ResolveField(TProtobufFieldDescriptionBase* parent)
{
    if (!parent) {
        return &RootDescription_;
    }
    return static_cast<TProtobufParserFieldDescription*>(parent);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
