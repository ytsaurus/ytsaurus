#include "protobuf.h"

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <contrib/libs/protobuf/wire_format_lite.h>

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
        const Message* descriptor,
        DescriptorPool::ErrorCollector::ErrorLocation location,
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

static ::google::protobuf::FieldDescriptor::Type ConvertFromInternalProtobufType(EProtobufType type)
{
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
    }
    YT_ABORT();
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
            if (getLogicalTypeKind(logicalType) != getProtobufTypeKind(protobufType)) {
                throwMismatchError("signedness of both types must be the same");
            }
            return;
        }

        case EProtobufType::Float:
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
            throwMismatchError("protobuf type cannot match any simple type");
    }
    YT_ABORT();
}

static bool CanBePacked(EProtobufType type)
{
    auto wireFieldType = static_cast<WireFormatLite::FieldType>(ConvertFromInternalProtobufType(type));
    auto wireType = WireFormatLite::WireTypeForFieldType(wireFieldType);
    return
        wireType == WireFormatLite::WireType::WIRETYPE_FIXED32 ||
        wireType == WireFormatLite::WireType::WIRETYPE_FIXED64 ||
        wireType == WireFormatLite::WireType::WIRETYPE_VARINT;
}

void TProtobufFormatDescription::Init(
    const TProtobufFormatConfigPtr& config,
    const std::vector<NTableClient::TTableSchema>& schemas,
    bool validateMissingFieldsOptionality)
{
    const bool tablesSpecified = !config->Tables.empty();
    const bool fileDescriptorSetSpecified = !config->FileDescriptorSet.empty();
    if (tablesSpecified && fileDescriptorSetSpecified) {
        THROW_ERROR_EXCEPTION(R"("tables" and "file_descriptor_set" must not be used together in protobuf format)");
    } else if (tablesSpecified) {
        InitFromProtobufSchema(config, schemas, validateMissingFieldsOptionality);
    } else if (fileDescriptorSetSpecified) {
        InitFromFileDescriptors(config);
    } else {
        THROW_ERROR_EXCEPTION(R"("tables" attribute is not specified in protobuf format)");
    }
}

const TProtobufTableDescription& TProtobufFormatDescription::GetTableDescription(ui32 tableIndex) const
{
    if (tableIndex >= Tables_.size()) {
        THROW_ERROR_EXCEPTION("Protobuf format does not have table with index %v",
            tableIndex);
    }
    return Tables_[tableIndex];
}

size_t TProtobufFormatDescription::GetTableCount() const
{
    return Tables_.size();
}

void TProtobufFormatDescription::InitFromFileDescriptors(const TProtobufFormatConfigPtr& config)
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

        Tables_.emplace_back();
        auto& columns = Tables_.back().Columns;

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

            auto [fieldIt, inserted] = columns.emplace(columnName, TProtobufFieldDescription{});
            if (!inserted) {
                THROW_ERROR_EXCEPTION("Multiple fields with same column name %Qv are forbidden in protobuf format",
                    columnName);
            }
            auto& field = fieldIt->second;

            field.Name = columnName;
            auto wireFieldType = static_cast<WireFormatLite::FieldType>(fieldDescriptor->type());
            field.WireTag = WireFormatLite::MakeTag(
                fieldDescriptor->number(),
                WireFormatLite::WireTypeForFieldType(wireFieldType));
            field.Type = *optionalType;
            field.TagSize = WireFormatLite::TagSize(fieldDescriptor->number(), wireFieldType);

            if (field.Type == EProtobufType::EnumString || field.Type == EProtobufType::EnumInt) {
                auto enumDescriptor = fieldDescriptor->enum_type();
                YT_VERIFY(enumDescriptor);
                auto enumName = enumDescriptor->full_name();

                auto it = EnumerationDescriptionMap_.find(enumName);
                if (it == EnumerationDescriptionMap_.end()) {
                    it = EnumerationDescriptionMap_.emplace(
                        enumName,
                        ConvertToEnumMap(*enumDescriptor)).first;
                }
                field.EnumerationDescription = &it->second;
            }
        }
    }
}

void TProtobufFormatDescription::InitFromProtobufSchema(
    const TProtobufFormatConfigPtr& config,
    const std::vector<NTableClient::TTableSchema>& schemas,
    bool validateMissingFieldsOptionality)
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
        const auto& tableConfig = tableConfigs[tableIndex];
        const auto& tableSchema = schemas[tableIndex];
        auto& columns = Tables_.emplace_back().Columns;
        for (const auto& columnConfig : tableConfig->Columns) {
            auto [fieldIt, inserted] = columns.emplace(columnConfig->Name, TProtobufFieldDescription{});
            if (!inserted) {
                THROW_ERROR_EXCEPTION("Multiple fields with same column name %Qv are forbidden in protobuf format",
                    columnConfig->Name);
            }
            auto columnSchema = tableSchema.FindColumn(columnConfig->Name);
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

            bool needSchema = columnConfig->Repeated || columnConfig->ProtoType == EProtobufType::StructuredMessage;
            if (!logicalType && needSchema) {
                THROW_ERROR_EXCEPTION("Schema is required for repeated and %Qlv protobuf fields",
                    EProtobufType::StructuredMessage);
            }

            if (columnConfig->Packed && !columnConfig->Repeated) {
                THROW_ERROR_EXCEPTION("Field %Qv is marked \"packed\" but is not marked \"repeated\"",
                    columnConfig->Name);
            }

            if (logicalType) {
                InitField(
                    &fieldIt->second,
                    columnConfig,
                    logicalType,
                    /* elementIndex */ 0,
                    validateMissingFieldsOptionality);
            } else {
                InitSchemalessField(&fieldIt->second, columnConfig);
            }
        }
    }
}

void TProtobufFormatDescription::InitSchemalessField(
    TProtobufFieldDescription* field,
    const TProtobufColumnConfigPtr& columnConfig)
{
    field->Name = columnConfig->Name;
    field->Type = columnConfig->ProtoType;
    field->Repeated = columnConfig->Repeated;
    field->Packed = columnConfig->Packed;

    auto wireFieldType = static_cast<WireFormatLite::FieldType>(
        ConvertFromInternalProtobufType(field->Type));
    field->TagSize = WireFormatLite::TagSize(
        columnConfig->FieldNumber,
        wireFieldType);
    WireFormatLite::WireType wireType;
    if (columnConfig->Packed) {
        wireType = WireFormatLite::WireType::WIRETYPE_LENGTH_DELIMITED;
    } else {
        wireType = WireFormatLite::WireTypeForFieldType(wireFieldType);
    }
    field->WireTag = WireFormatLite::MakeTag(columnConfig->FieldNumber, wireType);


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
}

void TProtobufFormatDescription::InitField(
    TProtobufFieldDescription* field,
    const TProtobufColumnConfigPtr& columnConfig,
    TLogicalTypePtr logicalType,
    int elementIndex,
    bool validateMissingFieldsOptionality)
{
    YT_VERIFY(logicalType);

    std::vector<TErrorAttribute> errorAttributes = {
        {"config", columnConfig},
        {"logical_type", logicalType},
    };

    if (columnConfig->ProtoType == EProtobufType::OtherColumns) {
        THROW_ERROR_EXCEPTION("Protobuf field of type %Qlv is not allowed inside complex types",
            EProtobufType::OtherColumns)
            << errorAttributes;
    }

    InitSchemalessField(field, columnConfig);

    field->StructElementIndex = elementIndex;

    if (field->Packed && !CanBePacked(field->Type)) {
        THROW_ERROR_EXCEPTION("Packed protobuf field %Qv must have primitive numeric type, got %Qlv",
            field->Name,
            field->Type)
            << errorAttributes;
    }

    if (logicalType->GetMetatype() == ELogicalMetatype::Optional) {
        logicalType = logicalType->AsOptionalTypeRef().GetElement();
        field->Optional = true;
    } else {
        field->Optional = false;
    }

    if (field->Repeated) {
        if (field->Optional) {
            THROW_ERROR_EXCEPTION("Optional list is not supported in protobuf")
                << errorAttributes;
        }
        if (logicalType->GetMetatype() != ELogicalMetatype::List) {
            THROW_ERROR_EXCEPTION("Schema and protobuf config mismatch: expected metatype %Qlv, got %Qlv",
                ELogicalMetatype::List,
                logicalType->GetMetatype())
                << errorAttributes;
        }
        logicalType = logicalType->AsListTypeRef().GetElement();
    }

    // list<optional<any>> is allowed.
    if (field->Repeated &&
        field->Type == EProtobufType::Any &&
        logicalType->GetMetatype() == ELogicalMetatype::Optional &&
        logicalType->AsOptionalTypeRef().GetElement()->GetMetatype() == ELogicalMetatype::Simple &&
        logicalType->AsOptionalTypeRef().GetElement()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Any)
    {
        return;
    }

    if (field->Type != EProtobufType::StructuredMessage) {
        if (logicalType->GetMetatype() != ELogicalMetatype::Simple) {
            THROW_ERROR_EXCEPTION("Schema and protobuf config mismatch: expected metatype %Qlv, got %Qlv",
                ELogicalMetatype::Simple,
                logicalType->GetMetatype())
                << errorAttributes;
        }
        ValidateSimpleType(field->Type, logicalType->AsSimpleTypeRef().GetElement());
        return;
    }

    if (logicalType->GetMetatype() != ELogicalMetatype::Struct) {
        THROW_ERROR_EXCEPTION("Schema and protobuf config mismatch: expected metatype %Qlv, got %Qlv",
            ELogicalMetatype::Struct,
            logicalType->GetMetatype())
            << errorAttributes;
    }

    const auto& structElements = logicalType->AsStructTypeRef().GetFields();

    THashMap<TStringBuf, TProtobufColumnConfigPtr> nameToConfig;
    for (const auto& config : columnConfig->Fields) {
        auto inserted = nameToConfig.emplace(config->Name, config).second;
        if (!inserted) {
            THROW_ERROR_EXCEPTION("Multiple fields with same column name %Qv are forbidden in protobuf format",
                config->Name)
                << errorAttributes;
        }
    }

    field->Children.reserve(columnConfig->Fields.size());
    field->StructElementCount = static_cast<int>(structElements.size());
    for (int childElementIndex = 0; childElementIndex != static_cast<int>(structElements.size()); ++childElementIndex) {
        const auto& element = structElements[childElementIndex];
        auto configIt = nameToConfig.find(element.Name);
        if (configIt == nameToConfig.end()) {
            if (validateMissingFieldsOptionality && element.Type->GetMetatype() != ELogicalMetatype::Optional) {
                THROW_ERROR_EXCEPTION("Schema and protobuf config mismatch: "
                    "non-optional field %Qv in schema is missing from protobuf config",
                    element.Name)
                    << errorAttributes;
            }
            continue;
        }
        field->Children.emplace_back();
        InitField(
            &field->Children.back(),
            configIt->second,
            element.Type,
            childElementIndex,
            validateMissingFieldsOptionality);
        nameToConfig.erase(configIt);
    }

    if (!nameToConfig.empty()) {
        std::vector<TString> notFoundKeys;
        for (const auto& [name, config] : nameToConfig) {
            notFoundKeys.push_back(Format("%Qv", name));
        }
        THROW_ERROR_EXCEPTION("Fields %v from protobuf config not found in schema",
            notFoundKeys)
            << errorAttributes;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
