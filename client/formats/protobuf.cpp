#include "protobuf.h"

#include <yt/client/table_client/name_table.h>

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <contrib/libs/protobuf/wire_format_lite.h>

namespace NYT::NFormats {

using ::google::protobuf::Descriptor;
using ::google::protobuf::FileDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::DescriptorPool;

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

static TEnumerationDescription CreateEnumerationMap(const TString& enumName, const NYTree::IMapNodePtr& enumerationConfig)
{
    TEnumerationDescription result(enumName);
    for (const auto& enumValue : enumerationConfig->GetChildren()) {
        const auto& name = enumValue.first;
        const auto& valueNode = enumValue.second;
        switch (valueNode->GetType()) {
            case NYTree::ENodeType::Uint64:
                result.Add(name, valueNode->GetValue<ui64>());
                break;
            case NYTree::ENodeType::Int64:
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

::google::protobuf::FieldDescriptor::Type ConvertFromInternalProtobufType(EProtobufType type)
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
            return FieldDescriptor::TYPE_MESSAGE;
    }
    Y_UNREACHABLE();
}

std::optional<EProtobufType> ConvertToInternalProtobufType(::google::protobuf::FieldDescriptor::Type type, bool enumAsStrings)
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
            return enumAsStrings ? EProtobufType::EnumString : EProtobufType::EnumInt;
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

TEnumerationDescription ConvertToEnumMap(const ::google::protobuf::EnumDescriptor& enumDescriptor)
{
    TEnumerationDescription result(enumDescriptor.full_name());
    for (int i = 0; i < enumDescriptor.value_count(); ++i) {
        auto valueDescriptor = enumDescriptor.value(i);
        YCHECK(valueDescriptor);
        result.Add(valueDescriptor->name(), valueDescriptor->number());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

ui32 TProtobufFieldDescription::GetFieldNumber() const
{
    return ::google::protobuf::internal::WireFormatLite::GetTagFieldNumber(WireTag);
}

////////////////////////////////////////////////////////////////////////////////

void TProtobufFormatDescription::Init(const TProtobufFormatConfigPtr& config)
{
    const bool tablesSpecified = !config->Tables.empty();
    const bool fileDescriptorSetSpecified = !config->FileDescriptorSet.empty();
    if (tablesSpecified && fileDescriptorSetSpecified) {
        THROW_ERROR_EXCEPTION(R"("tables" and "file_descriptor_set" must not be used together in protobuf format)");
    } else if (tablesSpecified) {
        InitFromProtobufSchema(config);
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

            auto wireFieldType = static_cast<::google::protobuf::internal::WireFormatLite::FieldType>(fieldDescriptor->type());
            auto& field = columns[columnName];
            field.Name = columnName;
            field.WireTag = google::protobuf::internal::WireFormatLite::MakeTag(
                fieldDescriptor->number(),
                ::google::protobuf::internal::WireFormatLite::WireTypeForFieldType(wireFieldType));
            field.Type = *optionalType;
            field.TagSize = ::google::protobuf::internal::WireFormatLite::TagSize(fieldDescriptor->number(), wireFieldType);

            if (field.Type == EProtobufType::EnumString || field.Type == EProtobufType::EnumInt)
            {
                auto enumDescriptor = fieldDescriptor->enum_type();
                YCHECK(enumDescriptor);
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

void TProtobufFormatDescription::InitFromProtobufSchema(const TProtobufFormatConfigPtr& config)
{
    if (config->Enumerations) {
        const auto& enumerationConfigMap = config->Enumerations;
        for (const auto& entry : enumerationConfigMap->GetChildren()) {
            const auto& name = entry.first;
            const auto& node = entry.second;
            if (node->GetType() != NYTree::ENodeType::Map) {
                THROW_ERROR_EXCEPTION(R"(Invalid enumeration specification type, expected "map" found %Qv)",
                    node->GetType());
            }
            const auto& enumerationConfig = node->AsMap();
            EnumerationDescriptionMap_.emplace(name, CreateEnumerationMap(name, enumerationConfig));
        }
    }

    const auto& tableConfigList = config->Tables;

    for (const auto& tableConfig : tableConfigList) {
        Tables_.emplace_back();
        auto& columns = Tables_.back().Columns;
        for (const auto& columnConfig : tableConfig->Columns) {
            auto& field = columns[columnConfig->Name];
            field.Name = columnConfig->Name;
            field.Type = columnConfig->ProtoType;

            auto wireFieldType = static_cast<::google::protobuf::internal::WireFormatLite::FieldType>(ConvertFromInternalProtobufType(field.Type));
            field.WireTag = ::google::protobuf::internal::WireFormatLite::MakeTag(
                columnConfig->FieldNumber,
                ::google::protobuf::internal::WireFormatLite::WireTypeForFieldType(wireFieldType));
            field.TagSize = ::google::protobuf::internal::WireFormatLite::TagSize(columnConfig->FieldNumber, wireFieldType);
            if (field.Type == EProtobufType::EnumString) {
                if (!columnConfig->EnumerationName) {
                    THROW_ERROR_EXCEPTION("Invalid format: enumeration_name for column %Qv is not specified",
                        field.Name);
                }
                auto it = EnumerationDescriptionMap_.find(*columnConfig->EnumerationName);
                if (it == EnumerationDescriptionMap_.end()) {
                    THROW_ERROR_EXCEPTION("Invalid format: cannot find enumeration with name %Qv",
                        *columnConfig->EnumerationName);
                }
                field.EnumerationDescription = &it->second;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
