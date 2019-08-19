#include "format.h"

#include "errors.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <contrib/libs/protobuf/descriptor.h>
#include <contrib/libs/protobuf/google/protobuf/descriptor.pb.h>
#include <contrib/libs/protobuf/messagext.h>

namespace NYT {

using ::google::protobuf::Message;
using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::FileDescriptor;
using ::google::protobuf::FileDescriptorSet;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString FormatName(const TFormat& format)
{
    if (!format.Config.IsString()) {
        Y_VERIFY(format.Config.IsUndefined());
        return "<undefined>";
    }
    return format.Config.AsString();
}

TNode MakeEnumerationConfig(const ::google::protobuf::EnumDescriptor* enumDescriptor)
{
    auto config = TNode::CreateMap();
    for (int i = 0; i < enumDescriptor->value_count(); ++i) {
        config[enumDescriptor->value(i)->name()] = enumDescriptor->value(i)->number();
    }
    return config;
}

TNode MakeProtoFormatMessageFieldsConfig(
    const Descriptor* descriptor,
    TNode* enumerations);

TNode MakeProtoFormatFieldConfig(
    const FieldDescriptor* fieldDescriptor,
    TNode* enumerations,
    ESerializationMode::Enum messageSerializationMode)
{
    auto fieldConfig = TNode::CreateMap();
    fieldConfig["field_number"] = fieldDescriptor->number();
    fieldConfig["name"] = NDetail::GetColumnName(*fieldDescriptor);

    auto fieldSerializationMode = messageSerializationMode;
    if (fieldDescriptor->options().HasExtension(serialization_mode)) {
        fieldSerializationMode = fieldDescriptor->options().GetExtension(serialization_mode);
    }

    if (fieldDescriptor->is_repeated()) {
        Y_ENSURE_EX(fieldSerializationMode == ESerializationMode::YT,
            TApiUsageError() << "Repeated fields are allowed only for YT serialization mode");
    }
    fieldConfig["repeated"] = fieldDescriptor->is_repeated();

    if (fieldDescriptor->type() == FieldDescriptor::TYPE_ENUM) {
        auto* enumeration = fieldDescriptor->enum_type();
        (*enumerations)[enumeration->name()] = MakeEnumerationConfig(enumeration);
        fieldConfig["proto_type"] = "enum_string";
        fieldConfig["enumeration_name"] = enumeration->name();
    } else if (
        fieldDescriptor->type() == FieldDescriptor::TYPE_MESSAGE &&
        fieldSerializationMode == ESerializationMode::YT)
    {
        fieldConfig["proto_type"] = "structured_message";
        fieldConfig["fields"] = MakeProtoFormatMessageFieldsConfig(fieldDescriptor->message_type(), enumerations);
    } else {
        fieldConfig["proto_type"] = fieldDescriptor->type_name();
    }
    return fieldConfig;
}

TNode MakeProtoFormatMessageFieldsConfig(
    const Descriptor* descriptor,
    TNode* enumerations)
{
    TNode fields = TNode::CreateList();
    auto messageSerializationMode = descriptor->options().GetExtension(field_serialization_mode);
    for (int fieldIndex = 0; fieldIndex < descriptor->field_count(); ++fieldIndex) {
        auto* fieldDesc = descriptor->field(fieldIndex);
        fields.Add(MakeProtoFormatFieldConfig(
            fieldDesc,
            enumerations,
            messageSerializationMode));
    }
    return fields;
}

TNode MakeProtoFormatConfig(const TVector<const Descriptor*>& descriptors)
{
    TNode config("protobuf");
    config.Attributes()
        ("enumerations", TNode::CreateMap())
        ("tables", TNode::CreateList());

    auto& enumerations = config.Attributes()["enumerations"];

    for (auto* descriptor : descriptors) {
        auto columns = MakeProtoFormatMessageFieldsConfig(descriptor, &enumerations);
        config.Attributes()["tables"].Add(
            TNode()("columns", std::move(columns)));
    }

    return config;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFormat::TFormat(const TNode& config)
    : Config(config)
{ }


TFormat TFormat::Protobuf(const TVector<const ::google::protobuf::Descriptor*>& descriptors)
{
    return TFormat(MakeProtoFormatConfig(descriptors));
}

TFormat TFormat::YsonText()
{
    TNode config("yson");
    config.Attributes()("format", "text");
    return TFormat(config);
}

TFormat TFormat::YsonBinary()
{
    TNode config("yson");
    config.Attributes()("format", "binary");
    return TFormat(config);
}

TFormat TFormat::YaMRLenval()
{
    TNode config("yamr");
    config.Attributes()("lenval", true)("has_subkey", true);
    return TFormat(config);
}

bool TFormat::IsTextYson() const
{
    if (!Config.IsString() || Config.AsString() != "yson") {
        return false;
    }
    if (!Config.HasAttributes()) {
        return false;
    }
    const auto& attributes = Config.GetAttributes();
    if (!attributes.HasKey("format") || attributes["format"] != TNode("text")) {
        return false;
    }
    return true;
}

bool TFormat::IsProtobuf() const
{
    return Config.IsString() && Config.AsString() == "protobuf";
}

bool TFormat::IsYamredDsv() const
{
    return Config.IsString() && Config.AsString() == "yamred_dsv";
}

TYamredDsvAttributes TFormat::GetYamredDsvAttributes() const
{
    if (!IsYamredDsv()) {
        ythrow TApiUsageError() << "Cannot get yamred_dsv attributes for " << FormatName(*this) << " format";
    }
    TYamredDsvAttributes attributes;

    const auto& nodeAttributes = Config.GetAttributes();
    {
        const auto& keyColumns = nodeAttributes["key_column_names"];
        if (!keyColumns.IsList()) {
            ythrow yexception() << "Ill-formed format: key_column_names is of non-list type: " << keyColumns.GetType();
        }
        for (auto& column : keyColumns.AsList()) {
            if (!column.IsString()) {
                ythrow yexception() << "Ill-formed format: key_column_names: " << column.GetType();
            }
            attributes.KeyColumnNames.push_back(column.AsString());
        }
    }

    if (nodeAttributes.HasKey("subkey_column_names")) {
        const auto& subkeyColumns = nodeAttributes["subkey_column_names"];
        if (!subkeyColumns.IsList()) {
            ythrow yexception() << "Ill-formed format: subkey_column_names is not a list: " << subkeyColumns.GetType();
        }
        for (const auto& column : subkeyColumns.AsList()) {
            if (!column.IsString()) {
                ythrow yexception() << "Ill-formed format: non-string inside subkey_key_column_names: " << column.GetType();
            }
            attributes.SubkeyColumnNames.push_back(column.AsString());
        }
    }

    return attributes;
}

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TString GetColumnName(const ::google::protobuf::FieldDescriptor& field) {
    const auto& options = field.options();
    const auto columnName = options.GetExtension(column_name);
    if (!columnName.empty()) {
        return columnName;
    }
    const auto keyColumnName = options.GetExtension(key_column_name);
    if (!keyColumnName.empty()) {
        return keyColumnName;
    }
    return field.name();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT

