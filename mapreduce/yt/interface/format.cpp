#include "format.h"

#include "errors.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <contrib/libs/protobuf/descriptor.h>
#include <contrib/libs/protobuf/google/protobuf/descriptor.pb.h>
#include <contrib/libs/protobuf/messagext.h>

namespace NYT {

using ::google::protobuf::Message;
using ::google::protobuf::Descriptor;
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

TNode MakeProtoFormatConfig(const TVector<const Descriptor*>& descriptors)
{
    auto enumerations = TNode::CreateMap();
    TVector<TNode> tables;

    for (auto* descriptor : descriptors) {
        auto columns = TNode::CreateList();
        for (int fieldIndex = 0; fieldIndex < descriptor->field_count(); ++fieldIndex) {
            auto* fieldDesc = descriptor->field(fieldIndex);
            auto columnConfig = TNode()("field_number", fieldDesc->number());
            TString columnName = fieldDesc->options().GetExtension(column_name);
            if (columnName.empty()) {
                const auto& keyColumnName = fieldDesc->options().GetExtension(key_column_name);
                columnName = keyColumnName.empty() ? fieldDesc->name() : keyColumnName;
            }
            columnConfig["name"] = columnName;
            if (fieldDesc->type() == ::google::protobuf::FieldDescriptor::TYPE_ENUM) {
                auto* enumeration = fieldDesc->enum_type();
                enumerations[enumeration->name()] = MakeEnumerationConfig(enumeration);
                columnConfig["proto_type"] = "enum_string";
                columnConfig["enumeration_name"] = enumeration->name();
            } else {
                columnConfig["proto_type"] = fieldDesc->type_name();
            }
            columns.Add(columnConfig);
        }
        tables.push_back(TNode()("columns", columns));
    }

    TNode config("protobuf");
    config.Attributes()
        ("enumerations", enumerations)
        ("tables", TNode::CreateList());
    config.Attributes()["tables"].AsList().assign(tables.cbegin(), tables.cend());
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

