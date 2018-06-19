#include "format.h"

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

constexpr bool USE_NEW_PROTO_FORMAT_DESCRIPTION = true;

////////////////////////////////////////////////////////////////////////////////

int SaveDependencies(
    FileDescriptorSet& set,
    THashMap<const FileDescriptor*, int>& saved,
    const FileDescriptor* fileDescriptor)
{
    auto* check = saved.FindPtr(fileDescriptor);
    if (check) {
        return *check;
    }

    for (int i = 0; i < fileDescriptor->dependency_count(); ++i) {
        SaveDependencies(set, saved, fileDescriptor->dependency(i));
    }

    auto *fileDescriptorProto = set.add_file();
    fileDescriptor->CopyTo(fileDescriptorProto);

    int fileIndex = set.file_size() - 1;
    saved[fileDescriptor] = fileIndex;
    return fileIndex;
}

TNode MakeEnumerationConfig(const ::google::protobuf::EnumDescriptor* enumDescriptor)
{
    auto config = TNode::CreateMap();
    for (int i = 0; i < enumDescriptor->value_count(); ++i) {
        config[enumDescriptor->value(i)->name()] = enumDescriptor->value(i)->number();
    }
    return config;
}

TNode MakeProtoFormatConfigOld(const TVector<const Descriptor*>& descriptors)
{
    FileDescriptorSet set;
    THashMap<const FileDescriptor*, int> saved;
    TVector<int> fileIndices;
    TVector<int> messageIndices;

    for (auto* descriptor : descriptors) {
        auto* fileDescriptor = descriptor->file();
        int fileIndex = SaveDependencies(set, saved, fileDescriptor);
        fileIndices.push_back(fileIndex);
        messageIndices.push_back(descriptor->index());
    }

    TString fileDescriptorSetBytes;
    set.SerializeToString(&fileDescriptorSetBytes);

    TNode config("protobuf");
    config.Attributes()
        ("file_descriptor_set", fileDescriptorSetBytes)
        ("file_indices", TNode::CreateList()) //  TNode::TListType(fileIndices.cbegin(), fileIndices.cend())))
        ("message_indices", TNode::CreateList()) //TListType(messageIndices.cbegin(), messageIndices.cend()))
        ("enums_as_strings", true)
        ("nested_messages_mode", "protobuf");
    config.Attributes()["file_indices"].AsList().assign(fileIndices.cbegin(), fileIndices.cend());
    config.Attributes()["message_indices"].AsList().assign(messageIndices.cbegin(), messageIndices.cend());
    return config;
}

TNode MakeProtoFormatConfigNew(const TVector<const Descriptor*>& descriptors)
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

TNode MakeProtoFormatConfig(const TVector<const Descriptor*>& descriptors)
{
    if (USE_NEW_PROTO_FORMAT_DESCRIPTION) {
        return MakeProtoFormatConfigNew(descriptors);
    } else {
        return MakeProtoFormatConfigOld(descriptors);
    }
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

