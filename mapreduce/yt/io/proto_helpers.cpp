#include "proto_helpers.h"

#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/interface/protos/extension.pb.h>
#include <mapreduce/yt/common/fluent.h>

#include <contrib/libs/protobuf/descriptor.h>
#include <contrib/libs/protobuf/google/protobuf/descriptor.pb.h>
#include <contrib/libs/protobuf/messagext.h>
#include <contrib/libs/protobuf/io/coded_stream.h>

#include <util/stream/str.h>
#include <util/stream/file.h>
#include <util/folder/path.h>

namespace NYT {

using ::google::protobuf::Message;
using ::google::protobuf::Descriptor;
using ::google::protobuf::FileDescriptor;
using ::google::protobuf::FileDescriptorSet;
using ::google::protobuf::DescriptorPool;

using ::google::protobuf::io::CodedInputStream;
using ::google::protobuf::io::TCopyingInputStreamAdaptor;

////////////////////////////////////////////////////////////////////////////////

namespace {

int SaveDependencies(
    FileDescriptorSet& set,
    yhash<const FileDescriptor*, int>& saved,
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

yvector<const Descriptor*> GetJobDescriptors(const TString& fileName)
{
    yvector<const Descriptor*> descriptors;
    if (!TFsPath(fileName).Exists()) {
        ythrow TIOException() <<
            "Cannot load '" << fileName << "' file";
    }

    TIFStream input(fileName);
    TString line;
    while (input.ReadLine(line)) {
        const auto* pool = DescriptorPool::generated_pool();
        const auto* descriptor = pool->FindMessageTypeByName(line);
        descriptors.push_back(descriptor);
    }

    return descriptors;
}

TNode MakeEnumerationConfig(const ::google::protobuf::EnumDescriptor* enumDescriptor)
{
    auto config = TNode::CreateMap();
    for (int i = 0; i < enumDescriptor->value_count(); ++i) {
        config[enumDescriptor->value(i)->name()] = enumDescriptor->value(i)->number();
    }
    return config;
}

TNode MakeProtoFormatConfigOld(const yvector<const Descriptor*>& descriptors)
{
    FileDescriptorSet set;
    yhash<const FileDescriptor*, int> saved;
    yvector<int> fileIndices;
    yvector<int> messageIndices;

    for (auto* descriptor : descriptors) {
        auto* fileDescriptor = descriptor->file();
        int fileIndex = SaveDependencies(set, saved, fileDescriptor);
        fileIndices.push_back(fileIndex);
        messageIndices.push_back(descriptor->index());
    }

    TString fileDescriptorSetBytes;
    set.SerializeToString(&fileDescriptorSetBytes);

    return BuildYsonNodeFluently()
        .BeginAttributes()
        .Item("file_descriptor_set").Value(fileDescriptorSetBytes)
        .Item("file_indices").List(fileIndices)
        .Item("message_indices").List(messageIndices)
        .Item("enums_as_strings").Value(true)
        .Item("nested_messages_mode").Value("protobuf")
        .EndAttributes()
        .Value("protobuf");
}

TNode MakeProtoFormatConfigNew(const yvector<const Descriptor*>& descriptors)
{
    auto enumerations = TNode::CreateMap();
    yvector<TNode> tables;

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

    return BuildYsonNodeFluently()
        .BeginAttributes()
        .Item("enumerations").Value(enumerations)
        .Item("tables").List(tables)
        .EndAttributes()
        .Value("protobuf");
}

constexpr bool USE_NEW_PROTO_FORMAT_DESCRIPTION = false;

} // namespace

////////////////////////////////////////////////////////////////////////////////

TNode MakeProtoFormatConfig(const yvector<const Descriptor*>& descriptors)
{
    if (USE_NEW_PROTO_FORMAT_DESCRIPTION) {
        return MakeProtoFormatConfigNew(descriptors);
    } else {
        return MakeProtoFormatConfigOld(descriptors);
    }
}

yvector<const Descriptor*> GetJobInputDescriptors()
{
    return GetJobDescriptors("proto_input");
}

yvector<const Descriptor*> GetJobOutputDescriptors()
{
    return GetJobDescriptors("proto_output");
}

TNode MakeProtoFormatConfig(const Message* prototype)
{
    yvector<const Descriptor*> descriptors(1, prototype->GetDescriptor());
    return MakeProtoFormatConfig(descriptors);
}

void ValidateProtoDescriptor(
    const Message& row,
    size_t tableIndex,
    const yvector<const Descriptor*>& descriptors,
    bool isRead)
{
    const char* direction = isRead ? "input" : "output";

    if (tableIndex >= descriptors.size()) {
        ythrow TIOException() <<
            "Table index " << tableIndex <<
            " is out of range [0, " << descriptors.size() <<
            ") in " << direction;
    }

    if (row.GetDescriptor() != descriptors[tableIndex]) {
        ythrow TIOException() <<
            "Invalid row of type " << row.GetDescriptor()->full_name() <<
            " at index " << tableIndex <<
            ", row of type " << descriptors[tableIndex]->full_name() <<
            " expected in " << direction;
    }
}

void ParseFromStream(IInputStream* stream, Message& row, ui32 length)
{
    TLengthLimitedInput input(stream, length);
    TCopyingInputStreamAdaptor adaptor(&input);
    CodedInputStream codedStream(&adaptor);
    codedStream.SetTotalBytesLimit(length + 1, length + 1);
    bool parsedOk = row.ParseFromCodedStream(&codedStream);
    Y_ENSURE(parsedOk, "Failed to parse protobuf message");

    Y_ENSURE(input.Left() == 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
