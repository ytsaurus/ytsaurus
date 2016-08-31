#include "proto_helpers.h"

#include <mapreduce/yt/common/fluent.h>

#include <contrib/libs/protobuf/descriptor.h>
#include <contrib/libs/protobuf/google/protobuf/descriptor.pb.h>

#include <util/stream/str.h>

namespace NYT {

using ::google::protobuf::Message;
using ::google::protobuf::Descriptor;
using ::google::protobuf::FileDescriptor;
using ::google::protobuf::FileDescriptorSet;
using ::google::protobuf::DescriptorPool;

////////////////////////////////////////////////////////////////////////////////

namespace {

int SaveDependencies(
    FileDescriptorSet& set,
    yhash_map<const FileDescriptor*, int>& saved,
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

} // namespace

TNode MakeProtoFormatConfig(const yvector<const Descriptor*>& descriptors)
{
    FileDescriptorSet set;
    yhash_map<const FileDescriptor*, int> saved;
    yvector<int> fileIndices;
    yvector<int> messageIndices;

    for (auto* descriptor : descriptors) {
        auto* fileDescriptor = descriptor->file();
        int fileIndex = SaveDependencies(set, saved, fileDescriptor);
        fileIndices.push_back(fileIndex);
        messageIndices.push_back(descriptor->index());
    }

    TStringStream stream;
    set.SerializeToStream(&stream);

    return BuildYsonNodeFluently()
    .BeginAttributes()
        .Item("file_descriptor_set").Value(stream.Str())
        .Item("file_indices").List(fileIndices)
        .Item("message_indices").List(messageIndices)
        .Item("enums_as_strings").Value(true)
        .Item("embedded_messages").Value(true)
    .EndAttributes()
    .Value("protobuf");
}

TNode MakeProtoFormatConfig(const Message* prototype)
{
    yvector<const Descriptor*> descriptors(1, prototype->GetDescriptor());
    return MakeProtoFormatConfig(descriptors);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
