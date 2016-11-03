#pragma once

#include <mapreduce/yt/interface/node.h>

namespace google {
namespace protobuf {

class Message;
class Descriptor;

}
}

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNode MakeProtoFormatConfig(
    const yvector<const ::google::protobuf::Descriptor*>& descriptors);

TNode MakeProtoFormatConfig(const ::google::protobuf::Message* prototype);

yvector<const ::google::protobuf::Descriptor*> GetJobInputDescriptors();
yvector<const ::google::protobuf::Descriptor*> GetJobOutputDescriptors();

void ValidateProtoDescriptor(
    const ::google::protobuf::Message& row,
    size_t tableIndex,
    const yvector<const ::google::protobuf::Descriptor*>& descriptors,
    bool isRead);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
