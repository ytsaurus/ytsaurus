#pragma once

#include "config.h"

#include <contrib/libs/protobuf/descriptor.h>
#include <contrib/libs/protobuf/google/protobuf/descriptor.pb.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TProtobufTypeSet
{
public:
    explicit TProtobufTypeSet(TProtobufFormatConfigPtr config);

    const ::google::protobuf::Descriptor* GetMessageDescriptor(int tableIndex) const;

private:
    ::google::protobuf::DescriptorPool DescriptorPool_;
    ::google::protobuf::FileDescriptorSet FileDescriptorSet_;

    std::vector<const ::google::protobuf::Descriptor*> MessageDescriptors_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

