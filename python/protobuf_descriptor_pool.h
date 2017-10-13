#pragma once

#include <contrib/libs/protobuf/descriptor.h>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

::google::protobuf::DescriptorPool* GetDescriptorPool()
{
    static ::google::protobuf::DescriptorPool descriptorPool;
    return &descriptorPool;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
