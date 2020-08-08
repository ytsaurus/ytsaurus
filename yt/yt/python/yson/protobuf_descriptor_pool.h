#pragma once

#include <google/protobuf/descriptor.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

::google::protobuf::DescriptorPool* GetDescriptorPool()
{
    static ::google::protobuf::DescriptorPool descriptorPool;
    return &descriptorPool;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
