#pragma once

#include <contrib/libs/protobuf/descriptor.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

::google::protobuf::DescriptorPool* GetDescriptorPool()
{
    static ::google::protobuf::DescriptorPool descriptorPool;
    return &descriptorPool;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
