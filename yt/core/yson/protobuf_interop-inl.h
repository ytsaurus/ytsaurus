#pragma once
#ifndef PROTOBUF_INTEROP_INL_H_
#error "Direct inclusion of this file is not allowed, include protobuf_interop.h"
// For the sake of sane code completion
#include "protobuf_interop.h"
#endif

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

template <class T>
const TProtobufMessageType* ReflectProtobufMessageType()
{
    static const TProtobufMessageType* type;
    if (Y_UNLIKELY(!type)) {
        type = ReflectProtobufMessageType(T::default_instance().GetDescriptor());
    }
    return type;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
