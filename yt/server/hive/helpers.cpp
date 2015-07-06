#include "stdafx.h"
#include "helpers.h"

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

NProto::TEncapsulatedMessage SerializeMessage(const ::google::protobuf::MessageLite& message)
{
    NProto::TEncapsulatedMessage encapsulatedMessage;
    encapsulatedMessage.set_type(message.GetTypeName());

    auto serializedMessage = SerializeToProtoWithEnvelope(message);
    encapsulatedMessage.set_data(ToString(serializedMessage));

    return encapsulatedMessage;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
