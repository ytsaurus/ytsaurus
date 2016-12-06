#include "helpers.h"

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

NHiveClient::NProto::TEncapsulatedMessage SerializeMessage(const ::google::protobuf::MessageLite& message)
{
    NHiveClient::NProto::TEncapsulatedMessage encapsulatedMessage;
    encapsulatedMessage.set_type(message.GetTypeName());

    auto serializedMessage = SerializeToProtoWithEnvelope(message);
    encapsulatedMessage.set_data(ToString(serializedMessage));

    return encapsulatedMessage;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
