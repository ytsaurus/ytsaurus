#include "helpers.h"

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

void SerializeMessage(
    const ::google::protobuf::MessageLite& protoMessage,
    NHiveClient::NProto::TEncapsulatedMessage* encapsulatedMessage)
{
    encapsulatedMessage->set_type(protoMessage.GetTypeName());
    encapsulatedMessage->set_data(SerializeProtoToStringWithEnvelope(protoMessage));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
