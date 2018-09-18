#include "helpers.h"

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/ytlib/hive/hive_service.pb.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

TRefCountedEncapsulatedMessagePtr SerializeMessage(
    const ::google::protobuf::MessageLite& protoMessage)
{
    auto encapsulatedMessage = New<TRefCountedEncapsulatedMessage>();
    encapsulatedMessage->set_type(protoMessage.GetTypeName());
    encapsulatedMessage->set_data(SerializeProtoToStringWithEnvelope(protoMessage));
    return encapsulatedMessage;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
