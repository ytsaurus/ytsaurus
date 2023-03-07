#include "helpers.h"
#include "hive_manager.h"

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

TSerializedMessagePtr SerializeOutcomingMessage(const ::google::protobuf::MessageLite& protoMessage)
{
    auto serializedMessage = New<TSerializedMessage>();
    serializedMessage->Type = protoMessage.GetTypeName();
    serializedMessage->Data = SerializeProtoToStringWithEnvelope(protoMessage);
    return serializedMessage;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
