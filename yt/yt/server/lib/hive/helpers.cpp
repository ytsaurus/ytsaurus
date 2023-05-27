#include "helpers.h"
#include "hive_manager.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NHiveServer {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TSerializedMessagePtr SerializeOutcomingMessage(const ::google::protobuf::MessageLite& protoMessage)
{
    auto serializedMessage = New<TSerializedMessage>();
    serializedMessage->Type = protoMessage.GetTypeName();
    serializedMessage->Data = SerializeProtoToStringWithEnvelope(protoMessage);
    return serializedMessage;
}

////////////////////////////////////////////////////////////////////////////////

bool IsAvenueEndpointType(NObjectClient::EObjectType type)
{
    return
        type == EObjectType::AliceAvenueEndpoint ||
        type == EObjectType::BobAvenueEndpoint;
}

TAvenueEndpointId GetSiblingAvenueEndpointId(TAvenueEndpointId endpointId)
{
    auto type = TypeFromId(endpointId);
    YT_VERIFY(IsAvenueEndpointType(type));

    type = type == EObjectType::AliceAvenueEndpoint
        ? EObjectType::BobAvenueEndpoint
        : EObjectType::AliceAvenueEndpoint;

    return ReplaceTypeInId(endpointId, type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
