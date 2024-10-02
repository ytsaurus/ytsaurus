#include "attribute.h"

#include <yt/yt/core/yson/protobuf_interop.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

void ParseProtobufAttribute(
    google::protobuf::Message* message,
    const NYson::TProtobufMessageType* messageType,
    const TYsonPayload& payload,
    const TParseAttributeOptions& options)
{
    NYson::EUnknownYsonFieldsMode unknownFieldMode;
    switch (options.UnknownFieldMode) {
        case EUnknownFieldMode::Skip:
            unknownFieldMode = NYson::EUnknownYsonFieldsMode::Skip;
            break;
        case EUnknownFieldMode::Fail:
            unknownFieldMode = NYson::EUnknownYsonFieldsMode::Fail;
            break;
    }
    auto protoPayload = YsonStringToProto(payload.Yson, messageType, unknownFieldMode);

    THROW_ERROR_EXCEPTION_IF(!message->ParseFromString(protoPayload),
        "Error parsing %Qv from payload",
        message->GetTypeName());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
