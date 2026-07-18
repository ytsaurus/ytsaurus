#include "yson_message.h"

#include "message.h"
#include "timer.h"

#include <yt/yt/flow/library/cpp/serializer/serializer.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TYsonMessageMeta::Register(TRegistrar registrar)
{
    registrar.Parameter("message_id", &TThis::MessageId)
        .Default();
    registrar.Parameter("system_timestamp", &TThis::SystemTimestamp)
        .Default();
    registrar.Parameter("alignment_timestamp", &TThis::AlignmentTimestamp)
        .Default();
    registrar.Parameter("event_timestamp", &TThis::EventTimestamp)
        .Default();
    registrar.Parameter("stream_id", &TThis::StreamId)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TYsonMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("$meta", &TThis::Meta)
        .AddOption(NYsonSerializer::TSkipSerializationTag{})
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr GetYsonMessagePayloadSchema(const TYsonMessagePtr& ysonMessage)
{
    return NYsonSerializer::GetYsonSchema(ysonMessage);
}

TMessage ConvertToMessage(const TYsonMessagePtr& ysonMessage, const NTableClient::TTableSchemaPtr& schema)
{
    TMessage message;
    message.MessageId = ysonMessage->Meta->MessageId;
    message.SystemTimestamp = ysonMessage->Meta->SystemTimestamp;
    message.AlignmentTimestamp = ysonMessage->Meta->AlignmentTimestamp;
    message.EventTimestamp = ysonMessage->Meta->EventTimestamp;
    message.StreamId = ysonMessage->Meta->StreamId;
    message.Payload = TPayload(TPayload::TUnderlying(NYsonSerializer::Serialize(ysonMessage, schema)));
    message.PayloadSchema = schema;
    return message;
}

void ConvertToYsonMessage(const TMessage& message, const TYsonMessagePtr& ysonStruct)
{
    NYsonSerializer::Deserialize(ysonStruct, message.Payload.Underlying(), message.PayloadSchema);
    ysonStruct->Meta->MessageId = message.MessageId;
    ysonStruct->Meta->SystemTimestamp = message.SystemTimestamp;
    ysonStruct->Meta->AlignmentTimestamp = message.AlignmentTimestamp;
    ysonStruct->Meta->EventTimestamp = message.EventTimestamp;
    ysonStruct->Meta->StreamId = message.StreamId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
