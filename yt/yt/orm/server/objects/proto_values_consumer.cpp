#include "proto_values_consumer.h"

namespace NYT::NOrm::NServer::NObjects {

using NYson::TProtobufWriterOptions;

////////////////////////////////////////////////////////////////////////////////

TProtoFormatConsumerState::TProtoFormatConsumerState(
    const NYson::TProtobufMessageType* rootType,
    NYson::EUnknownYsonFieldsMode unknownFieldsMode,
    TString* outputBuffer)
    : Stream_(outputBuffer)
    , Consumer_(CreateProtobufWriter(&Stream_, rootType, TProtobufWriterOptions{
        .UnknownYsonFieldModeResolver = TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
            unknownFieldsMode)}))
{ }

NYson::IYsonConsumer* TProtoFormatConsumerState::GetConsumer()
{
    return Consumer_.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
