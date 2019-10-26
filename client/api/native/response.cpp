#include "response.h"

#include "payload.h"

#include <yt/core/misc/error.h>
#include <yt/core/misc/variant.h>

namespace NYP::NClient::NApi::NNative {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <>
void ParseYsonPayload<NYson::TYsonString>(
    TYsonPayload yson,
    NYson::TYsonString* attribute)
{
    *attribute = std::move(yson.Yson);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TAttributeList* attributeList,
    const NProto::TAttributeList& protoAttributeList)
{
    attributeList->ValuePayloads.reserve(protoAttributeList.value_payloads_size());
    for (const auto& protoPayload : protoAttributeList.value_payloads()) {
        auto& payload = attributeList->ValuePayloads.emplace_back();
        FromProto(&payload, protoPayload);
    }

    attributeList->Timestamps.reserve(protoAttributeList.timestamps_size());
    for (auto protoTimestamp : protoAttributeList.timestamps()) {
        attributeList->Timestamps.push_back(protoTimestamp);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidatePayloadFormat(EPayloadFormat expectedFormat, const TPayload& payload)
{
    // None payload format expects raw bytes, but not payload type.
    if (expectedFormat == EPayloadFormat::None) {
        THROW_ERROR_EXCEPTION("Unsupported %Qlv payload format",
            expectedFormat);
    }

    EPayloadFormat actualFormat = EPayloadFormat::None;
    Visit(payload,
        [&] (const TNullPayload& /*null*/) {
            actualFormat = EPayloadFormat::None;
        },
        [&] (const TYsonPayload& /*yson*/) {
            actualFormat = EPayloadFormat::Yson;
        },
        [&] (const TProtobufPayload& /*protobuf*/) {
            actualFormat = EPayloadFormat::Protobuf;
        });
    if (expectedFormat != actualFormat) {
        THROW_ERROR_EXCEPTION("Expected %Qlv payload format, but got %Qlv",
            expectedFormat,
            actualFormat);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
