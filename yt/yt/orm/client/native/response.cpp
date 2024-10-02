#include "response.h"

#include "payload.h"

#include <yt/yt/orm/client/objects/key.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NOrm::NClient::NNative {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <>
void ParseYsonPayload<NYson::TYsonString>(
    TYsonPayload yson,
    NYson::TYsonString* attribute,
    const TParseAttributeOptions& /*options*/)
{
    *attribute = std::move(yson.Yson);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <>
void ParsePayloads<NObjects::TObjectKey>(
    std::vector<TPayload> payloads,
    const TParsePayloadsOptions& options,
    NObjects::TObjectKey* attribute)
{
    NObjects::TObjectKey::TKeyFields fields;
    fields.reserve(payloads.size());
    for (auto& payload : payloads) {
        fields.emplace_back();
        NDetail::ParsePayload(std::move(payload), &fields.back(), options);
    }

    *attribute = NObjects::TObjectKey(std::move(fields));
}

////////////////////////////////////////////////////////////////////////////////

void ValidatePayloadFormat(EPayloadFormat expectedFormat, const TPayload& payload)
{
    // None payload format expects raw bytes, but not payload type.
    if (expectedFormat == EPayloadFormat::None) {
        THROW_ERROR_EXCEPTION("Unsupported %Qlv payload format",
            expectedFormat);
    }

    auto actualFormat = EPayloadFormat::None;
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

} // namespace NYT::NOrm::NClient::NNative
