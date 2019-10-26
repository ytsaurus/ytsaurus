#pragma once
#ifndef RESPONSE_INL_H_
#error "Direct inclusion of this file is not allowed, include response.h"
// For the sake of sane code completion.
#include "response.h"
#endif

#include <yt/core/misc/variant.h>
#include <yt/core/ytree/convert.h>

namespace NYP::NClient::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TAttribute>
void ParseYsonPayload(
    TYsonPayload yson,
    TAttribute* attribute)
{
    using namespace NYT::NYTree;
    using namespace NYT::NYson;
    auto node = ConvertTo<INodePtr>(std::move(yson.Yson));
    if (node->GetType() == ENodeType::Entity) {
        if (!node->Attributes().ListKeys().empty()) {
            THROW_ERROR_EXCEPTION("Error parsing yson payload: entity with attributes is not supported yet");
        }
        *attribute = TAttribute();
    } else {
        *attribute = ConvertTo<TAttribute>(std::move(node));
    }
}

template <>
void ParseYsonPayload(
    TYsonPayload yson,
    NYT::NYson::TYsonString* attribute);

////////////////////////////////////////////////////////////////////////////////

template <class TAttribute>
void ParseProtobufPayload(
    TProtobufPayload /*protobuf*/,
    TAttribute* /*attribute*/)
{
    THROW_ERROR_EXCEPTION("Protobuf payload parsing is not supported yet");
}

////////////////////////////////////////////////////////////////////////////////

template <class TAttribute>
void ParsePayload(TPayload payload, TAttribute* attribute)
{
    NYT::Visit(payload,
        [&] (TNullPayload& /*null*/) {
            THROW_ERROR_EXCEPTION("Unexpected null payload");
        },
        [&] (TYsonPayload& yson) {
            ParseYsonPayload(std::move(yson), attribute);
        },
        [&] (TProtobufPayload& protobuf) {
            ParseProtobufPayload(std::move(protobuf), attribute);
        });
}

////////////////////////////////////////////////////////////////////////////////

template <size_t Index, class... TAttributes>
struct TParsePayloadsHelper;

template <size_t Index>
struct TParsePayloadsHelper<Index>
{
    static void Run(std::vector<TPayload>& /*payloads*/)
    { }
};

template <size_t Index, class TAttribute, class... TAttributes>
struct TParsePayloadsHelper<Index, TAttribute, TAttributes...>
{
    static void Run(
        std::vector<TPayload>& payloads,
        TAttribute* attribute,
        TAttributes*... attributes)
    {
        ParsePayload(std::move(payloads[Index]), attribute);
        TParsePayloadsHelper<Index + 1, TAttributes...>::Run(payloads, attributes...);
    }
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class... TAttributes>
void ParsePayloads(
    std::vector<TPayload> payloads,
    TAttributes*... attributes)
{
    if (sizeof...(attributes) != payloads.size()) {
        THROW_ERROR_EXCEPTION("Incorrect number of payloads: expected %v, but got %v",
            sizeof...(attributes),
            payloads.size());
    }
    NDetail::TParsePayloadsHelper<0, TAttributes...>::Run(
        payloads,
        attributes...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative {
