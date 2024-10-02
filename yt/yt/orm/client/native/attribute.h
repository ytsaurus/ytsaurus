#pragma once

#include "payload.h"

#include <yt/yt/core/ytree/serialize.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUnknownFieldMode,
    (Skip)
    (Fail)
);

struct TParseAttributeOptions
{
    EUnknownFieldMode UnknownFieldMode = EUnknownFieldMode::Fail;
};

void ParseProtobufAttribute(
    google::protobuf::Message* message,
    const NYson::TProtobufMessageType* messageType,
    const TYsonPayload& payload,
    const TParseAttributeOptions& options);

template <class TAttribute>
void ParseAttribute(
    TAttribute& attribute,
    NYTree::INodePtr node,
    const TParseAttributeOptions& options);

// std::vector
template <class TAttribute>
void ParseAttribute(
    std::vector<TAttribute>& attribute,
    NYTree::INodePtr node,
    const TParseAttributeOptions& options);

// std::optional
template <class TAttribute>
void ParseAttribute(
    std::optional<TAttribute>& attribute,
    NYTree::INodePtr node,
    const TParseAttributeOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative

#define ATTRIBUTE_INL_H_
#include "attribute-inl.h"
#undef ATTRIBUTE_INL_H_
