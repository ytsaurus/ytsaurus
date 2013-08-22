#pragma once

#include "public.h"
#include "yson_string.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonInput
{
public:
    explicit TYsonInput(
        TInputStream* stream,
        NYson::EYsonType type = NYson::EYsonType::Node);

    DEFINE_BYVAL_RO_PROPERTY(TInputStream*, Stream);
    DEFINE_BYVAL_RO_PROPERTY(NYson::EYsonType, Type);

};

////////////////////////////////////////////////////////////////////////////////

class TYsonOutput
{
public:
    explicit TYsonOutput(
        TOutputStream* stream,
        NYson::EYsonType type = NYson::EYsonType::Node);

    DEFINE_BYVAL_RO_PROPERTY(TOutputStream*, Stream);
    DEFINE_BYVAL_RO_PROPERTY(NYson::EYsonType, Type);

};

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const TYsonInput& input,
    NYson::IYsonConsumer* consumer);

void ParseYson(
    const TYsonInput& input,
    NYson::IYsonConsumer* consumer,
    bool enableLinePositionInfo = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
