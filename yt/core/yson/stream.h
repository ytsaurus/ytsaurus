#pragma once

#include "public.h"
#include "string.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

class TYsonInput
{
public:
    explicit TYsonInput(
        TInputStream* stream,
        EYsonType type = EYsonType::Node);

    DEFINE_BYVAL_RO_PROPERTY(TInputStream*, Stream);
    DEFINE_BYVAL_RO_PROPERTY(EYsonType, Type);

};

////////////////////////////////////////////////////////////////////////////////

class TYsonOutput
{
public:
    explicit TYsonOutput(
        TOutputStream* stream,
        EYsonType type = EYsonType::Node);

    DEFINE_BYVAL_RO_PROPERTY(TOutputStream*, Stream);
    DEFINE_BYVAL_RO_PROPERTY(EYsonType, Type);

};

////////////////////////////////////////////////////////////////////////////////

// To hook-up with Serialize/Deserialize framework.
// For direct calss, use ParseYson instead.
void Serialize(
    const TYsonInput& input,
    IYsonConsumer* consumer);

void ParseYson(
    const TYsonInput& input,
    IYsonConsumer* consumer,
    bool enableLinePositionInfo = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
