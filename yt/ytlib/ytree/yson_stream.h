#pragma once

#include "public.h"
#include "yson_string.h"
#include "yson_parser.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonInput
{
public:
    explicit TYsonInput(TInputStream* stream, EYsonType type = EYsonType::Node);

    DEFINE_BYVAL_RO_PROPERTY(TInputStream*, Stream);
    DEFINE_BYVAL_RO_PROPERTY(EYsonType, Type);
};

////////////////////////////////////////////////////////////////////////////////

class TYsonOutput
{
public:
    explicit TYsonOutput(TOutputStream* stream, EYsonType type = EYsonType::Node);

    DEFINE_BYVAL_RO_PROPERTY(TOutputStream*, Stream);
    DEFINE_BYVAL_RO_PROPERTY(EYsonType, Type);
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonInput& input, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
