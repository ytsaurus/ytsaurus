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
    TYsonInput(TInputStream* stream, EYsonType type = EYsonType::Node):
        Stream_(stream), Type_(type)
    { }

    DEFINE_BYVAL_RO_PROPERTY(EYsonType, Type);
    DEFINE_BYVAL_RO_PROPERTY(TInputStream*, Stream);
};

////////////////////////////////////////////////////////////////////////////////

class TYsonOutput
{
public:
    TYsonOutput(TOutputStream* stream, EYsonType type = EYsonType::Node):
        Stream_(stream), Type_(type)
    { }

    DEFINE_BYVAL_RO_PROPERTY(EYsonType, Type);
    DEFINE_BYVAL_RO_PROPERTY(TOutputStream*, Stream);
};

////////////////////////////////////////////////////////////////////////////////

// TODO(roizner): move to .cpp
inline void Serialize(const TYsonInput& input, IYsonConsumer* consumer)
{
    ParseYson(input, consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
