#include "stdafx.h"
#include "yson_stream.h"

#include <core/formats/parser.h>
#include <core/formats/yson_parser.h>

namespace NYT {
namespace NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TYsonInput::TYsonInput(TInputStream* stream, EYsonType type)
    : Stream_(stream)
    , Type_(type)
{ }

////////////////////////////////////////////////////////////////////////////////

TYsonOutput::TYsonOutput(TOutputStream* stream, EYsonType type)
    : Stream_(stream)
    , Type_(type)
{ }

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonInput& input, IYsonConsumer* consumer)
{
    ParseYson(input, consumer);
}

void ParseYson(
    const TYsonInput& input,
    IYsonConsumer* consumer,
    bool enableLinePositionInfo)
{
    auto parser = NFormats::CreateParserForYson(
        consumer,
        input.GetType(),
        enableLinePositionInfo);
    NFormats::Parse(input.GetStream(), ~parser);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
