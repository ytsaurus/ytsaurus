#include "stdafx.h"
#include "yson_stream.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TYsonInput::TYsonInput(TInputStream* stream, EYsonType type)
    : Stream_(stream)
    , Type_(type)
{ }

TYsonOutput::TYsonOutput(TOutputStream* stream, EYsonType type)
    : Stream_(stream)
    , Type_(type)
{ }

void Serialize(const TYsonInput& input, IYsonConsumer* consumer)
{
    ParseYson(input, consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
