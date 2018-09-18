#include "config.h"

#include "protobuf.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

void TProtobufFormatConfig::Validate()
{
    // Just try to construct.
    New<TProtobufFormatDescription>()->Init(this);
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NFormats
} // namespace NYT
