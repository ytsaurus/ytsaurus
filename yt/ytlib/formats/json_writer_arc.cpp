#include "json_writer.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;

std::unique_ptr<TYsonConsumerBase> CreateJsonConsumer(
    TOutputStream* output,
    EYsonType type,
    TJsonFormatConfigPtr config)
{
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
