#include "stubs.h"

#include <core/misc/guid.h>
#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

Stroka GetObjectIdFromDataSplit(const TDataSplit& split)
{
    auto guid = NYT::FromProto<TGuid>(split.chunk_id());
    return ToString(guid);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

