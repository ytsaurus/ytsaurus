#include "helpers.h"

#include <yt/client/object_client/helpers.h>

namespace NYT {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TGuid GenerateId(EObjectType type)
{
    static i64 counter = 0;
    return MakeId(type, 0, counter++, 0);
}

#define XX(object)\
TGuid Generate##object##Id() \
{ \
    return GenerateId(EObjectType::object); \
} \

XX(Chunk)
XX(ChunkList)
XX(TabletCell)
XX(TabletCellBundle)
XX(ClusterNode)

#undef XX

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
