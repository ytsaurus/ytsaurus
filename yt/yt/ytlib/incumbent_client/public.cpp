#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/cellar_client/public.h>

namespace NYT::NIncumbentClient {

////////////////////////////////////////////////////////////////////////////////

// BEWARE: Changing these values requires reign promotion since rolling update
// is not possible.
int GetIncumbentShardCount(EIncumbentType type)
{
    switch (type) {
        case EIncumbentType::CellJanitor:
            return NCellarClient::CellShardCount;
        case EIncumbentType::ChunkReplicator:
            return NChunkClient::ChunkShardCount;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentClient
