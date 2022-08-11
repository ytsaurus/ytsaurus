#include "public.h"

namespace NYT::NIncumbentClient {

////////////////////////////////////////////////////////////////////////////////

// BEWARE: Changing these values requires reign promotion since rolling update
// is not possible.
int GetIncumbentShardCount(EIncumbentType type)
{
    switch (type) {
        case EIncumbentType::CellJanitor:
            return 1;
        case EIncumbentType::ChunkReplicator:
            return 60;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentClient
