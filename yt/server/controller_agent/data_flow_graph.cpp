#include "data_flow_graph.h"

#include <yt/server/chunk_pools/chunk_pool.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

void TEdgeDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, DestinationPool);
    Persist(context, RequiresRecoveryInfo);
    Persist(context, TableWriterOptions);
    Persist(context, TableUploadOptions);
    Persist(context, TableWriterConfig);
    Persist(context, Timestamp);
    Persist(context, CellTag);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT