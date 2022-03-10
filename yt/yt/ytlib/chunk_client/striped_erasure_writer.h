#include "public.h"

#include <yt/yt/library/erasure/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateStripedErasureWriter(
    TErasureWriterConfigPtr config,
    NErasure::ECodec codecId,
    TSessionId sessionId,
    const TWorkloadDescriptor& workloadDescriptor,
    std::vector<IChunkWriterPtr> writers);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
