#include "stdafx.h"
#include "node_statistics.h"

namespace NYT {
namespace NChunkServer {
namespace NProto {

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const NYT::NChunkServer::NProto::TNodeStatistics& statistics)
{
    return Sprintf("AvailableSpace: %" PRId64 ", UsedSpace: %" PRId64 ", ChunkCount: %d, SessionCount: %d",
        statistics.total_available_space(),
        statistics.total_used_space(),
        statistics.total_chunk_count(),
        statistics.total_session_count());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto
} // namespace NChunkServer
} // namespace NYT
