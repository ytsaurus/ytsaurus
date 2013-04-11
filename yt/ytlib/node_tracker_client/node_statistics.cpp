#include "stdafx.h"
#include "node_statistics.h"

namespace NYT {
namespace NNodeTrackerClient {
namespace NProto {

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const NYT::NNodeTrackerClient::NProto::TNodeStatistics& statistics)
{
    return Sprintf("AvailableSpace: %" PRId64 ", UsedSpace: %" PRId64 ", ChunkCount: %d, SessionCount: %d",
        statistics.total_available_space(),
        statistics.total_used_space(),
        statistics.total_chunk_count(),
        statistics.total_session_count());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto
} // namespace NNodeTrackerClient
} // namespace NYT
