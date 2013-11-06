#include "stdafx.h"
#include "node_statistics.h"

#include <ytlib/node_tracker_client/node.pb.h>

namespace NYT {
namespace NNodeTrackerClient {
namespace NProto {

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TNodeStatistics& statistics)
{
    return Sprintf(
        "AvailableSpace: %" PRId64 ", UsedSpace: %" PRId64 ", Chunks: %d, UserSessions: %d, "
        "ReplicationSessions: %d, RepairSessions: %d",
        statistics.total_available_space(),
        statistics.total_used_space(),
        statistics.total_chunk_count(),
        statistics.total_user_session_count(),
        statistics.total_replication_session_count(),
        statistics.total_repair_session_count());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto
} // namespace NNodeTrackerClient
} // namespace NYT
