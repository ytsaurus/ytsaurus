#include "stdafx.h"
#include "node_statistics.h"

namespace NYT {
namespace NNodeTrackerClient {
namespace NProto {

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TNodeStatistics& statistics)
{
    return Sprintf(
        "Space: %" PRId64 "/%" PRId64 ", Chunks: %d, UserSessions: %d, "
        "ReplicationSessions: %d, RepairSessions: %d, TabletSlots: %d/%d",
        statistics.total_used_space(),
        statistics.total_available_space() + statistics.total_used_space(),
        statistics.total_chunk_count(),
        statistics.total_user_session_count(),
        statistics.total_replication_session_count(),
        statistics.total_repair_session_count(),
        statistics.used_tablet_slots(),
        statistics.available_tablet_slots() + statistics.used_tablet_slots());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto
} // namespace NNodeTrackerClient
} // namespace NYT
