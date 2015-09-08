#include "stdafx.h"
#include "node_statistics.h"

#include <core/misc/format.h>

#include <ytlib/node_tracker_client/node.pb.h>

namespace NYT {
namespace NNodeTrackerClient {
namespace NProto {

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TNodeStatistics& statistics)
{
    return Format(
        "Space: %v/%v, StoredChunks: %v, CachedChunks: %v, UserSessions: %v, "
        "ReplicationSessions: %v, RepairSessions: %v, TabletSlots: %v/%v",
        statistics.total_used_space(),
        statistics.total_available_space() + statistics.total_used_space(),
        statistics.total_stored_chunk_count(),
        statistics.total_cached_chunk_count(),
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
