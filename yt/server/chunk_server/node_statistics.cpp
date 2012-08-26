#include "stdafx.h"
#include "node_statistics.h"

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const NYT::NChunkServer::NProto::TNodeStatistics& statistics)
{
    return Sprintf("AvailableSpace: %" PRId64 ", UsedSpace: %" PRId64 ", ChunkCount: %d, SessionCount: %d",
        statistics.available_space(),
        statistics.used_space(),
        statistics.chunk_count(),
        statistics.session_count());
}

////////////////////////////////////////////////////////////////////////////////
