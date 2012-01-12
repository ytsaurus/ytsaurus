#include "stdafx.h"
#include "common.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkServerLogger("ChunkServer");

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT


Stroka ToString(const NYT::NChunkServer::NProto::THolderStatistics& statistics)
{
    return Sprintf("AvailableSpace: %" PRId64 ", UsedSpace: %" PRId64 ", ChunkCount: %d, SessionCount: %d",
        statistics.available_space(),
        statistics.used_space(),
        statistics.chunk_count(),
        statistics.session_count());
}

////////////////////////////////////////////////////////////////////////////////
