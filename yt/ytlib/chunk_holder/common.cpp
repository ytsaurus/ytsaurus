#include "stdafx.h"
#include "common.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkHolderLogger("ChunkHolder");

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const NChunkServer::NProto::THolderStatistics& statistics)
{
    return Sprintf("AvailableSpace: %" PRId64 ", UsedSpace: %" PRId64 ", ChunkCount: %d, SessionCount: %d",
       statistics.available_space(),
       statistics.used_space(),
       statistics.chunk_count(),
       statistics.session_count());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
