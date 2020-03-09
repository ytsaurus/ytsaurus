#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger ServerLogger("Server");
const NLogging::TLogger EngineLogger("Engine");
const NProfiling::TProfiler ServerProfiler("/server");

const TString CacheUserName("yt-clickhouse-cache");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

// This is an override of weak symbol from Common/Allocator.h.
// We do not want ClickHouse allocator to use raw mmaps as ytalloc already
// does that by himself.
__attribute__((__used__)) extern const size_t MMAP_THRESHOLD = static_cast<size_t>(1) << 60;
