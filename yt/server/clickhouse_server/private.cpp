#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger ServerLogger("Server");
const NLogging::TLogger EngineLogger("Engine");
const NProfiling::TProfiler ServerProfiler("/server");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
