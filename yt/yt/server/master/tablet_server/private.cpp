#include "private.h"

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger TabletServerLogger("TabletServer");
const NProfiling::TProfiler TabletServerProfiler("/tablet_server");
const NProfiling::TRegistry TabletServerProfilerRegistry("/tablet_server");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
