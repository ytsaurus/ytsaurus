#include "private.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger TabletNodeLogger("TabletNode");
const NProfiling::TProfiler TabletNodeProfiler("/tablet_node");
const NProfiling::TRegistry TabletNodeProfilerRegistry{"yt/tablet_node"};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
