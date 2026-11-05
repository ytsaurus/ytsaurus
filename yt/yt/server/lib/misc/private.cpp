#include "private.h"

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

const std::string DisabledLockFileName("disabled");
const std::string HealthCheckFileName("health_check~");
const NYPath::TYPath ClusterThrottlersConfigPath("//sys/cluster_throttlers");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer

