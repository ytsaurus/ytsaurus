#pragma once

#include <yt/yt/core/misc/common.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

extern const std::string DisabledLockFileName;
extern const std::string HealthCheckFileName;
extern const NYPath::TYPath ClusterThrottlersConfigPath;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
