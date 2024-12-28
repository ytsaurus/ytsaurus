#pragma once

#include <yt/yt/core/misc/common.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

extern const TString DisabledLockFileName;
extern const TString HealthCheckFileName;
extern const NYPath::TYPath ClusterThrottlersConfigPath;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
