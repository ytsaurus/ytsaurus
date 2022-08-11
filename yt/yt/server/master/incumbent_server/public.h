#pragma once

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NIncumbentServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TIncumbentBase)
DECLARE_REFCOUNTED_CLASS(TIncumbentSchedulerConfig)
DECLARE_REFCOUNTED_CLASS(TIncumbentSchedulingConfig)
DECLARE_REFCOUNTED_CLASS(TIncumbentManagerConfig)

DECLARE_REFCOUNTED_STRUCT(IIncumbent)
DECLARE_REFCOUNTED_STRUCT(IIncumbentManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
