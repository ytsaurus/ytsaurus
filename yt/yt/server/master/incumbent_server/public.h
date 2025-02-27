#pragma once

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/ytlib/incumbent_client/public.h>

namespace NYT::NIncumbentServer {

////////////////////////////////////////////////////////////////////////////////

using NIncumbentClient::EIncumbentType;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TIncumbentBase)
DECLARE_REFCOUNTED_STRUCT(TIncumbentSchedulerConfig)
DECLARE_REFCOUNTED_STRUCT(TIncumbentSchedulingConfig)
DECLARE_REFCOUNTED_STRUCT(TIncumbentManagerDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IIncumbent)
DECLARE_REFCOUNTED_STRUCT(IIncumbentManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
