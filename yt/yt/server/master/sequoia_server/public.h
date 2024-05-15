#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISequoiaContext)
DECLARE_REFCOUNTED_STRUCT(ISequoiaManager)
DECLARE_REFCOUNTED_STRUCT(ISequoiaQueueManager)

DECLARE_REFCOUNTED_CLASS(TDynamicSequoiaQueueConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicSequoiaManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
