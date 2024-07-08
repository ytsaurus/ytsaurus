#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISequoiaContext)
DECLARE_REFCOUNTED_STRUCT(ISequoiaManager)
DECLARE_REFCOUNTED_STRUCT(IGroundUpdateQueueManager)

DECLARE_REFCOUNTED_CLASS(TDynamicTableUpdateQueueConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicGroundUpdateQueueManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicSequoiaManagerConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGroundUpdateAction,
    ((Write)              (0))
    ((Delete)             (1))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
