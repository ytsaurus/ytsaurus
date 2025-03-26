#pragma once

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/sequoia/public.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISequoiaContext)
DECLARE_REFCOUNTED_STRUCT(ISequoiaManager)
DECLARE_REFCOUNTED_STRUCT(IGroundUpdateQueueManager)

DECLARE_REFCOUNTED_STRUCT(TDynamicTableUpdateQueueConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicGroundUpdateQueueManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicCypressProxyTrackerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicSequoiaManagerConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ICypressProxyTracker)

DECLARE_ENTITY_TYPE(TCypressProxyObject, NObjectServer::TObjectId, NObjectClient::TObjectIdEntropyHash)

DECLARE_MASTER_OBJECT_TYPE(TCypressProxyObject)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGroundUpdateAction,
    ((Write)              (0))
    ((Delete)             (1))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
