#pragma once

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

using TSchedulerPoolId = NObjectClient::TObjectId;

DECLARE_ENTITY_TYPE(TSchedulerPool, TSchedulerPoolId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TSchedulerPoolTree, TSchedulerPoolId, NObjectClient::TObjectIdEntropyHash)

DECLARE_MASTER_OBJECT_TYPE(TSchedulerPool)
DECLARE_MASTER_OBJECT_TYPE(TSchedulerPoolTree)

DECLARE_REFCOUNTED_STRUCT(ISchedulerPoolManager)

DECLARE_REFCOUNTED_STRUCT(TDynamicSchedulerPoolManagerConfig)

DECLARE_REFCOUNTED_CLASS(TPoolResources)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
