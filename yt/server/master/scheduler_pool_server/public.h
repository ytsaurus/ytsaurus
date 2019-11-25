#pragma once

#include <yt/client/object_client/public.h>

#include <yt/server/lib/hydra/public.h>

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

using TSchedulerPoolId = NObjectClient::TObjectId;

DECLARE_ENTITY_TYPE(TSchedulerPool, TSchedulerPoolId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TSchedulerPoolTree, TSchedulerPoolId, NObjectClient::TDirectObjectIdHash)

DECLARE_REFCOUNTED_CLASS(TSchedulerPoolManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
