#pragma once

#include <yt/client/object_client/public.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TReqExecuteBatchWithRetriesConfig)

DECLARE_REFCOUNTED_CLASS(TObjectAttributeCacheConfig)
DECLARE_REFCOUNTED_CLASS(TObjectAttributeCache)

DECLARE_REFCOUNTED_CLASS(TObjectServiceCacheConfig)
DECLARE_REFCOUNTED_CLASS(TObjectServiceCacheDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TCachingObjectServiceConfig)
DECLARE_REFCOUNTED_CLASS(TCachingObjectServiceDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TObjectServiceCacheEntry)
DECLARE_REFCOUNTED_CLASS(TObjectServiceCache)

DECLARE_REFCOUNTED_STRUCT(ICachingObjectService)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterFeature,
    ((OverlayedJournals)    (0))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
