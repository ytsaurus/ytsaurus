#pragma once

#include <yt/client/object_client/public.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TReqExecuteBatchWithRetriesConfig)

DECLARE_REFCOUNTED_STRUCT(TObjectAttributeCacheConfig)
DECLARE_REFCOUNTED_CLASS(TObjectAttributeCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
