#pragma once

#include <yt/server/lib/hydra/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/core/misc/small_vector.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TObjectId;
using NObjectClient::TTransactionId;
using NObjectClient::TVersionedObjectId;
using NObjectClient::EObjectType;
using NObjectClient::TCellTag;
using NObjectClient::TCellTagList;
using NObjectClient::NullObjectId;
using NObjectClient::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TObjectServiceCacheConfig)
DECLARE_REFCOUNTED_CLASS(TMasterCacheServiceConfig)
DECLARE_REFCOUNTED_CLASS(TObjectServiceCacheEntry)
DECLARE_REFCOUNTED_CLASS(TObjectServiceCache)

using TEpoch = ui32;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
