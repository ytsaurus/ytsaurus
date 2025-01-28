#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Thread affinity: any.
 */
struct IWorldInitializerCache
    : public TRefCounted
{
    virtual void UpdateWorldInitializationStatus(bool initialized) = 0;

    virtual TFuture<void> ValidateWorldInitialized() = 0;
};

DEFINE_REFCOUNTED_TYPE(IWorldInitializerCache)

////////////////////////////////////////////////////////////////////////////////

IWorldInitializerCachePtr CreateWorldInitializerCache(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
