#pragma once

#include "private.h"

#include <yt/yt/server/lib/hydra_common/entity_map.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct IDataNodeTrackerInternal
    : public virtual TRefCounted
{
    virtual NHydra::TEntityMap<TRealChunkLocation>* MutableChunkLocations() = 0;
    virtual TRealChunkLocation* CreateChunkLocation(
        TChunkLocationUuid locationUuid,
        NObjectClient::TObjectId hintId) = 0;
    virtual void DestroyChunkLocation(TRealChunkLocation* location) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDataNodeTrackerInternal)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
