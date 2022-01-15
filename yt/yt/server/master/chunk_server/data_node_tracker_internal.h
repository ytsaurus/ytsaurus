#pragma once

#include "private.h"

#include <yt/yt/server/lib/hydra_common/entity_map.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct IDataNodeTrackerInternal
    : public virtual TRefCounted
{
    virtual NHydra::TEntityMap<TChunkLocation>* MutableChunkLocations() = 0;
    virtual TChunkLocation* CreateChunkLocation(
        TChunkLocationUuid locationUuid,
        NObjectClient::TObjectId hintId) = 0;
    virtual void DestroyChunkLocation(TChunkLocation* location) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDataNodeTrackerInternal)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
