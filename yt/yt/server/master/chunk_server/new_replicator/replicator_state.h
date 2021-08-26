#pragma once

#include "public.h"

#include "replicator_state_proxy.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

struct IReplicatorState
    : public TRefCounted
{
    virtual void Load() = 0;

    // After this call all dual mutations that were enqueued prior to this call are applied.
    virtual void SyncWithUpstream() = 0;

    // Dual mutations.
    virtual void UpdateDynamicConfig(const NCellMaster::TDynamicClusterConfigPtr& newConfig) = 0;

    virtual void CreateMedium(NChunkServer::TMedium* medium) = 0;
    virtual void RenameMedium(TMediumId mediumId, const TString& newName) = 0;
    virtual void UpdateMediumConfig(TMediumId mediumId, const TMediumConfigPtr& newConfig) = 0;

    // Dual state.
    virtual const NCellMaster::TDynamicClusterConfigPtr& GetDynamicConfig() const = 0;

    //! Returns the map with all media.
    virtual const THashMap<TMediumId, std::unique_ptr<TMedium>>& Media() const = 0;

    //! Returns the medium with a given id (|nullptr| if none).
    virtual TMedium* FindMedium(TMediumId mediumId) const = 0;

    //! Returns the medium with a given id (fails if none).
    virtual TMedium* GetMedium(TMediumId mediumId) const = 0;

    //! Returns the medium with a given index (|nullptr| if none).
    virtual TMedium* FindMediumByIndex(TMediumIndex index) const = 0;

    //! Returns the medium with a given name (|nullptr| if none).
    virtual TMedium* FindMediumByName(const TString& name) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicatorState)

////////////////////////////////////////////////////////////////////////////////

IReplicatorStatePtr CreateReplicatorState(std::unique_ptr<IReplicatorStateProxy> proxy);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
