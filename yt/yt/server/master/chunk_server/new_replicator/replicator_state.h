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

    // Dual state.
    virtual const NCellMaster::TDynamicClusterConfigPtr& GetDynamicConfig() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicatorState)

////////////////////////////////////////////////////////////////////////////////

IReplicatorStatePtr CreateReplicatorState(std::unique_ptr<IReplicatorStateProxy> proxy);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
