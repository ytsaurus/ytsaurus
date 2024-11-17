#pragma once

#include "public.h"

#include <yt/yt/ytlib/node_tracker_client/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

//! Allows to read persistent state from non-automaton threads. Used in exec
//! node tracker service and can be used in similar places.
struct IPersistentStateTransientCache
    : public TRefCounted
{
    // World initialization.

    // Thread affinity: automaton or local-read.
    virtual void UpdateWorldInitializationStatus(bool initialized) = 0;
    // Thread affinity: any.
    virtual void ValidateWorldInitialized() = 0;

    // Node tracking.

    // Thread affinity: automaton.
    virtual void ResetNodeDefaultAddresses() = 0;
    // Thread affinity: automaton.
    virtual void UpdateNodeDefaultAddress(
        NNodeTrackerClient::TNodeId nodeId,
        std::optional<std::string> defaultAddress) = 0;
    // Thread affinity: any.
    virtual std::string GetNodeDefaultAddress(NNodeTrackerClient::TNodeId nodeId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPersistentStateTransientCache)

////////////////////////////////////////////////////////////////////////////////

IPersistentStateTransientCachePtr CreatePersistentStateTransientCache(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
