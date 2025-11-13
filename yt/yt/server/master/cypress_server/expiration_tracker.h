#pragma once

#include "public.h"
#include "node.h"

#include <yt/yt/server/master/cypress_server/proto/expiration_tracker.pb.h>

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cell_master/automaton.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class IExpirationTracker
    : public virtual TRefCounted
{
public:
    virtual void OnNodeExpirationTimeUpdated(
        TCypressNode* trunkNode,
        std::optional<TCypressNode::TExpirationTimeProperties::TView> oldExpirationTimeProperties = {}) = 0;

    virtual void OnNodeExpirationTimeoutUpdated(
        TCypressNode* trunkNode,
        std::optional<TCypressNode::TExpirationTimeoutProperties::TView> oldExpirationTimeoutProperties = {}) = 0;

    virtual void OnNodeTouched(TCypressNode* trunkNode) = 0;

    virtual void OnNodeDestroyed(TCypressNode* trunkNode) = 0;
};

DEFINE_REFCOUNTED_TYPE(IExpirationTracker)

////////////////////////////////////////////////////////////////////////////////

IExpirationTrackerPtr CreateExpirationTracker(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
