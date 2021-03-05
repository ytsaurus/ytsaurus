#pragma once

#include "public.h"
#include "node.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TExpirationTracker
    : public TRefCounted
{
public:
    explicit TExpirationTracker(NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

    void Clear();

    void OnNodeExpirationTimeUpdated(TCypressNode* trunkNode);

    void OnNodeExpirationTimeoutUpdated(TCypressNode* trunkNode);
    void OnNodeTouched(TCypressNode* trunkNode);

    void OnNodeDestroyed(TCypressNode* trunkNode);
    void OnNodeRemovalFailed(TCypressNode* trunkNode);

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TExpirationTracker::OnDynamicConfigChanged, MakeWeak(this));

    NConcurrency::TPeriodicExecutorPtr CheckExecutor_;

    // NB: nodes that have both expiration time and expiration timeout may appear twice here.
    TCypressNodeExpirationMap ExpirationMap_;
    THashSet<TCypressNode*> ExpiredNodes_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void RegisterNodeExpirationTime(TCypressNode* trunkNode, TInstant expirationTime);
    void RegisterNodeExpirationTimeout(TCypressNode* trunkNode, TInstant expirationTime);
    void UnregisterNodeExpirationTime(TCypressNode* trunkNode);
    void UnregisterNodeExpirationTimeout(TCypressNode* trunkNode);

    bool IsNodeLocked(TCypressNode* trunkNode) const;

    void OnCheck();

    bool IsRecovery() const;
    bool IsMutationLoggingEnabled() const;

    const TDynamicCypressManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr oldConfig = nullptr);
};

DEFINE_REFCOUNTED_TYPE(TExpirationTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
