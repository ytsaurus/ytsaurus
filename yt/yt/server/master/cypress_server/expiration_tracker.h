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

    void OnNodeExpirationTimeUpdated(
        TCypressNode* trunkNode,
        std::optional<TInstant> oldExpirationTime = std::nullopt);

    void OnNodeExpirationTimeoutUpdated(
        TCypressNode* trunkNode,
        std::optional<TDuration> oldExpirationTimeout = std::nullopt);
    void OnNodeTouched(TCypressNode* trunkNode);

    void OnNodeDestroyed(TCypressNode* trunkNode);
    void OnNodeRemovalFailed(TCypressNode* trunkNode);

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    const NProfiling::TBufferedProducerPtr BufferedProducer_ = New<NProfiling::TBufferedProducer>();

    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TExpirationTracker::OnDynamicConfigChanged, MakeWeak(this));

    NConcurrency::TPeriodicExecutorPtr CheckExecutor_;

    // NB: Nodes that have both expiration time and expiration timeout may appear twice here.
    TCypressNodeExpirationMap ExpirationMap_;
    THashSet<TCypressNode*> ScheduledForRemovalNodes_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void RegisterNodeExpirationTime(TCypressNode* trunkNode, TInstant expirationTime);
    void RegisterNodeExpirationTimeout(TCypressNode* trunkNode, std::optional<TInstant> touchTimeOverride = {});
    void UnregisterNodeExpirationTime(TCypressNode* trunkNode);
    void UnregisterNodeExpirationTimeout(TCypressNode* trunkNode);

    bool IsNodeLocked(TCypressNode* trunkNode) const;

    void RunCheckIteration();
    void CollectAndRemoveExpiredNodes(TInstant checkTime);
    void UpdateProfiling(TInstant checkTime);

    void RemoveExpiredNodesViaClient(const std::vector<NObjectServer::TEphemeralObjectPtr<TCypressNode>>& trunkNodes);
    void RemoveExpiredNodesViaMutation(const std::vector<TCypressNode*>& trunkNodes);

    bool IsRecovery() const;

    const TDynamicCypressManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr oldConfig);
};

DEFINE_REFCOUNTED_TYPE(TExpirationTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
