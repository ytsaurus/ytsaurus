#pragma once

#include "public.h"
#include "node.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

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
    void OnNodeDestroyed(TCypressNode* trunkNode);
    void OnNodeRemovalFailed(TCypressNode* trunkNode);

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    const TClosure DynamicConfigChangedCallback_ = BIND(&TExpirationTracker::OnDynamicConfigChanged, MakeWeak(this));

    NConcurrency::TPeriodicExecutorPtr CheckExecutor_;

    TCypressNodeExpirationMap ExpirationMap_;
    THashSet<TCypressNode*> ExpiredNodes_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void RegisterNodeExpiration(TCypressNode* trunkNode, TInstant expirationTime);
    void UnregisterNodeExpiration(TCypressNode* trunkNode);

    void OnCheck();

    bool IsRecovery() const;

    const TDynamicCypressManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged();
};

DEFINE_REFCOUNTED_TYPE(TExpirationTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
