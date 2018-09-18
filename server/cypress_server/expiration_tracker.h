#pragma once

#include "public.h"
#include "node.h"

#include <yt/server/cell_master/public.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TExpirationTracker
    : public TRefCounted
{
public:
    TExpirationTracker(
        TCypressManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

    void Clear();

    void OnNodeExpirationTimeUpdated(TCypressNodeBase* trunkNode);
    void OnNodeDestroyed(TCypressNodeBase* trunkNode);
    void OnNodeRemovalFailed(TCypressNodeBase* trunkNode);

private:
    const TCypressManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;

    NConcurrency::TPeriodicExecutorPtr CheckExecutor_;

    TCypressNodeExpirationMap ExpirationMap_;
    THashSet<TCypressNodeBase*> ExpiredNodes_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void RegisterNodeExpiration(TCypressNodeBase* trunkNode, TInstant expirationTime);
    void UnregisterNodeExpiration(TCypressNodeBase* trunkNode);

    void OnCheck();

    bool IsRecovery();

};

DEFINE_REFCOUNTED_TYPE(TExpirationTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
