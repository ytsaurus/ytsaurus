#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/cypress_server/proto/cypress_manager.pb.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/error.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TAccessTracker
    : public TRefCounted
{
public:
    explicit TAccessTracker(NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

    void SetModified(
        TCypressNode* node,
        EModificationType modificationType);

    void SetAccessed(TCypressNode* trunkNode);

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    const TClosure DynamicConfigChangedCallback_ = BIND(&TAccessTracker::OnDynamicConfigChanged, MakeWeak(this));

    NProto::TReqUpdateAccessStatistics UpdateAccessStatisticsRequest_;
    std::vector<TCypressNode*> NodesWithAccessStatisticsUpdate_;

    NConcurrency::TPeriodicExecutorPtr FlushExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void Reset();
    void OnFlush();

    const TDynamicCypressManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged();
};

DEFINE_REFCOUNTED_TYPE(TAccessTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
