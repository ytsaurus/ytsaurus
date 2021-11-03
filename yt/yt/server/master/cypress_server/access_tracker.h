#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/proto/cypress_manager.pb.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_service.pb.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/error.h>

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

    void SetTouched(TCypressNode* trunkNode);

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TAccessTracker::OnDynamicConfigChanged, MakeWeak(this));

    struct TShard
    {
        NProto::TReqUpdateAccessStatistics UpdateAccessStatisticsRequest;
        std::vector<NObjectClient::TObjectId> NodesWithAccessStatisticsUpdate;

        NCypressClient::NProto::TReqTouchNodes TouchNodesRequest;
        std::vector<NObjectClient::TObjectId> TouchedNodes;

        YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock);
    };

    constexpr static int ShardCount = 256;
    static_assert(IsPowerOf2(ShardCount), "Number of shards must be a power of two");

    std::array<TShard, ShardCount> Shards_;

    NConcurrency::TPeriodicExecutorPtr FlushExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void Reset();
    void OnFlush();

    TShard* GetShard(TCypressNode* node);

    const TDynamicCypressManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr oldConfig = nullptr);
};

DEFINE_REFCOUNTED_TYPE(TAccessTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
