#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/security_server/proto/security_manager.pb.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TUserActivityTracker
    : public TRefCounted
{
public:
    explicit TUserActivityTracker(NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

    void OnUserSeen(TUser* user);

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TUserActivityTracker::OnDynamicConfigChanged, MakeWeak(this));

    THashMap<NObjectClient::TObjectId, NProto::TUserActivityStatisticsUpdate> UserToActivityStatistics_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);

    NConcurrency::TPeriodicExecutorPtr FlushExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void Reset();
    void OnFlush();

    const TDynamicSecurityManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr oldConfig);

    NProto::TUserActivityStatisticsUpdate AggregateUserStatistics(
        const NProto::TUserActivityStatisticsUpdate& accumulatedStatistics,
        const NProto::TUserActivityStatisticsUpdate& update);
};

DEFINE_REFCOUNTED_TYPE(TUserActivityTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
