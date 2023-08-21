#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/ytlib/distributed_throttler/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TRequestTracker
    : public TRefCounted
{
public:
    TRequestTracker(
        const NDistributedThrottler::TDistributedThrottlerConfigPtr& userThrottlerConfig,
        NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

    void ChargeUser(
        TUser* user,
        const TUserWorkload& workload);

    TFuture<void> ThrottleUserRequest(
        TUser* user,
        int requestCount,
        EUserWorkloadType type);

    void SetUserRequestRateLimit(
        TUser* user,
        int limit,
        EUserWorkloadType type);

    void SetUserRequestLimits(
        TUser* user,
        TUserRequestLimitsConfigPtr config);

    void ReconfigureUserRequestRateThrottlers(TUser* user);

    void SetUserRequestQueueSizeLimit(
        TUser* user,
        int limit);

    bool TryIncreaseRequestQueueSize(TUser* user);
    void DecreaseRequestQueueSize(TUser* user);

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const NDistributedThrottler::IDistributedThrottlerFactoryPtr ThrottlerFactory_;

    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TRequestTracker::OnDynamicConfigChanged, MakeWeak(this));

    NConcurrency::TPeriodicExecutorPtr AlivePeerCountExecutor_;
    int AlivePeerCount_ = 0;
    bool DistributedThrottlerEnabled_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void DoChargeUser(
        TUser* user,
        const TUserWorkload& workload);

    const TDynamicSecurityManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr oldConfig);
    void ReconfigureUserThrottlers();
    void OnUpdateAlivePeerCount();
};

DEFINE_REFCOUNTED_TYPE(TRequestTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
