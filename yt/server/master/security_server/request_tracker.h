#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/error.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TRequestTracker
    : public TRefCounted
{
public:
    explicit TRequestTracker(NCellMaster::TBootstrap* bootstrap);

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

    void ReconfigureUserRequestRateThrottler(TUser* user);

    void SetUserRequestQueueSizeLimit(
        TUser* user,
        int limit);

    bool TryIncreaseRequestQueueSize(TUser* user);
    void DecreaseRequestQueueSize(TUser* user);

private:
    const NCellMaster::TBootstrap* Bootstrap_;

    const TClosure DynamicConfigChangedCallback_ = BIND(&TRequestTracker::OnDynamicConfigChanged, MakeWeak(this));

    int AlivePeerCount_ = 0;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void DoChargeUser(
        TUser* user,
        const TUserWorkload& workload);

    const TDynamicSecurityManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged();
    void ReconfigureUserThrottlers();
    void OnAlivePeerSetChanged(const THashSet<NElection::TPeerId>& alivePeers);
};

DEFINE_REFCOUNTED_TYPE(TRequestTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
