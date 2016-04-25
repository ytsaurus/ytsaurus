#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/security_server/security_manager.pb.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TRequestTracker
    : public TRefCounted
{
public:
    TRequestTracker(
        TSecurityManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

    void ChargeUser(
        TUser* user,
        int requestCount,
        TDuration readRequestTime,
        TDuration writeRequestTime);

    TFuture<void> ThrottleUser(
        TUser* user,
        int requestCount);

    void SetUserRequestRateLimit(
        TUser* user,
        double limit);

    void ReconfigureUserRequestRateThrottler(TUser* user);

private:
    const TSecurityManagerConfigPtr Config_;
    const NCellMaster::TBootstrap* Bootstrap_;

    NProto::TReqIncreaseUserStatistics Request_;
    std::vector<TUser*> UsersWithsEntry_;

    NConcurrency::TPeriodicExecutorPtr FlushExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void Reset();
    void OnFlush();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
