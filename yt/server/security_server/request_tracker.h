#pragma once

#include "public.h"

#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/error.h>

#include <server/cell_master/public.h>

#include <server/security_server/security_manager.pb.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TRequestTracker
    : public TRefCounted
{
public:
    explicit TRequestTracker(
        TSecurityManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void StartFlush();
    void StopFlush();

    void ChargeUser(TUser* user, int requestCount);

private:
    TSecurityManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    NProto::TMetaReqUpdateRequestStatistics UpdateRequestStatisticsRequest;
    std::vector<TUser*> UsersWithRequestStatisticsUpdate;

    TPeriodicInvokerPtr FlushInvoker;


    void Reset();

    void OnFlush();
    void OnCommitSucceeded();
    void OnCommitFailed(const TError& error);


    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
