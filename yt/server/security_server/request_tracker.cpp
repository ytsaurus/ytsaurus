#include "stdafx.h"
#include "request_tracker.h"
#include "user.h"
#include "config.h"
#include "security_manager.h"
#include "private.h"

#include <core/profiling/timing.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>

#include <server/object_server/object_manager.h>

namespace NYT {
namespace NSecurityServer {

using namespace NConcurrency;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SecurityServerLogger;

////////////////////////////////////////////////////////////////////////////////

TRequestTracker::TRequestTracker(
    TSecurityManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{ }

void TRequestTracker::StartFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YCHECK(!FlushExecutor);
    FlushExecutor = New<TPeriodicExecutor>(
        Bootstrap->GetHydraFacade()->GetEpochAutomatonInvoker(),
        BIND(&TRequestTracker::OnFlush, MakeWeak(this)),
        Config->StatisticsFlushPeriod,
        EPeriodicExecutorMode::Manual);
    FlushExecutor->Start();
}

void TRequestTracker::StopFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (FlushExecutor) {
        FlushExecutor->Stop();
        FlushExecutor.Reset();
    }

    Reset();
}

void TRequestTracker::ChargeUser(TUser* user, int requestCount)
{
    auto* update = user->GetRequestStatisticsUpdate();
    if (!update) {
        update = UpdateRequestStatisticsRequest.add_updates();
        ToProto(update->mutable_user_id(), user->GetId());
    
        user->SetRequestStatisticsUpdate(update);
        UsersWithRequestStatisticsUpdate.push_back(user);
    
        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->WeakRefObject(user);
    }
    
    auto now = NProfiling::CpuInstantToInstant(NProfiling::GetCpuInstant());
    update->set_access_time(now.MicroSeconds());
    update->set_request_counter_delta(update->request_counter_delta() + requestCount);
}

void TRequestTracker::Reset()
{
    auto objectManager = Bootstrap->GetObjectManager();
    for (auto* user : UsersWithRequestStatisticsUpdate) {
        user->SetRequestStatisticsUpdate(nullptr);
        objectManager->WeakUnrefObject(user);
    }    

    UpdateRequestStatisticsRequest.Clear();
    UsersWithRequestStatisticsUpdate.clear();
}

void TRequestTracker::OnFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (UsersWithRequestStatisticsUpdate.empty()) {
        FlushExecutor->ScheduleNext();
        return;
    }

    LOG_DEBUG("Starting request statistics commit for %d users",
        UpdateRequestStatisticsRequest.updates_size());

    auto hydraFacade = Bootstrap->GetHydraFacade();
    auto invoker = hydraFacade->GetEpochAutomatonInvoker();
    auto this_ = MakeStrong(this);
    Bootstrap
        ->GetSecurityManager()
        ->CreateUpdateRequestStatisticsMutation(UpdateRequestStatisticsRequest)
        ->Commit()
        .Subscribe(BIND([this, this_] (TErrorOr<TMutationResponse> error) {
            if (error.IsOK()) {
                FlushExecutor->ScheduleOutOfBand();
            }
            FlushExecutor->ScheduleNext();
        }).Via(invoker));

    Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
