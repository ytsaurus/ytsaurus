#include "stdafx.h"
#include "request_tracker.h"
#include "user.h"
#include "config.h"
#include "security_manager.h"
#include "private.h"

#include <core/profiling/timing.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

#include <server/object_server/object_manager.h>

namespace NYT {
namespace NSecurityServer {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = SecurityServerLogger;

////////////////////////////////////////////////////////////////////////////////

TRequestTracker::TRequestTracker(
    TSecurityManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{ }

void TRequestTracker::StartFlush()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YCHECK(!FlushInvoker);
    FlushInvoker = New<TPeriodicExecutor>(
        Bootstrap->GetMetaStateFacade()->GetEpochInvoker(),
        BIND(&TRequestTracker::OnFlush, MakeWeak(this)),
        Config->StatisticsFlushPeriod,
        EPeriodicInvokerMode::Manual);
    FlushInvoker->Start();
}

void TRequestTracker::StopFlush()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (FlushInvoker) {
        FlushInvoker->Stop();
        FlushInvoker.Reset();
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
    FOREACH (auto* user, UsersWithRequestStatisticsUpdate) {
        user->SetRequestStatisticsUpdate(nullptr);
        objectManager->WeakUnrefObject(user);
    }    

    UpdateRequestStatisticsRequest.Clear();
    UsersWithRequestStatisticsUpdate.clear();
}

void TRequestTracker::OnFlush()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (UsersWithRequestStatisticsUpdate.empty()) {
        FlushInvoker->ScheduleNext();
        return;
    }

    LOG_DEBUG("Starting request statistics commit for %d users",
        UpdateRequestStatisticsRequest.updates_size());

    auto metaStateFacade = Bootstrap->GetMetaStateFacade();
    auto invoker = metaStateFacade->GetEpochInvoker();
    Bootstrap
        ->GetSecurityManager()
        ->CreateUpdateRequestStatisticsMutation(UpdateRequestStatisticsRequest)
        ->OnSuccess(BIND(&TRequestTracker::OnCommitSucceeded, MakeWeak(this)).Via(invoker))
        ->OnError(BIND(&TRequestTracker::OnCommitFailed, MakeWeak(this)).Via(invoker))
        ->PostCommit();

    Reset();
}

void TRequestTracker::OnCommitSucceeded()
{
    LOG_DEBUG("Request statistics commit succeeded");

    FlushInvoker->ScheduleOutOfBand();
    FlushInvoker->ScheduleNext();
}

void TRequestTracker::OnCommitFailed(const TError& error)
{
    LOG_ERROR(error, "Request statistics commit failed");

    FlushInvoker->ScheduleNext();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
