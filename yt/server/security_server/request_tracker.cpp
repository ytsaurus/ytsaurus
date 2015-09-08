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
    : Config_(config)
    , Bootstrap_(bootstrap)
{ }

void TRequestTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YCHECK(!FlushExecutor_);
    FlushExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
        BIND(&TRequestTracker::OnFlush, MakeWeak(this)),
        Config_->UserStatisticsFlushPeriod,
        EPeriodicExecutorMode::Manual);
    FlushExecutor_->Start();
}

void TRequestTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto securityManager = Bootstrap_->GetSecurityManager();
    for (const auto& pair : securityManager->Users()) {
        auto* user = pair.second;
        user->ResetRequestRate();
    }

    FlushExecutor_.Reset();
    Reset();
}

void TRequestTracker::ChargeUser(
    TUser* user,
    int requestCount,
    TDuration readRequestTime,
    TDuration writeRequestTime)
{
    YCHECK(FlushExecutor_);

    int index = user->GetRequestStatisticsUpdateIndex();
    if (index < 0) {
        index = Request_.entries_size();
        user->SetRequestStatisticsUpdateIndex(index);
        UsersWithsEntry_.push_back(user);

        auto* entry = Request_.add_entries();
        ToProto(entry->mutable_user_id(), user->GetId());
    
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->WeakRefObject(user);
    }
    
    auto now = NProfiling::CpuInstantToInstant(NProfiling::GetCpuInstant());
    auto* entry = Request_.mutable_entries(index);
    auto* statistics = entry->mutable_statistics();
    statistics->set_request_counter(statistics->request_counter() + requestCount);
    statistics->set_read_request_timer(statistics->read_request_timer() + readRequestTime.MicroSeconds());
    statistics->set_write_request_timer(statistics->write_request_timer() + writeRequestTime.MicroSeconds());
    statistics->set_access_time(now.MicroSeconds());
}

void TRequestTracker::Reset()
{
    auto objectManager = Bootstrap_->GetObjectManager();
    for (auto* user : UsersWithsEntry_) {
        user->SetRequestStatisticsUpdateIndex(-1);
        objectManager->WeakUnrefObject(user);
    }    

    Request_.Clear();
    UsersWithsEntry_.clear();
}

void TRequestTracker::OnFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (UsersWithsEntry_.empty() ||
        !hydraManager->IsActiveLeader() && !hydraManager->IsActiveFollower())
    {
        FlushExecutor_->ScheduleNext();
        return;
    }

    LOG_DEBUG("Starting user statistics commit for %v users",
        Request_.entries_size());

    auto hydraFacade = Bootstrap_->GetHydraFacade();
    auto invoker = hydraFacade->GetEpochAutomatonInvoker();
    CreateMutation(hydraFacade->GetHydraManager(), Request_)
        ->SetAllowLeaderForwarding(true)
        ->Commit()
        .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TMutationResponse>& error) {
            if (error.IsOK()) {
                FlushExecutor_->ScheduleOutOfBand();
            } else {
                LOG_ERROR(error, "Error committing user statistics update mutation");
            }
            FlushExecutor_->ScheduleNext();
        }).Via(invoker));

    Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
