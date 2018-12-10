#include "request_tracker.h"
#include "private.h"
#include "config.h"
#include "security_manager.h"
#include "user.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/object_server/object_manager.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/config.h>

namespace NYT {
namespace NSecurityServer {

using namespace NConcurrency;
using namespace NHydra;

using NYT::ToProto;
using NYT::FromProto;

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

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    for (const auto& pair : securityManager->Users()) {
        auto* user = pair.second;
        ReconfigureUserRequestRateThrottler(user);
    }

    YCHECK(!FlushExecutor_);
    FlushExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
        BIND(&TRequestTracker::OnFlush, MakeWeak(this)),
        Config_->UserStatisticsFlushPeriod);
    FlushExecutor_->Start();
}

void TRequestTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    for (const auto& pair : securityManager->Users()) {
        auto* user = pair.second;
        user->SetRequestRateThrottler(nullptr, EUserWorkloadType::Read);
        user->SetRequestRateThrottler(nullptr, EUserWorkloadType::Write);
        user->SetRequestQueueSize(0);
    }

    FlushExecutor_.Reset();
    Reset();
}

void TRequestTracker::ChargeUser(
    TUser* user,
    const TUserWorkload& workload)
{
    switch (workload.Type) {
        case EUserWorkloadType::Read:
            DoChargeUser(user, workload);
            break;
        case EUserWorkloadType::Write: {
            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            if (hydraManager->IsLeader()) {
                DoChargeUser(user, workload);
            } else {
                user->GetRequestRateThrottler(workload.Type)->Acquire(workload.RequestCount);
            }
            break;
        }
        default:
            Y_UNREACHABLE();

    }
}

void TRequestTracker::DoChargeUser(
    TUser* user,
    const TUserWorkload& workload)
{
    YCHECK(FlushExecutor_);

    int index = user->GetRequestStatisticsUpdateIndex();
    if (index < 0) {
        index = Request_.entries_size();
        user->SetRequestStatisticsUpdateIndex(index);
        UsersWithsEntry_.push_back(user);

        auto* entry = Request_.add_entries();
        ToProto(entry->mutable_user_id(), user->GetId());
    
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->EphemeralRefObject(user);
    }
    
    auto now = NProfiling::GetInstant();
    auto* entry = Request_.mutable_entries(index);
    auto* statistics = entry->mutable_statistics();
    statistics->set_request_count(statistics->request_count() + workload.RequestCount);
    switch (workload.Type) {
        case EUserWorkloadType::Read:
            statistics->set_read_request_time(ToProto<i64>(FromProto<TDuration>(statistics->read_request_time()) + workload.Time));
            break;
        case EUserWorkloadType::Write:
            statistics->set_write_request_time(ToProto<i64>(FromProto<TDuration>(statistics->write_request_time()) + workload.Time));
            break;
        default:
            Y_UNREACHABLE();
    }
    statistics->set_access_time(ToProto<i64>(now));
}

TFuture<void> TRequestTracker::ThrottleUserRequest(TUser* user, int requestCount, EUserWorkloadType workloadType)
{
    return user->GetRequestRateThrottler(workloadType)->Throttle(requestCount);
}

void TRequestTracker::SetUserRequestRateLimit(TUser* user, int limit, EUserWorkloadType type)
{
    user->SetRequestRateLimit(limit, type);
    ReconfigureUserRequestRateThrottler(user);
}

void TRequestTracker::ReconfigureUserRequestRateThrottler(TUser* user)
{
    for (auto workloadType : {EUserWorkloadType::Read, EUserWorkloadType::Write}) {
        if (!user->GetRequestRateThrottler(workloadType)) {
            user->SetRequestRateThrottler(CreateReconfigurableThroughputThrottler(New<TThroughputThrottlerConfig>()), workloadType);
        }

        auto config = New<TThroughputThrottlerConfig>();
        config->Period = Config_->RequestRateSmoothingPeriod;
        config->Limit = user->GetRequestRateLimit(workloadType);
        user->GetRequestRateThrottler(workloadType)->Reconfigure(std::move(config));
    }
}

void TRequestTracker::SetUserRequestQueueSizeLimit(TUser* user, int limit)
{
    user->SetRequestQueueSizeLimit(limit);
}

bool TRequestTracker::TryIncreaseRequestQueueSize(TUser* user)
{
    auto size = user->GetRequestQueueSize();
    auto limit = user->GetRequestQueueSizeLimit();
    if (size >= limit) {
        return false;
    }
    user->SetRequestQueueSize(size + 1);
    return true;
}

void TRequestTracker::DecreaseRequestQueueSize(TUser* user)
{
    auto size = user->GetRequestQueueSize();
    YCHECK(size > 0);
    user->SetRequestQueueSize(size - 1);
}

void TRequestTracker::Reset()
{
    const auto& objectManager = Bootstrap_->GetObjectManager();
    for (auto* user : UsersWithsEntry_) {
        user->SetRequestStatisticsUpdateIndex(-1);
        objectManager->EphemeralUnrefObject(user);
    }

    Request_.Clear();
    UsersWithsEntry_.clear();
}

void TRequestTracker::OnFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (UsersWithsEntry_.empty() ||
        !hydraManager->IsActiveLeader() && !hydraManager->IsActiveFollower())
    {
        return;
    }

    LOG_DEBUG("Starting user statistics commit (UserCount: %v)",
        Request_.entries_size());

    const auto& hydraFacade = Bootstrap_->GetHydraFacade();
    auto mutation = CreateMutation(hydraFacade->GetHydraManager(), Request_);
    mutation->SetAllowLeaderForwarding(true);
    auto asyncResult = mutation->CommitAndLog(Logger);

    Reset();

    Y_UNUSED(WaitFor(asyncResult));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
