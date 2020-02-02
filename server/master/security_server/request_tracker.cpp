#include "request_tracker.h"
#include "private.h"
#include "config.h"
#include "security_manager.h"
#include "user.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/multicell_manager.h>
#include <yt/server/master/cell_master/config.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/config.h>

#include <yt/ytlib/election/cell_manager.h>

namespace NYT::NSecurityServer {

using namespace NConcurrency;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TRequestTracker::TRequestTracker(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

void TRequestTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
    OnDynamicConfigChanged();

    const auto& hydraFacade = Bootstrap_->GetHydraFacade();
    const auto& hydraManager = hydraFacade->GetHydraManager();
    AlivePeerCount_ = static_cast<int>(hydraManager->AlivePeers().size());
    hydraManager->SubscribeAlivePeerSetChanged(
        BIND(&TRequestTracker::OnAlivePeerSetChanged, MakeWeak(this))
            .Via(hydraFacade->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::SecurityManager)));
}

void TRequestTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    for (auto [userId, user] : securityManager->Users()) {
        user->SetRequestRateThrottler(nullptr, EUserWorkloadType::Read);
        user->SetRequestRateThrottler(nullptr, EUserWorkloadType::Write);
        user->SetRequestQueueSize(0);
        user->SetNeedsProfiling(false);
    }

    AlivePeerCount_ = 0;
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
                const auto& throttler = user->GetRequestRateThrottler(workload.Type);
                if (throttler) {
                    throttler->Acquire(workload.RequestCount);
                }
            }
            break;
        }
        default:
            YT_ABORT();
    }
}

void TRequestTracker::DoChargeUser(
    TUser* user,
    const TUserWorkload& workload)
{
    auto& statistics = user->Statistics()[workload.Type];
    statistics.RequestCount += workload.RequestCount;
    statistics.RequestTime += workload.RequestTime;
    user->SetNeedsProfiling(true);
}

TFuture<void> TRequestTracker::ThrottleUserRequest(TUser* user, int requestCount, EUserWorkloadType workloadType)
{
    const auto& throttler = user->GetRequestRateThrottler(workloadType);
    return throttler ? throttler->Throttle(requestCount) : VoidFuture;
}

void TRequestTracker::SetUserRequestRateLimit(TUser* user, int limit, EUserWorkloadType type)
{
    user->SetRequestRateLimit(limit, type);
    ReconfigureUserRequestRateThrottler(user);
}

void TRequestTracker::SetUserRequestLimits(TUser* user, TUserRequestLimitsConfigPtr config)
{
    user->SetRequestLimits(std::move(config));
    ReconfigureUserRequestRateThrottler(user);
}

void TRequestTracker::ReconfigureUserRequestRateThrottler(TUser* user)
{
    for (auto workloadType : {EUserWorkloadType::Read, EUserWorkloadType::Write}) {
        if (!user->GetRequestRateThrottler(workloadType)) {
            user->SetRequestRateThrottler(CreateReconfigurableThroughputThrottler(New<TThroughputThrottlerConfig>()), workloadType);
        }

        auto config = New<TThroughputThrottlerConfig>();
        config->Period = GetDynamicConfig()->RequestRateSmoothingPeriod;

        auto cellTag = Bootstrap_->GetMulticellManager()->GetCellTag();
        auto requestRateLimit = user->GetRequestRateLimit(workloadType, cellTag);

        // If there're three or more peers, divide user limits by the number of
        // followers (because it's they who handle read requests).
        // If there're two peers, there's only one follower - no division necessary.
        // If there's only one peer, its certainly being read from - no division necessary.
        if (workloadType == EUserWorkloadType::Read && AlivePeerCount_ > 2) {
            requestRateLimit /= AlivePeerCount_ - 1;
        }

        config->Limit = requestRateLimit;

        user->GetRequestRateThrottler(workloadType)->Reconfigure(std::move(config));
    }
}

void TRequestTracker::SetUserRequestQueueSizeLimit(TUser* user, int limit)
{
    user->SetRequestQueueSizeLimit(limit);
}

bool TRequestTracker::TryIncreaseRequestQueueSize(TUser* user)
{
    auto cellTag = Bootstrap_->GetMulticellManager()->GetCellTag();
    auto limit = user->GetRequestQueueSizeLimit(cellTag);

    auto size = user->GetRequestQueueSize();
    if (size >= limit) {
        return false;
    }
    user->SetRequestQueueSize(size + 1);
    user->SetNeedsProfiling(true);
    return true;
}

void TRequestTracker::DecreaseRequestQueueSize(TUser* user)
{
    auto size = user->GetRequestQueueSize();
    YT_VERIFY(size > 0);
    user->SetRequestQueueSize(size - 1);
    user->SetNeedsProfiling(true);
}

const TDynamicSecurityManagerConfigPtr& TRequestTracker::GetDynamicConfig()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->SecurityManager;
}

void TRequestTracker::OnDynamicConfigChanged()
{
    ReconfigureUserThrottlers();
}

void TRequestTracker::ReconfigureUserThrottlers()
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    for (auto [userId, user] : securityManager->Users()) {
        if (IsObjectAlive(user)) {
            ReconfigureUserRequestRateThrottler(user);
        }
    }
}

void TRequestTracker::OnAlivePeerSetChanged(const THashSet<NElection::TPeerId>& alivePeers)
{
    auto peerCount = static_cast<int>(alivePeers.size());
    if (peerCount != AlivePeerCount_) {
        AlivePeerCount_ = peerCount;
        ReconfigureUserThrottlers();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
