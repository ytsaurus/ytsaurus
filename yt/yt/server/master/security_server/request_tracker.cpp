#include "request_tracker.h"
#include "private.h"
#include "config.h"
#include "security_manager.h"
#include "user.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/ytlib/distributed_throttler/distributed_throttler.h>

namespace NYT::NSecurityServer {

using namespace NConcurrency;
using namespace NHydra;
using namespace NDistributedThrottler;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = SecurityServerLogger;

////////////////////////////////////////////////////////////////////////////////

TRequestTracker::TRequestTracker(
    const NDistributedThrottler::TDistributedThrottlerConfigPtr& userThrottlerConfig,
    NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , ThrottlerFactory_(Bootstrap_->CreateDistributedThrottlerFactory(
        userThrottlerConfig,
        NRpc::TDispatcher::Get()->GetHeavyInvoker(),
        "/security/master_cells",
        SecurityServerLogger(),
        SecurityProfiler().WithPrefix("/distributed_throttler")))
{ }

void TRequestTracker::Start()
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);

    const auto& hydraFacade = Bootstrap_->GetHydraFacade();
    AlivePeerCountExecutor_ = New<TPeriodicExecutor>(
        hydraFacade->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::SecurityManager),
        BIND(&TRequestTracker::OnUpdateAlivePeerCount, MakeWeak(this)),
        TDuration::Seconds(5));
    AlivePeerCountExecutor_->Start();
    OnUpdateAlivePeerCount();
}

void TRequestTracker::Stop()
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    for (auto [userId, user] : securityManager->Users()) {
        user->SetRequestRateThrottler(nullptr, EUserWorkloadType::Read);
        user->SetRequestRateThrottler(nullptr, EUserWorkloadType::Write);
        user->ResetRequestQueueSize();
    }

    AlivePeerCount_ = 0;
}

void TRequestTracker::ChargeUser(
    TUser* user,
    const TUserWorkload& workload)
{
    VerifyPersistentStateRead();

    switch (workload.Type) {
        case EUserWorkloadType::Read:
            user->Charge(workload);
            break;
        case EUserWorkloadType::Write: {
            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            if (hydraManager->IsLeader()) {
                user->Charge(workload);
            } else if (const auto& throttler = user->GetRequestRateThrottler(workload.Type)) {
                throttler->Acquire(workload.RequestCount);
            }
            break;
        }
        default:
            YT_ABORT();
    }
}

TFuture<void> TRequestTracker::ThrottleUserRequest(TUser* user, int requestCount, EUserWorkloadType workloadType)
{
    const auto& throttler = user->GetRequestRateThrottler(workloadType);
    return throttler ? throttler->Throttle(requestCount) : VoidFuture;
}

void TRequestTracker::SetUserRequestRateLimit(TUser* user, int limit, EUserWorkloadType type)
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* rootUser = securityManager->GetRootUser();
    YT_VERIFY(user != rootUser);

    user->SetRequestRateLimit(limit, type);
    ReconfigureUserRequestRateThrottlers(user);
}

void TRequestTracker::SetUserRequestLimits(TUser* user, TUserRequestLimitsConfigPtr config)
{
    YT_VERIFY(config);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* rootUser = securityManager->GetRootUser();
    YT_VERIFY(user != rootUser);

    user->SetObjectServiceRequestLimits(std::move(config));
    ReconfigureUserRequestRateThrottlers(user);
}

void TRequestTracker::ReconfigureUserRequestRateThrottlers(TUser* user)
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* rootUser = securityManager->GetRootUser();
    auto enableDistributedThrottler = GetDynamicConfig()->EnableDistributedThrottler;
    auto createUserThrottler = [&] (TUser* user, EUserWorkloadType workloadType) {
        if (enableDistributedThrottler && workloadType == EUserWorkloadType::Read && user != rootUser) {
            auto throttlerId = Format("%v:request_count:%v", user->GetName(), workloadType);
            return ThrottlerFactory_->GetOrCreateThrottler(std::move(throttlerId), New<TThroughputThrottlerConfig>());
        }
        return CreateReconfigurableThroughputThrottler(New<TThroughputThrottlerConfig>());
    };

    for (auto workloadType : {EUserWorkloadType::Read, EUserWorkloadType::Write}) {
        if (DistributedThrottlerEnabled_ != enableDistributedThrottler || !user->GetRequestRateThrottler(workloadType)) {
            user->SetRequestRateThrottler(createUserThrottler(user, workloadType), workloadType);
        }

        auto config = New<TThroughputThrottlerConfig>();
        config->Period = GetDynamicConfig()->RequestRateSmoothingPeriod;

        auto cellTag = Bootstrap_->GetMulticellManager()->GetCellTag();
        // Conversion to double is needed due to potential division below.
        std::optional<double> requestRateLimit = user->GetRequestRateLimit(workloadType, cellTag);
        // If there're three or more peers, divide user limits by the number of
        // followers (because it's they who handle read requests).
        // If there're two peers, there's only one follower - no division necessary.
        // If there's only one peer, its certainly being read from - no division necessary.
        if (!enableDistributedThrottler && requestRateLimit && workloadType == EUserWorkloadType::Read && AlivePeerCount_ > 2) {
            *requestRateLimit /= AlivePeerCount_ - 1;
        }

        config->Limit = requestRateLimit;

        user->GetRequestRateThrottler(workloadType)->Reconfigure(std::move(config));
    }
}

void TRequestTracker::SetUserRequestQueueSizeLimit(TUser* user, int limit)
{
    user->SetRequestQueueSizeLimit(limit);
}

bool TRequestTracker::TryIncrementRequestQueueSize(TUser* user)
{
    auto cellTag = Bootstrap_->GetMulticellManager()->GetCellTag();
    return user->TryIncrementRequestQueueSize(cellTag);
}

void TRequestTracker::DecrementRequestQueueSize(TUser* user)
{
    user->DecrementRequestQueueSize();
}

const TDynamicSecurityManagerConfigPtr& TRequestTracker::GetDynamicConfig()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->SecurityManager;
}

void TRequestTracker::OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr /*oldConfig*/)
{
    ReconfigureUserThrottlers();
    DistributedThrottlerEnabled_ = GetDynamicConfig()->EnableDistributedThrottler;
}

void TRequestTracker::ReconfigureUserThrottlers()
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    for (auto [userId, user] : securityManager->Users()) {
        if (IsObjectAlive(user)) {
            ReconfigureUserRequestRateThrottlers(user);
        }
    }
}

void TRequestTracker::OnUpdateAlivePeerCount()
{
    const auto& hydraFacade = Bootstrap_->GetHydraFacade();
    const auto& hydraManager = hydraFacade->GetHydraManager();
    auto alivePeerIds = hydraManager->GetAlivePeerIds();
    YT_LOG_DEBUG("Alive peers updated (AlivePeerIds: %v)", alivePeerIds);
    int peerCount = std::ssize(alivePeerIds);
    if (peerCount != AlivePeerCount_) {
        AlivePeerCount_ = peerCount;
        if (!GetDynamicConfig()->EnableDistributedThrottler) {
            ReconfigureUserThrottlers();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
