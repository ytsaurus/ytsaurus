#include "user_activity_tracker.h"
#include "private.h"
#include "user.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

namespace NYT::NSecurityServer {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SecurityServerLogger;

////////////////////////////////////////////////////////////////////////////////

TUserActivityTracker::TUserActivityTracker(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

void TUserActivityTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(!FlushExecutor_);
    FlushExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
        BIND(&TUserActivityTracker::OnFlush, MakeWeak(this)),
        GetDynamicConfig()->UserStatisticsFlushPeriod);
    FlushExecutor_->Start();

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
}

void TUserActivityTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

    FlushExecutor_.Reset();

    Reset();
}

void TUserActivityTracker::OnUserSeen(TUser* user)
{
    Bootstrap_->VerifyPersistentStateRead();

    YT_VERIFY(FlushExecutor_);
    YT_VERIFY(IsObjectAlive(user));

    auto guard = Guard(Lock);

    auto now = NProfiling::GetInstant();
    NProto::TUserActivityStatisticsUpdate update;
    update.set_last_seen(NYT::ToProto<i64>(now));
    ToProto(update.mutable_user_id(), user->GetId());

    auto accumulatedStatistics = UserToActivityStatistics_.empty() ? update : UserToActivityStatistics_[user->GetId()];
    UserToActivityStatistics_[user->GetId()] = AggregateUserStatistics(accumulatedStatistics, update);
}

void TUserActivityTracker::Reset()
{
    UserToActivityStatistics_.clear();
}

void TUserActivityTracker::OnFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (!hydraManager->IsActive()) {
        return;
    }

    if (UserToActivityStatistics_.size() > 0) {
        YT_LOG_DEBUG("Starting user activity statistics commit (UserCount: %v)",
            UserToActivityStatistics_.size());

        NProto::TReqUpdateUserActivityStatistics request;
        for (const auto& [userId, statisticsUpdate] : UserToActivityStatistics_) {
            *request.add_updates() = statisticsUpdate;
        }

        auto mutation = CreateMutation(hydraManager, request);
        mutation->SetAllowLeaderForwarding(true);
        auto asyncMutationResult = mutation->CommitAndLog(Logger).AsVoid();

        Reset();

        Y_UNUSED(WaitFor(asyncMutationResult));
    } else {
        Reset();
    }
}

const TDynamicSecurityManagerConfigPtr& TUserActivityTracker::GetDynamicConfig()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->SecurityManager;
}

void TUserActivityTracker::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    FlushExecutor_->SetPeriod(GetDynamicConfig()->UserStatisticsFlushPeriod);
}

NProto::TUserActivityStatisticsUpdate TUserActivityTracker::AggregateUserStatistics(
    const NProto::TUserActivityStatisticsUpdate& accumulatedStatistics,
    const NProto::TUserActivityStatisticsUpdate& update)
{
    return update.last_seen() > accumulatedStatistics.last_seen() ? update : accumulatedStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
