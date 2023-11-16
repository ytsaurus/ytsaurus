#include "user_activity_tracker.h"
#include "private.h"
#include "user.h"
#include "config.h"


#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/security_server/proto/security_manager.pb.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

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

class TUserActivityTracker
    : public IUserActivityTracker
{
public:
    explicit TUserActivityTracker(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void Start() override
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

    void Stop() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

        FlushExecutor_.Reset();

        Reset();
    }

    void OnUserSeen(TUser* user) override
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

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TUserActivityTracker::OnDynamicConfigChanged, MakeWeak(this));

    THashMap<NObjectClient::TObjectId, NProto::TUserActivityStatisticsUpdate> UserToActivityStatistics_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);

    NConcurrency::TPeriodicExecutorPtr FlushExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void Reset()
    {
        UserToActivityStatistics_.clear();
    }

    void OnFlush()
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

    const TDynamicSecurityManagerConfigPtr& GetDynamicConfig()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->SecurityManager;
    }

    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr /*oldConfig*/)
    {
        FlushExecutor_->SetPeriod(GetDynamicConfig()->UserStatisticsFlushPeriod);
    }

    NProto::TUserActivityStatisticsUpdate AggregateUserStatistics(
        const NProto::TUserActivityStatisticsUpdate& accumulatedStatistics,
        const NProto::TUserActivityStatisticsUpdate& update)
    {
        return update.last_seen() > accumulatedStatistics.last_seen() ? update : accumulatedStatistics;
    }
};

////////////////////////////////////////////////////////////////////////////////

IUserActivityTrackerPtr CreateUserActivityTracker(NCellMaster::TBootstrap* bootstrap)
{
    return New<TUserActivityTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
