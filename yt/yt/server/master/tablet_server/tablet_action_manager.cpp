#include "config.h"
#include "private.h"
#include "public.h"
#include "tablet_action_manager.h"
#include "tablet_manager.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NTabletServer {

using namespace NConcurrency;
using namespace NTabletServer::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletActionManager::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , CleanupExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletManager),
            BIND(&TImpl::RunCleanup, MakeWeak(this))))
    { }

    void Start()
    {
        DoReconfigure();
        CleanupExecutor_->Start();
    }

    void Stop()
    {
        CleanupExecutor_->Stop();
    }

    void Reconfigure(TTabletActionManagerMasterConfigPtr config)
    {
        Config_ = std::move(config);
        DoReconfigure();
    }

private:
    TTabletActionManagerMasterConfigPtr Config_ = New<TTabletActionManagerMasterConfig>();
    const NCellMaster::TBootstrap* Bootstrap_;
    NConcurrency::TPeriodicExecutorPtr CleanupExecutor_;

    void DoReconfigure()
    {
        CleanupExecutor_->SetPeriod(Config_->TabletActionsCleanupPeriod);
    }

    void RunCleanup()
    {
        YT_LOG_DEBUG("Periodic tablet action cleanup started");

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        const auto now = TInstant::Now();

        std::vector<TTabletActionId> actionsPendingRemoval;
        for (const auto& pair : tabletManager->TabletActions()) {
            const auto* action = pair.second;
            if (IsObjectAlive(action) && action->IsFinished() && action->GetExpirationTime() <= now) {
                actionsPendingRemoval.push_back(pair.first);
            }
        }

        if (!actionsPendingRemoval.empty()) {
            YT_LOG_DEBUG("Destroying expired tablet actions (TabletActionIds: %v)",
                actionsPendingRemoval);

            TReqDestroyTabletActions request;
            ToProto(request.mutable_tablet_action_ids(), actionsPendingRemoval);

            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger);
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TTabletActionManager::TTabletActionManager(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TTabletActionManager::~TTabletActionManager() = default;

void TTabletActionManager::Stop()
{
    Impl_->Stop();
}

void TTabletActionManager::Start()
{
    Impl_->Start();
}

void TTabletActionManager::Reconfigure(TTabletActionManagerMasterConfigPtr config)
{
    Impl_->Reconfigure(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
