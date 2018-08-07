#include "config.h"
#include "private.h"
#include "public.h"
#include "tablet_cell_decommissioner.h"
#include "tablet_manager.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/tablet_server/tablet_manager.pb.h>

#include <yt/core/profiling/profiler.h>

#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NTabletServer {

using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NTabletServer::NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const auto static& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellDecommissioner::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletCellDecommissionerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
        , Profiler("/tablet_server/tablet_cell_decommissioner")
        , DecommissionExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
            BIND(&TImpl::CheckDecommission, MakeWeak(this)),
            Config_->DecommissionCheckPeriod))
        , KickOrphansExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
            BIND(&TImpl::CheckOrphans, MakeWeak(this)),
            Config_->OrphansCheckPeriod))
        , DecommissionThrottler_(CreateNamedReconfigurableThroughputThrottler(
            Config_->DecommissionThrottler,
            "TabletCellDecommission",
            TabletServerLogger,
            Profiler))
        , KickOrphansThrottler_(CreateNamedReconfigurableThroughputThrottler(
            Config_->KickOrphansThrottler,
            "KickOrphans",
            TabletServerLogger,
            Profiler))
    { }

    void Start()
    {
        DecommissionExecutor_->Start();
        KickOrphansExecutor_->Start();
    }

    void Stop()
    {
        DecommissionExecutor_->Stop();
        KickOrphansExecutor_->Stop();
    }

    void Reconfigure(TTabletCellDecommissionerConfigPtr config)
    {
        Config_ = std::move(config);

        DecommissionExecutor_->SetPeriod(Config_->DecommissionCheckPeriod);
        KickOrphansExecutor_->SetPeriod(Config_->OrphansCheckPeriod);

        DecommissionThrottler_->Reconfigure(Config_->DecommissionThrottler);
        KickOrphansThrottler_->Reconfigure(Config_->KickOrphansThrottler);
    }

private:
    TTabletCellDecommissionerConfigPtr Config_;
    const NCellMaster::TBootstrap* Bootstrap_;
    const NProfiling::TProfiler Profiler;
    const TPeriodicExecutorPtr DecommissionExecutor_;
    const TPeriodicExecutorPtr KickOrphansExecutor_;
    const IReconfigurableThroughputThrottlerPtr DecommissionThrottler_;
    const IReconfigurableThroughputThrottlerPtr KickOrphansThrottler_;

    void CheckDecommission()
    {
        PROFILE_TIMING("/check_decommission") {
            DoCheckDecommission();
        }
    }

    void CheckOrphans()
    {
        PROFILE_TIMING("/check_orphans") {
            DoCheckOrphans();
        }
    }

    void DoCheckDecommission()
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        THashSet<const TTabletCell*> retiringCells;
        for (const auto& pair : tabletManager->TabletActions()) {
            const auto* action = pair.second;

            if (IsObjectAlive(action) &&
                action->GetState() != ETabletActionState::Completed &&
                action->GetState() != ETabletActionState::Failed)
            {
                for (const auto* tablet : action->Tablets()) {
                    if (tablet->GetState() != ETabletState::Unmounted &&
                        tablet->GetCell()->GetDecommissioned())
                    {
                        retiringCells.insert(tablet->GetCell());
                    }
                }

                // When cell is decommissioned all tablet actions should be unlinked.
                for (const auto* cell : action->TabletCells()) {
                    if (cell->GetDecommissioned()) {
                        LOG_ERROR("Tablet action target cell is decommissioned (ActionId: %v, CellId: %v)",
                            action->GetId(),
                            cell->GetId());

                        retiringCells.insert(cell);
                    }
                }
            }
        }

        LOG_DEBUG("Tablet cell decommissioner observes decommissioned cells with tablets (CellCount: %v)",
            retiringCells.size());

        for (const auto& pair : tabletManager->TabletCells()) {
            const auto* cell = pair.second;

            if (IsObjectAlive(cell) &&
                cell->GetDecommissioned() &&
                !retiringCells.has(cell))
            {
                for (auto* tablet : cell->Tablets()) {
                    if (!DecommissionThrottler_->TryAcquire(1)) {
                        auto result = WaitFor(DecommissionThrottler_->Throttle(1));
                        if (!result.IsOK()) {
                            return;
                        }
                    }

                    TReqCreateTabletAction request;
                    request.set_kind(static_cast<int>(ETabletActionKind::Move));
                    ToProto(request.mutable_tablet_ids(), std::vector<TTabletId>{tablet->GetId()});

                    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
                    CreateMutation(hydraManager, request)
                        ->CommitAndLog(Logger);
                }

                if (Bootstrap_->IsPrimaryMaster()) {
                    const auto& statistics = cell->ClusterStatistics();
                    if (statistics.Decommissioned && statistics.TabletCount == 0) {
                        const auto& objectManager = Bootstrap_->GetObjectManager();
                        auto rootService = objectManager->GetRootService();
                        auto req = TYPathProxy::Remove(NObjectClient::FromObjectId(cell->GetId()));
                        ExecuteVerb(rootService, req);
                    }
                }
            }
        }
    }

    void DoCheckOrphans()
    {
        std::vector<TTabletActionId> orphans;

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        for (const auto& pair : tabletManager->TabletActions()) {
            const auto* action = pair.second;

            if (IsObjectAlive(action) &&
                action->GetState() == ETabletActionState::Orphaned)
            {
                if (KickOrphansThrottler_->TryAcquire(1)) {
                    orphans.push_back(action->GetId());
                } else {
                    break;
                }
            }
        }

        if (!orphans.empty()) {
            TReqKickOrphanedTabletActions request;
            ToProto(request.mutable_tablet_action_ids(), orphans);
            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TTabletCellDecommissioner::TTabletCellDecommissioner(
    TTabletCellDecommissionerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

TTabletCellDecommissioner::~TTabletCellDecommissioner() = default;

void TTabletCellDecommissioner::Start()
{
    Impl_->Start();
}

void TTabletCellDecommissioner::Stop()
{
    Impl_->Stop();
}

void TTabletCellDecommissioner::Reconfigure(TTabletCellDecommissionerConfigPtr config)
{
    Impl_->Reconfigure(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

