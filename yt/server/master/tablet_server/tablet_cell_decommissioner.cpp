#include "config.h"
#include "private.h"
#include "public.h"
#include "tablet_cell_decommissioner.h"
#include "tablet_manager.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/core/profiling/profiler.h>

#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NTabletServer {

using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NTabletServer::NProto;
using namespace NTabletClient;
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
        auto retiringCells = GetDecommissionedCellsUsedByActions();

        YT_LOG_DEBUG("Tablet cell decommissioner observes decommissioned cells with tablets (CellCount: %v)",
            retiringCells.size());

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        if (Config_->EnableTabletCellDecommission) {
            for (const auto& pair : tabletManager->TabletCells()) {
                const auto* cell = pair.second;

                if (IsObjectAlive(cell) &&
                    cell->DecommissionStarted() &&
                    !retiringCells.contains(cell))
                {
                    MoveTabletsToOtherCells(cell);
                    RequestTabletCellDecommissonOnNode(cell);
                }
            }
        }

        if (Bootstrap_->IsPrimaryMaster() && Config_->EnableTabletCellRemoval) {
            for (const auto& pair : tabletManager->TabletCells()) {
                const auto* cell = pair.second;

                if (IsObjectAlive(cell) &&
                    cell->DecommissionStarted() &&
                    !retiringCells.contains(cell))
                {
                    RemoveCellIfDecommissioned(cell);
                }
            }
        }
    }

    THashSet<const TTabletCell*> GetDecommissionedCellsUsedByActions()
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
                        tablet->GetCell()->DecommissionStarted())
                    {
                        retiringCells.insert(tablet->GetCell());
                    }
                }

                // When cell is decommissioned all tablet actions should be unlinked.
                for (const auto* cell : action->TabletCells()) {
                    if (cell->DecommissionStarted()) {
                        YT_LOG_ERROR("Tablet action target cell is decommissioned (ActionId: %v, CellId: %v)",
                            action->GetId(),
                            cell->GetId());

                        retiringCells.insert(cell);
                    }
                }
            }
        }

        return retiringCells;
    }

    void MoveTabletsToOtherCells(const TTabletCell* cell)
    {
        for (auto* tablet : cell->Tablets()) {
            if (!DecommissionThrottler_->TryAcquire(1)) {
                return;
            }

            TReqCreateTabletAction request;
            request.set_kind(static_cast<int>(ETabletActionKind::Move));
            ToProto(request.mutable_tablet_ids(), std::vector<TTabletId>{tablet->GetId()});

            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger);
        }
    }

    void RequestTabletCellDecommissonOnNode(const TTabletCell* cell)
    {
        const auto& statistics = cell->ClusterStatistics();
        if (!Bootstrap_->IsPrimaryMaster() ||
            cell->GetTabletCellLifeStage() != ETabletCellLifeStage::DecommissioningOnMaster ||
            !statistics.Decommissioned ||
            statistics.TabletCount != 0)
        {
            return;
        }

        TReqOnTabletCellDecommisionedOnMaster request;
        ToProto(request.mutable_cell_id(), cell->GetId());

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
    }

    void RemoveCellIfDecommissioned(const TTabletCell* cell)
    {
        const auto& statistics = cell->ClusterStatistics();
        if (!Bootstrap_->IsPrimaryMaster() ||
            !cell->DecommissionCompleted() ||
            !statistics.Decommissioned ||
            statistics.TabletCount != 0)
        {
            return;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto rootService = objectManager->GetRootService();
        auto req = TYPathProxy::Remove(NObjectClient::FromObjectId(cell->GetId()));
        ExecuteVerb(rootService, req);
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

} // namespace NYT::NTabletServer

