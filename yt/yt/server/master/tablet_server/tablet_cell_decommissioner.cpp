#include "config.h"
#include "private.h"
#include "public.h"
#include "tablet_cell_decommissioner.h"
#include "tablet_manager.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/server/master/cell_server/tamed_cell_manager.h>
#include <yt/server/master/cell_server/cell_base.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/core/profiling/profiler.h>

#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NTabletServer {

using namespace NCellServer;
using namespace NConcurrency;
using namespace NObjectServer;
using namespace NSecurityClient;
using namespace NTabletClient;
using namespace NTabletServer::NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const auto static& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellDecommissioner::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Profiler("/tablet_server/tablet_cell_decommissioner")
        , Config_(New<TTabletCellDecommissionerConfig>())
        , DecommissionExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletDecommissioner),
            BIND(&TImpl::CheckDecommission, MakeWeak(this))))
        , KickOrphansExecutor_ (New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletDecommissioner),
            BIND(&TImpl::CheckOrphans, MakeWeak(this))))
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
        DoReconfigure();
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
        DoReconfigure();
    }

private:
    const NCellMaster::TBootstrap* Bootstrap_;
    const NProfiling::TProfiler Profiler;
    TTabletCellDecommissionerConfigPtr Config_;
    TPeriodicExecutorPtr DecommissionExecutor_;
    TPeriodicExecutorPtr KickOrphansExecutor_;
    IReconfigurableThroughputThrottlerPtr DecommissionThrottler_;
    IReconfigurableThroughputThrottlerPtr KickOrphansThrottler_;

    void DoReconfigure()
    {
        DecommissionExecutor_->SetPeriod(Config_->DecommissionCheckPeriod);
        KickOrphansExecutor_->SetPeriod(Config_->OrphansCheckPeriod);

        DecommissionThrottler_->Reconfigure(Config_->DecommissionThrottler);
        KickOrphansThrottler_->Reconfigure(Config_->KickOrphansThrottler);
    }

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

        const auto& cellManager = Bootstrap_->GetTamedCellManager();

        if (Config_->EnableTabletCellDecommission) {
            for (const auto [cellId, cell] : cellManager->Cells()) {
                if (IsObjectAlive(cell) &&
                    cell->IsDecommissionStarted() &&
                    !retiringCells.contains(cell))
                {
                    if (cell->GetType() == EObjectType::TabletCell) {
                        MoveTabletsToOtherCells(cell->As<TTabletCell>());
                    }
                    RequestTabletCellDecommissonOnNode(cell);
                }
            }
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster() && Config_->EnableTabletCellRemoval) {
            for (const auto [cellId, cell] : cellManager->Cells()) {
                if (IsObjectAlive(cell) && !retiringCells.contains(cell)) {
                    RemoveCellIfDecommissioned(cell);
                }
            }
        }
    }

    THashSet<const TCellBase*> GetDecommissionedCellsUsedByActions()
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        THashSet<const TCellBase*> retiringCells;
        for (const auto [actionId, action] : tabletManager->TabletActions()) {
            if (IsObjectAlive(action) &&
                action->GetState() != ETabletActionState::Completed &&
                action->GetState() != ETabletActionState::Failed)
            {
                for (const auto* tablet : action->Tablets()) {
                    if (tablet->GetState() != ETabletState::Unmounted &&
                        tablet->GetCell()->IsDecommissionStarted())
                    {
                        retiringCells.insert(tablet->GetCell());
                    }
                }

                // When cell is decommissioned all tablet actions should be unlinked.
                for (const auto* cell : action->TabletCells()) {
                    if (cell->IsDecommissionStarted()) {
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

    bool IsCellDecommissionedAtAllMasters(const TCellBase* cell)
    {
        const auto& status = cell->GossipStatus().Cluster();
        if (!status.Decommissioned) {
            return false;
        }
        if (cell->GetType() == EObjectType::TabletCell) {
            const auto& statistics = cell->As<TTabletCell>()->GossipStatistics().Cluster();
            if (statistics.TabletCount != 0) {
                return false;
            }
        }
        return true;
    }

    void RequestTabletCellDecommissonOnNode(const TCellBase* cell)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsPrimaryMaster() ||
            cell->GetCellLifeStage() != ETabletCellLifeStage::DecommissioningOnMaster ||
            !IsCellDecommissionedAtAllMasters(cell))
        {
            return;
        }

        TReqOnTabletCellDecommisionedOnMaster request;
        ToProto(request.mutable_cell_id(), cell->GetId());

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
    }

    void RemoveCellIfDecommissioned(const TCellBase* cell)
    {
        if (!cell->IsDecommissionCompleted() ||
            !IsCellDecommissionedAtAllMasters(cell))
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
        for (const auto [actionId, action] : tabletManager->TabletActions()) {
            if (IsObjectAlive(action) &&
                action->GetState() == ETabletActionState::Orphaned &&
                action->GetTabletCellBundle()->Health() == ETabletCellHealth::Good)
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

TTabletCellDecommissioner::TTabletCellDecommissioner(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
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

