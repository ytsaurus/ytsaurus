#include "config.h"
#include "private.h"
#include "public.h"
#include "tablet_cell_decommissioner.h"
#include "tablet_manager.h"
#include "tablet_cell.h"
#include "tablet_action.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>
#include <yt/yt/server/master/cell_server/cell_base.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTabletServer {

using namespace NCellServer;
using namespace NConcurrency;
using namespace NObjectServer;
using namespace NSecurityClient;
using namespace NTabletClient;
using namespace NTabletServer::NProto;
using namespace NYTree;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

const auto static& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellDecommissioner::TImpl
    : public TRefCounted
{
public:
    TImpl(
        NCellMaster::TBootstrap* bootstrap,
        TTabletCellDecommissionerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Profiler(TabletServerProfiler().WithPrefix("/tablet_cell_decommissioner"))
        , DecommissionExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletDecommissioner),
            BIND(&TImpl::CheckDecommission, MakeWeak(this))))
        , KickOrphansExecutor_ (New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletDecommissioner),
            BIND(&TImpl::CheckOrphans, MakeWeak(this))))
        , DecommissionThrottler_(CreateNamedReconfigurableThroughputThrottler(
            config->DecommissionThrottler,
            "Decommission",
            TabletServerLogger(),
            Profiler.WithPrefix("/decommission_throttler")))
        , KickOrphansThrottler_(CreateNamedReconfigurableThroughputThrottler(
            config->KickOrphansThrottler,
            "KickOrphans",
            TabletServerLogger(),
            Profiler.WithPrefix("/kick_orphans_throttler")))
        , CheckDecommissionTimer_(Profiler.Timer("/check_decommission"))
        , CheckOrphansTimer_(Profiler.Timer("/check_orphans"))
        , Config_(std::move(config))
    { }

    void Start() const
    {
        DoReconfigure();
        DecommissionExecutor_->Start();
        KickOrphansExecutor_->Start();
    }

    void Stop() const
    {
        YT_UNUSED_FUTURE(DecommissionExecutor_->Stop());
        YT_UNUSED_FUTURE(KickOrphansExecutor_->Stop());
    }

    void Reconfigure(TTabletCellDecommissionerConfigPtr config)
    {
        Config_ = std::move(config);
        DoReconfigure();
    }

private:
    const NCellMaster::TBootstrap* Bootstrap_;
    const NProfiling::TProfiler Profiler;
    const TPeriodicExecutorPtr DecommissionExecutor_;
    const TPeriodicExecutorPtr KickOrphansExecutor_;
    const IReconfigurableThroughputThrottlerPtr DecommissionThrottler_;
    const IReconfigurableThroughputThrottlerPtr KickOrphansThrottler_;
    const NProfiling::TEventTimer CheckDecommissionTimer_;
    const NProfiling::TEventTimer CheckOrphansTimer_;

    TTabletCellDecommissionerConfigPtr Config_;


    void DoReconfigure() const
    {
        DecommissionExecutor_->SetPeriod(Config_->DecommissionCheckPeriod);
        KickOrphansExecutor_->SetPeriod(Config_->OrphansCheckPeriod);

        DecommissionThrottler_->Reconfigure(Config_->DecommissionThrottler);
        KickOrphansThrottler_->Reconfigure(Config_->KickOrphansThrottler);
    }

    void CheckDecommission() const
    {
        NProfiling::TEventTimerGuard guard(CheckDecommissionTimer_);

        DoCheckDecommission();
    }

    void CheckOrphans() const
    {
        NProfiling::TEventTimerGuard guard(CheckOrphansTimer_);

        DoCheckOrphans();
    }

    void DoCheckDecommission() const
    {
        auto retiringCells = GetDecommissionedCellsUsedByActions();

        YT_LOG_DEBUG_IF(!retiringCells.empty(),
            "Tablet cell decommissioner observes decommissioned cells with tablets (CellCount: %v)",
            retiringCells.size());

        const auto& cellManager = Bootstrap_->GetTamedCellManager();

        if (Config_->EnableTabletCellDecommission) {
            for (auto [cellId, cell] : cellManager->Cells()) {
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
            for (auto [cellId, cell] : cellManager->Cells()) {
                if (IsObjectAlive(cell) && !retiringCells.contains(cell)) {
                    RemoveCellIfDecommissioned(cell);
                }
            }
        }
    }

    THashSet<const TCellBase*> GetDecommissionedCellsUsedByActions() const
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        THashSet<const TCellBase*> retiringCells;
        for (auto [actionId, action] : tabletManager->TabletActions()) {
            if (IsObjectAlive(action) &&
                action->GetState() != ETabletActionState::Completed &&
                action->GetState() != ETabletActionState::Failed)
            {
                for (auto tablet : action->Tablets()) {
                    if (tablet->GetState() != ETabletState::Unmounted) {
                        for (auto* cell : tablet->GetCells()) {
                            if (cell->IsDecommissionStarted()) {
                                retiringCells.insert(cell);
                            }
                        }
                    }
                }

                // When cell is decommissioned all tablet actions should be unlinked.
                for (auto cell : action->TabletCells()) {
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

    void MoveTabletsToOtherCells(const TTabletCell* cell) const
    {
        for (auto tablet : cell->Tablets()) {
            if (!DecommissionThrottler_->TryAcquire(1)) {
                return;
            }

            TReqCreateTabletAction request;
            request.set_kind(ToProto(ETabletActionKind::Move));
            ToProto(request.mutable_tablet_ids(), std::vector<TTabletId>{tablet->GetId()});

            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger()));
        }
    }

    bool IsCellDecommissionedAtAllMasters(const TCellBase* cell) const
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

    void RequestTabletCellDecommissonOnNode(const TCellBase* cell) const
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
        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger()));
    }

    void RemoveCellIfDecommissioned(const TCellBase* cell) const
    {
        if (!cell->IsDecommissionCompleted() ||
            !IsCellDecommissionedAtAllMasters(cell))
        {
            return;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto rootService = objectManager->GetRootService();
        auto req = TYPathProxy::Remove(NObjectClient::FromObjectId(cell->GetId()));
        YT_UNUSED_FUTURE(ExecuteVerb(rootService, req));
    }

    void DoCheckOrphans() const
    {
        std::vector<TTabletActionId> orphans;

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        for (auto [actionId, action] : tabletManager->TabletActions()) {
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
            YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger()));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TTabletCellDecommissioner::TTabletCellDecommissioner(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        bootstrap,
        New<TTabletCellDecommissionerConfig>()))
{ }

TTabletCellDecommissioner::~TTabletCellDecommissioner() = default;

void TTabletCellDecommissioner::Start() const
{
    Impl_->Start();
}

void TTabletCellDecommissioner::Stop() const
{
    Impl_->Stop();
}

void TTabletCellDecommissioner::Reconfigure(TTabletCellDecommissionerConfigPtr config) const
{
    Impl_->Reconfigure(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

