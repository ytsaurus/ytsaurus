#include "balancing_helpers.h"
#include "config.h"
#include "private.h"
#include "public.h"
#include "tablet_balancer.h"
#include "tablet_manager.h"
#include "tablet_cell.h"
#include "tablet.h"
#include "tablet_action.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/world_initializer.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/lib/tablet_balancer/config.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/core/misc/arithmetic_formula.h>
#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <util/random/shuffle.h>

#include <queue>

namespace NYT::NTabletServer {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NTableClient;
using namespace NTableServer;
using namespace NTabletClient;
using namespace NTabletServer::NProto;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

TInstant TruncateToMinutes(TInstant t)
{
    auto timeval = t.TimeVal();
    timeval.tv_usec = 0;
    timeval.tv_sec /= 60;
    timeval.tv_sec *= 60;
    return TInstant(timeval);
}

TInstant TruncatedNow()
{
    return TruncateToMinutes(Now());
}

static constexpr TDuration MinBalanceFrequency = TDuration::Minutes(1);

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancer::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , BalanceExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletBalancer),
            BIND(&TImpl::Balance, MakeWeak(this))))
        , ConfigCheckExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
            BIND(&TImpl::OnCheckConfig, MakeWeak(this))))
        , QueueSizeGauge_(TabletServerProfiler.Gauge("/tablet_balancer/queue_size"))
        , LastBalancingTime_(TruncatedNow())
    { }

    void Start()
    {
        DoReconfigure();
        OnCheckConfig();
        BalanceExecutor_->Start();
        ConfigCheckExecutor_->Start();
        Started_ = true;
    }

    void Stop()
    {
        YT_UNUSED_FUTURE(ConfigCheckExecutor_->Stop());
        YT_UNUSED_FUTURE(BalanceExecutor_->Stop());
        Started_ = false;
    }

    void Reconfigure(TTabletBalancerMasterConfigPtr config)
    {
        Config_ = std::move(config);
        DoReconfigure();
    }

    void OnTabletHeartbeat(TTablet* tablet)
    {
        if (!Enabled_ || !Started_) {
            return;
        }

        if (!IsTabletReshardable(tablet) ||
            QueuedTabletIds_.find(tablet->GetId()) != QueuedTabletIds_.end())
        {
            return;
        }

        auto size = GetTabletBalancingSize(tablet);
        auto bounds = GetTabletSizeConfig(tablet->GetTable());
        if (size < bounds.MinTabletSize || size > bounds.MaxTabletSize) {
            auto bundleId = tablet->GetTable()->TabletCellBundle()->GetId();
            TabletIdQueue_[bundleId].push_back(tablet->GetId());
            QueuedTabletIds_.insert(tablet->GetId());
            QueueSizeGauge_.Update(++TotalQueueSize_);
            YT_LOG_DEBUG("Tablet is put into balancer queue (TableId: %v, TabletId: %v)",
                tablet->GetTable()->GetId(),
                tablet->GetId());
        }
    }

private:
    const NCellMaster::TBootstrap* Bootstrap_;

    TTabletBalancerMasterConfigPtr Config_ = New<TTabletBalancerMasterConfig>();
    NConcurrency::TPeriodicExecutorPtr BalanceExecutor_;
    NConcurrency::TPeriodicExecutorPtr ConfigCheckExecutor_;

    bool Enabled_ = false;
    bool Started_ = false;

    THashMap<TTabletCellBundleId, std::vector<TTabletId>> TabletIdQueue_;

    THashSet<TTabletId> QueuedTabletIds_;
    TTabletBalancerContext Context_;
    THashSet<const TTableNode*> TablesWithActiveActions_;
    THashSet<TTabletCellBundleId> BundlesPendingCellBalancing_;
    TTimeFormula FallbackBalancingSchedule_;
    THashMap<TTablet*, const TTabletCell*> TabletToTargetCellMap_;
    std::vector<TTabletActionId> SpawnedTabletActionIds_;

    int TotalQueueSize_ = 0;
    NProfiling::TGauge QueueSizeGauge_;

    TInstant LastBalancingTime_;
    TInstant CurrentTime_;

    void DoReconfigure()
    {
        BalanceExecutor_->SetPeriod(Config_->BalancePeriod);
        ConfigCheckExecutor_->SetPeriod(Config_->ConfigCheckPeriod);
    }

    bool IsTabletReshardable(const TTablet* tablet) const
    {
        return NTabletServer::IsTabletReshardable(tablet, /*ignoreConfig*/ false) && IsTabletUntouched(tablet);
    }

    const TTimeFormula& GetBundleSchedule(const TTabletCellBundle* bundle)
    {
        const auto& local = bundle->TabletBalancerConfig()->TabletBalancerSchedule;
        if (!local.IsEmpty()) {
            YT_LOG_DEBUG("Using local balancer schedule for bundle (BundleName: %v, ScheduleFormula: %v)",
                bundle->GetName(),
                local.GetFormula());
            return local;
        }
        YT_LOG_DEBUG("Using global balancer schedule for bundle (BundleName: %v, ScheduleFormula: %v)",
            bundle->GetName(),
            FallbackBalancingSchedule_.GetFormula());
        return FallbackBalancingSchedule_;
    }

    void Balance()
    {
        if (!Enabled_) {
            return;
        }

        std::vector<const TTabletCellBundle*> forReshard;
        std::vector<const TTabletCellBundle*> forMove;

        FillActiveTabletActions();

        CurrentTime_ = TruncatedNow();

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        THashSet<TTabletCellBundleId> bundlesForCellBalancingOnNextIteration;
        for (auto* bundleBase : cellManager->CellBundles(NCellarClient::ECellarType::Tablet)) {
            YT_VERIFY(bundleBase->GetType() == EObjectType::TabletCellBundle);
            const auto* bundle = bundleBase->As<TTabletCellBundle>();

            if (bundle->TabletBalancerConfig()->EnableStandaloneTabletBalancer) {
                continue;
            }

            // If it is necessary and possible to balance cells, do it...
            if (BundlesPendingCellBalancing_.contains(bundle->GetId()) && bundle->GetActiveTabletActionCount() == 0) {
                forMove.push_back(bundle);
            // ... else if time has come for reshard, do it.
            } else if (DidBundleBalancingTimeHappen(bundle)) {
                forReshard.push_back(bundle);
                bundlesForCellBalancingOnNextIteration.insert(bundle->GetId());
            }

            // If it was nesessary but not possible to balance cells, postpone balancing to the next iteration and log it.
            if (BundlesPendingCellBalancing_.contains(bundle->GetId()) && bundle->GetActiveTabletActionCount() > 0) {
                YT_LOG_DEBUG(
                    "Tablet balancer did not balance cells because bundle participates in action (Bundle: %v)",
                    bundle->GetName());
                bundlesForCellBalancingOnNextIteration.insert(bundle->GetId());
            }
        }

        BundlesPendingCellBalancing_ = std::move(bundlesForCellBalancingOnNextIteration);
        Context_.TouchedTablets.clear();
        PurgeDeletedBundles();

        TotalQueueSize_ = 0;
        for (const auto& bundleQueue : TabletIdQueue_) {
            TotalQueueSize_ += bundleQueue.second.size();
        }
        QueueSizeGauge_.Update(TotalQueueSize_);

        LastBalancingTime_ = CurrentTime_;

        YT_PROFILE_TIMING("/tablet_server/tablet_balancer/balance_tablets") {
            for (auto* bundle : forReshard) {
                YT_LOG_DEBUG("Balancing tablets for bundle (Bundle: %v)",
                    bundle->GetName());
                BalanceTablets(bundle);
            }
        }

        YT_PROFILE_TIMING("/tablet_server/tablet_balancer/balance_cells_in_memory") {
            for (auto* bundle : forMove) {
                YT_LOG_DEBUG("Balancing in memory cells for bundle (Bundle: %v)",
                    bundle->GetName());
                ReassignInMemoryTablets(bundle);
            }
        }

        YT_PROFILE_TIMING("/tablet_server/tablet_balancer/balance_cells_ext_memory") {
            for (auto* bundle : forMove) {
                YT_LOG_DEBUG("Balancing ext memory cells for bundle (Bundle: %v)",
                    bundle->GetName());
                ReassignExtMemoryTablets(bundle, std::nullopt);
            }
        }
    }

    void FillActiveTabletActions()
    {
        TablesWithActiveActions_.clear();

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        for (auto [actionId, action] : tabletManager->TabletActions()) {
            if (!action->IsFinished()) {
                for (const auto* tablet : action->Tablets()) {
                    YT_VERIFY(tablet->GetType() == EObjectType::Tablet);
                    TablesWithActiveActions_.insert(tablet->As<TTablet>()->GetTable());
                    break;
                }
            }
        }
    }

    bool DidBundleBalancingTimeHappen(const TTabletCellBundle* bundle)
    {
        const auto& formula = GetBundleSchedule(bundle);

        try {
            if (Config_->BalancePeriod >= MinBalanceFrequency) {
                TInstant timePoint = LastBalancingTime_ + MinBalanceFrequency;
                if (timePoint > CurrentTime_) {
                    return false;
                }
                while (timePoint <= CurrentTime_) {
                    if (formula.IsSatisfiedBy(timePoint)) {
                        return true;
                    }
                    timePoint += MinBalanceFrequency;
                }
                return false;
            } else {
                return formula.IsSatisfiedBy(CurrentTime_);
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to evaluate tablet balancer schedule formula");
            return false;
        }
    }

    std::vector<const TTabletCell*> GetAliveCells(const TTabletCellBundle* bundle)
    {
        std::vector<const TTabletCell*> cells;
        for (const auto* cell : bundle->Cells()) {
            if (IsObjectAlive(cell) && !cell->IsDecommissionStarted() && cell->CellBundle().Get() == bundle) {
                YT_VERIFY(cell->GetType() == EObjectType::TabletCell);
                cells.push_back(cell->As<TTabletCell>());
            }
        }
        return cells;
    }

    void CreateMoveAction(TTablet* tablet, TTabletCellId targetCellId)
    {
        auto* table = tablet->GetTable();
        auto* srcCell = tablet->GetCell();

        auto correlationId = TGuid::Create();
        YT_LOG_DEBUG("Tablet balancer would like to move tablet "
            "(TableId: %v, InMemoryMode: %v, TabletId: %v, SrcCellId: %v, DstCellId: %v, "
            "Bundle: %v, TabletBalancerCorrelationId: %v)",
            table->GetId(),
            table->GetInMemoryMode(),
            tablet->GetId(),
            srcCell->GetId(),
            targetCellId,
            table->TabletCellBundle()->GetName(),
            correlationId);

        TReqCreateTabletAction request;
        request.set_kind(static_cast<int>(ETabletActionKind::Move));
        ToProto(request.mutable_tablet_ids(), std::vector<TTabletId>{tablet->GetId()});
        ToProto(request.mutable_cell_ids(), std::vector<TTabletCellId>{targetCellId});
        ToProto(request.mutable_correlation_id(), correlationId);

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger()));
    }

    void CreateReshardAction(const std::vector<TTablet*>& tablets, int newTabletCount, i64 size)
    {
        YT_VERIFY(!tablets.empty());

        std::vector<TTabletId> tabletIds;
        for (const auto& tablet : tablets) {
            tabletIds.emplace_back(tablet->GetId());
        }

        auto* table = tablets[0]->GetTable();
        auto correlationId = TGuid::Create();
        YT_LOG_DEBUG("Tablet balancer would like to reshard tablets "
            "(TableId: %v, TabletIds: %v, NewTabletCount: %v, TotalSize: %v, Bundle: %v, TabletBalancerCorrelationId: %v)",
            table->GetId(),
            tabletIds,
            newTabletCount,
            size,
            table->TabletCellBundle()->GetName(),
            correlationId);

        TReqCreateTabletAction request;
        request.set_kind(static_cast<int>(ETabletActionKind::Reshard));
        ToProto(request.mutable_tablet_ids(), tabletIds);
        request.set_tablet_count(newTabletCount);
        ToProto(request.mutable_correlation_id(), correlationId);

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger()));
    }

    int ApplyMoveActions()
    {
        int actionCount = 0;
        for (auto [tablet, cell] : TabletToTargetCellMap_) {
            if (tablet->GetCell() != cell) {
                CreateMoveAction(tablet, cell->GetId());
                ++actionCount;
            }
        }

        TabletToTargetCellMap_.clear();
        return actionCount;
    }

    void ReassignInMemoryTablets(const TTabletCellBundle* bundle)
    {
        if (!bundle->TabletBalancerConfig()->EnableInMemoryCellBalancer) {
            return;
        }

        auto descriptors = NTabletServer::ReassignInMemoryTablets(
            bundle,
            /*movableTablets*/ std::nullopt,
            /*ignoreTableWiseConfig*/ false);

        if (!descriptors.empty()) {
            for (auto descriptor : descriptors) {
                CreateMoveAction(descriptor.Tablet, descriptor.TabletCellId);
            }

            bundle->ProfilingCounters().InMemoryMoves.Increment(descriptors.size());
        }
    }

    void ReassignExtMemoryTabletsOfTable(
        const std::vector<TTablet*>& tablets,
        const std::vector<const TTabletCell*>& bundleCells,
        THashMap<const TTabletCell*, std::vector<TTablet*>>* slackTablets)
    {
        THashMap<const TTabletCell*, std::vector<TTablet*>> cellToTablets;
        for (auto* tablet : tablets) {
            cellToTablets[tablet->GetCell()].push_back(tablet);
        }

        std::vector<std::pair<int, const TTabletCell*>> cells;
        for (const auto& [cell, tablets] : cellToTablets) {
            cells.emplace_back(tablets.size(), cell);
        }

        // Cells with the same number of tablets of current table should be distributed
        // randomly each time. It gives better per-cell distribution on average.
        Shuffle(cells.begin(), cells.end());
        std::sort(cells.begin(), cells.end(), [] (auto lhs, auto rhs) {
            return lhs.first > rhs.first;
        });

        size_t expectedCellCount = std::min(tablets.size(), bundleCells.size());
        for (auto* cell : bundleCells) {
            if (cells.size() == expectedCellCount) {
                break;
            }
            if (!cellToTablets.contains(cell)) {
                cells.emplace_back(0, cell);
            }
        }

        auto getExpectedTabletCount = [&] (int cellIndex) {
            int cellCount = bundleCells.size();
            int tabletCount = tablets.size();
            return tabletCount / cellCount + (cellIndex < tabletCount % cellCount);
        };

        const int minCellSize = tablets.size() / bundleCells.size();

        auto moveTablets = [&] (int srcIndex, int dstIndex, int limit) {
            int moveCount = 0;
            auto& srcTablets = cellToTablets[cells[srcIndex].second];
            while (moveCount < limit && !srcTablets.empty()) {
                auto* tablet = srcTablets.back();
                srcTablets.pop_back();

                TabletToTargetCellMap_[tablet] = cells[dstIndex].second;
                ++moveCount;
                --cells[srcIndex].first;
                ++cells[dstIndex].first;

                if (slackTablets && cells[dstIndex].first > minCellSize) {
                    slackTablets->at(cells[dstIndex].second).push_back(tablet);
                }
            }
            YT_VERIFY(moveCount == limit);
        };

        YT_VERIFY(!cells.empty());
        int dstIndex = cells.size() - 1;

        for (int srcIndex = 0; srcIndex < dstIndex; ++srcIndex) {
            int srcLimit = cells[srcIndex].first - getExpectedTabletCount(srcIndex);
            while (srcLimit > 0 && srcIndex < dstIndex) {
                int dstLimit = getExpectedTabletCount(dstIndex) - cells[dstIndex].first;
                int moveCount = std::min(srcLimit, dstLimit);
                YT_VERIFY(moveCount >= 0);
                moveTablets(srcIndex, dstIndex, moveCount);
                if (moveCount == dstLimit) {
                    --dstIndex;
                }
                srcLimit -= moveCount;
            }
        }

        if (slackTablets) {
            for (auto [cellid, cell] : cells) {
                auto& tablets = cellToTablets[cell];
                if (std::ssize(tablets) > minCellSize) {
                    slackTablets->at(cell).push_back(cellToTablets[cell].back());
                } else {
                    break;
                }
            }
        }

        for (int cellIndex = 0; cellIndex < std::ssize(cells); ++cellIndex) {
            YT_ASSERT(cells[cellIndex].first == getExpectedTabletCount(cellIndex));
        }
    }

    void ReassignSlackTablets(std::vector<std::pair<const TTabletCell*, std::vector<TTablet*>>> cellTablets)
    {
        std::sort(
            cellTablets.begin(),
            cellTablets.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.second.size() > rhs.second.size();
            });

        std::vector<THashSet<const TTableNode*>> presentTables;
        std::vector<int> tabletCount;
        int totalTabletCount = 0;
        for (const auto& [cell, tablets] : cellTablets) {
            totalTabletCount += tablets.size();
            tabletCount.push_back(tablets.size());

            presentTables.emplace_back();
            for (auto* tablet : tablets) {
                YT_VERIFY(presentTables.back().insert(tablet->GetTable()).second);
            }
        }

        auto getExpectedTabletCount = [&] (int cellIndex) {
            int cellCount = cellTablets.size();
            return totalTabletCount / cellCount + (cellIndex < totalTabletCount % cellCount);
        };

        auto moveTablets = [&] (int srcIndex, int dstIndex, int limit) {
            int moveCount = 0;
            auto& srcTablets = cellTablets[srcIndex].second;
            for (size_t tabletIndex = 0; tabletIndex < srcTablets.size() && moveCount < limit; ++tabletIndex) {
                auto* tablet = srcTablets[tabletIndex];
                if (!presentTables[dstIndex].contains(tablet->GetTable())) {
                    presentTables[dstIndex].insert(tablet->GetTable());
                    TabletToTargetCellMap_[tablet] = cellTablets[dstIndex].first;
                    std::swap(srcTablets[tabletIndex], srcTablets.back());
                    srcTablets.pop_back();

                    ++moveCount;
                    --tabletIndex;
                    --tabletCount[srcIndex];
                    ++tabletCount[dstIndex];
                }
            }
            YT_VERIFY(moveCount == limit);
        };

        YT_VERIFY(!cellTablets.empty());
        int dstIndex = cellTablets.size() - 1;

        for (int srcIndex = 0; srcIndex < dstIndex; ++srcIndex) {
            int srcLimit = tabletCount[srcIndex] - getExpectedTabletCount(srcIndex);
            while (srcLimit > 0 && srcIndex < dstIndex) {
                int dstLimit = getExpectedTabletCount(dstIndex) - tabletCount[dstIndex];
                int moveCount = std::min(srcLimit, dstLimit);
                YT_VERIFY(moveCount >= 0);
                moveTablets(srcIndex, dstIndex, moveCount);
                if (moveCount == dstLimit) {
                    --dstIndex;
                }
                srcLimit -= moveCount;
            }
        }

        for (int cellIndex = 0; cellIndex < std::ssize(cellTablets); ++cellIndex) {
            YT_ASSERT(tabletCount[cellIndex] == getExpectedTabletCount(cellIndex));
        }
    }

    void ReassignExtMemoryTablets(
        const TTabletCellBundle* bundle,
        const std::optional<THashSet<const TTableNode*>>& movableTables)
    {
        /*
           Balancing happens in two iterations. First iteration goes per-table.
           Tablets of each table are spread between cells as evenly as possible.
           Due to rounding errors some cells will contain more tablets than
           the others. These extra tablets are called slack tablets. In the
           picture C are slack tablets, T are the others.

           | C  C       |
           | T  T  T  T |
           | T  T  T  T |
           +------------+

           Next iteration runs only if there are empty cells (usually when new
           cells are added). All slack tablets are spead between cells
           once again. Tablets are moved from cells with many tablets to cells
           with fewer. After balancing no two slack tablets from the same
           table may be on the same cell.
        */
        const auto& config = bundle->TabletBalancerConfig();
        if (!config->EnableCellBalancer) {
            return;
        }

        auto bundleCells = GetAliveCells(bundle);

        bool haveEmptyCells = false;

        THashMap<const TTableNode*, std::vector<TTablet*>> tabletsByTable;
        for (const auto* cell : bundleCells) {
            if (cell->Tablets().empty()) {
                haveEmptyCells = true;
            }
            for (auto* tablet : cell->Tablets()) {
                if (tablet->GetType() != EObjectType::Tablet) {
                    continue;
                }

                const auto* table = tablet->As<TTablet>()->GetTable();

                // TODO(ifsmirnov): add '!sync && ' here.
                if (!table->TabletBalancerConfig()->EnableAutoTabletMove) {
                    continue;
                }

                if (movableTables && !movableTables->contains(table)) {
                    continue;
                }

                if (table->GetInMemoryMode() == EInMemoryMode::None) {
                    tabletsByTable[table].push_back(tablet->As<TTablet>());
                }
            }
        }

        THashMap<const TTabletCell*, std::vector<TTablet*>> slackTablets;
        for (const auto* cell : bundleCells) {
            slackTablets[cell] = {};
        }
        for (const auto& [node, tablets] : tabletsByTable) {
            ReassignExtMemoryTabletsOfTable(
                tablets,
                bundleCells,
                haveEmptyCells ? &slackTablets : nullptr);
        }

        if (haveEmptyCells) {
            std::vector<std::pair<const TTabletCell*, std::vector<TTablet*>>> slackTabletsVector;
            for (auto&& pair : slackTablets) {
                slackTabletsVector.emplace_back(pair.first, std::move(pair.second));
            }
            ReassignSlackTablets(slackTabletsVector);
        }

        int actionCount = ApplyMoveActions();
        bundle->ProfilingCounters().ExtMemoryMoves.Increment(actionCount);
    }

    void BalanceTablets(const TTabletCellBundle* bundle)
    {
        if (!TabletIdQueue_.contains(bundle->GetId())) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        auto it = TabletIdQueue_.find(bundle->GetId());
        if (it == TabletIdQueue_.end()) {
            return;
        }

        std::vector<TTablet*> tablets;
        for (auto tabletId : it->second) {
            QueuedTabletIds_.erase(tabletId);
            auto* tabletBase = tabletManager->FindTablet(tabletId);
            if (!IsObjectAlive(tabletBase)) {
                continue;
            }
            YT_VERIFY(tabletBase->GetType() == EObjectType::Tablet);
            auto* tablet = tabletBase->As<TTablet>();
            if (IsTabletReshardable(tablet)) {
                tablets.push_back(tablet);
            }
        }

        it->second.clear();

        std::sort(
            tablets.begin(),
            tablets.end(),
            [&] (const TTablet* lhs, const TTablet* rhs) {
                return lhs->GetTable()->GetId() < rhs->GetTable()->GetId();
            });

        int actionCount = 0;

        auto beginIt = tablets.begin();
        while (beginIt != tablets.end()) {
            auto endIt = beginIt;
            while (endIt != tablets.end() && (*beginIt)->GetTable() == (*endIt)->GetTable()) {
                ++endIt;
            }
            auto tabletRange = MakeRange(beginIt, endIt);
            beginIt = endIt;

            const auto* table = tabletRange.Front()->GetTable();
            if (TablesWithActiveActions_.contains(table)) {
                continue;
            }

            auto descriptors = MergeSplitTabletsOfTable(
                tabletRange,
                &Context_);
            for (auto descriptor : descriptors) {
                CreateReshardAction(descriptor.Tablets, descriptor.TabletCount, descriptor.DataSize);
            }
            actionCount += descriptors.size();
        }

        bundle->ProfilingCounters().TabletMerges.Increment(actionCount);
    }

    bool IsTabletUntouched(const TTablet* tablet) const
    {
        return Context_.IsTabletUntouched(tablet);
    }

    void PurgeDeletedBundles()
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        std::vector<TTabletCellBundleId> toErase;
        for (const auto& it : TabletIdQueue_) {
            if (!tabletManager->FindTabletCellBundle(it.first)) {
                toErase.push_back(it.first);
            }
        }
        for (const auto& bundleId : toErase) {
            TabletIdQueue_.erase(bundleId);
        }
    }

    void OnCheckConfig()
    {
        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        if (!hydraFacade->GetHydraManager()->IsActiveLeader()) {
            return;
        }

        const auto& worldInitializer = Bootstrap_->GetWorldInitializer();
        if (!worldInitializer->IsInitialized()) {
            return;
        }

        if (!Config_->EnableTabletBalancer) {
            if (Enabled_) {
                YT_LOG_INFO("Tablet balancer disabled");
            }
            Enabled_ = false;
            return;
        }

        const auto& balancerConfig = Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->TabletBalancer;

        if (!balancerConfig->EnableTabletBalancer) {
            if (Enabled_) {
                YT_LOG_INFO("Tablet balancer disabled");
            }
            Enabled_ = false;
            return;
        }

        FallbackBalancingSchedule_ = balancerConfig->TabletBalancerSchedule;

        if (!Enabled_) {
            YT_LOG_INFO("Tablet balancer enabled");
        }
        Enabled_ = true;
    }
};

////////////////////////////////////////////////////////////////////////////////

TTabletBalancer::TTabletBalancer(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TTabletBalancer::~TTabletBalancer() = default;

void TTabletBalancer::Start()
{
    Impl_->Start();
}

void TTabletBalancer::Stop()
{
    Impl_->Stop();
}

void TTabletBalancer::Reconfigure(TTabletBalancerMasterConfigPtr config)
{
    Impl_->Reconfigure(std::move(config));
}

void TTabletBalancer::OnTabletHeartbeat(TTablet* tablet)
{
    Impl_->OnTabletHeartbeat(tablet);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
