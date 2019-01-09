#include "config.h"
#include "private.h"
#include "public.h"
#include "tablet_balancer.h"
#include "tablet_manager.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config_manager.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/world_initializer.h>
#include <yt/server/cell_master/config.h>

#include <yt/server/tablet_server/tablet_manager.pb.h>

#include <yt/core/misc/arithmetic_formula.h>
#include <yt/core/misc/numeric_helpers.h>

#include <yt/core/profiling/profiler.h>

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

static const auto& Logger = TabletServerLogger;

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

constexpr static TDuration MinBalanceFrequency = TDuration::Minutes(1);

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletBalancerMasterConfigPtr config,
        NCellMaster::TBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
        , BalanceExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletBalancer),
            BIND(&TImpl::Balance, MakeWeak(this)),
            Config_->BalancePeriod))
        , ConfigCheckExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
            BIND(&TImpl::OnCheckConfig, MakeWeak(this)),
            Config_->ConfigCheckPeriod))
        , Profiler("/tablet_server/tablet_balancer")
        , QueueSizeCounter_("/queue_size")
        , LastBalancingTime_(TruncatedNow())
    { }

    void Start()
    {
        OnCheckConfig();
        BalanceExecutor_->Start();
        ConfigCheckExecutor_->Start();
        Started_ = true;
    }

    void Stop()
    {
        ConfigCheckExecutor_->Stop();
        BalanceExecutor_->Stop();
        Started_ = false;
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

        auto size = GetTabletSize(tablet);
        auto bounds = GetTabletSizeConfig(tablet);
        if (size < bounds.MinTabletSize || size > bounds.MaxTabletSize) {
            auto bundleId = tablet->GetTable()->GetTabletCellBundle()->GetId();
            TabletIdQueue_[bundleId].push_back(tablet->GetId());
            QueuedTabletIds_.insert(tablet->GetId());
            Profiler.Increment(QueueSizeCounter_);
            YT_LOG_DEBUG("Tablet is put into balancer queue (TableId: %v, TabletId: %v)",
                tablet->GetTable()->GetId(),
                tablet->GetId());
        }
    }

    std::vector<TTabletActionId> SyncBalanceCells(
        TTabletCellBundle* bundle,
        const std::optional<std::vector<TTableNode*>>& tables)
    {
        if (bundle->GetActiveTabletActionCount() > 0) {
            return {};
        }

        SpawnedTabletActionIds_.clear();
        std::optional<THashSet<const TTableNode*>> tablesSet;
        if (tables) {
            tablesSet = THashSet<const TTableNode*>(tables->begin(), tables->end());
        }
        ReassignInMemoryTablets(bundle, tablesSet, true);

        // TODO(ifsmirnov): turn it on someday.
        // ReassignExtMemoryTablets(bundle, tablesSet, true);

        return std::move(SpawnedTabletActionIds_);
    }

    std::vector<TTabletActionId> SyncBalanceTablets(TTableNode* table)
    {
        SpawnedTabletActionIds_.clear();
        BalanceTablets(table);
        return std::move(SpawnedTabletActionIds_);
    }

private:
    struct TTabletSizeConfig
    {
        i64 MinTabletSize;
        i64 MaxTabletSize;
        i64 DesiredTabletSize;
    };

    const TTabletBalancerMasterConfigPtr Config_;
    const NCellMaster::TBootstrap* Bootstrap_;
    const NConcurrency::TPeriodicExecutorPtr BalanceExecutor_;
    const NConcurrency::TPeriodicExecutorPtr ConfigCheckExecutor_;

    bool Enabled_ = false;
    bool Started_ = false;

    THashMap<TTabletCellBundleId, std::deque<TTabletId>> TabletIdQueue_;

    THashSet<TTabletId> QueuedTabletIds_;
    THashSet<const TTablet*> TouchedTablets_;
    THashSet<const TTableNode*> TablesWithActiveActions_;
    THashSet<TTabletCellBundleId> BundlesPendingCellBalancing_;
    TTimeFormula FallbackBalancingSchedule_;
    THashMap<TTablet*, const TTabletCell*> TabletToTargetCellMap_;
    std::vector<TTabletActionId> SpawnedTabletActionIds_;

    const NProfiling::TProfiler Profiler;
    NProfiling::TSimpleGauge QueueSizeCounter_;

    TInstant LastBalancingTime_;
    TInstant CurrentTime_;

    bool IsTabletReshardable(const TTablet* tablet, bool ignoreConfig = false)
    {
        return tablet &&
            IsObjectAlive(tablet) &&
            (tablet->GetState() == ETabletState::Mounted || tablet->GetState() == ETabletState::Frozen) &&
            (!tablet->GetAction() || tablet->GetAction()->IsFinished()) &&
            IsObjectAlive(tablet->GetTable()) &&
            (ignoreConfig || tablet->GetTable()->TabletBalancerConfig()->EnableAutoReshard) &&
            tablet->GetTable()->IsPhysicallySorted() &&
            IsObjectAlive(tablet->GetCell()) &&
            IsObjectAlive(tablet->GetCell()->GetCellBundle()) &&
            (ignoreConfig || tablet->GetCell()->GetCellBundle()->TabletBalancerConfig()->EnableTabletSizeBalancer) &&
            tablet->Replicas().empty() &&
            IsTabletUntouched(tablet);
    }

    const TTimeFormula& GetBundleSchedule(const TTabletCellBundle* bundle)
    {
        const auto& local = bundle->TabletBalancerConfig()->TabletBalancerSchedule;
        if (!local.IsEmpty()) {
            YT_LOG_DEBUG("Using local balancer schedule for bundle (BundleName: %v, ScheduleFormula: %Qv)",
                bundle->GetName(),
                local.GetFormula());
            return local;
        }
        YT_LOG_DEBUG("Using global balancer schedule for bundle (BundleName: %v, ScheduleFormula: %Qv)",
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

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        THashSet<TTabletCellBundleId> bundlesForCellBalancingOnNextIteration;
        for (const auto& bundleAndId : tabletManager->TabletCellBundles()) {
            const auto& bundleId = bundleAndId.first;
            const auto* bundle = bundleAndId.second;

            // If it is necessary and possible to balance cells, do it...
            if (BundlesPendingCellBalancing_.contains(bundleId) && bundle->GetActiveTabletActionCount() == 0) {
                forMove.push_back(bundle);
            // ... else if time has come for reshard, do it.
            } else if (DidBundleBalancingTimeHappen(bundle)) {
                forReshard.push_back(bundle);
                bundlesForCellBalancingOnNextIteration.insert(bundleId);
            }

            // If it was nesessary but not possible to balance cells, postpone balancing to the next iteration and log it.
            if (BundlesPendingCellBalancing_.contains(bundleId) && bundle->GetActiveTabletActionCount() > 0) {
                YT_LOG_DEBUG(
                    "Tablet balancer did not balance cells because bundle participates in action (Bundle: %v)",
                    bundle->GetName());
                bundlesForCellBalancingOnNextIteration.insert(bundleId);
            }
        }

        BundlesPendingCellBalancing_ = std::move(bundlesForCellBalancingOnNextIteration);
        TouchedTablets_.clear();
        PurgeDeletedBundles();

        size_t totalSize = 0;
        for (const auto& bundleQueue : TabletIdQueue_) {
            totalSize += bundleQueue.second.size();
        }
        Profiler.Update(QueueSizeCounter_, totalSize);

        LastBalancingTime_ = CurrentTime_;

        PROFILE_TIMING("/balance_tablets") {
            for (auto* bundle : forReshard) {
                YT_LOG_DEBUG("Balancing tablets for bundle (Bundle: %v)",
                    bundle->GetName());
                BalanceTablets(bundle);
            }
        }

        PROFILE_TIMING("/balance_cells_in_memory") {
            for (auto* bundle : forMove) {
                YT_LOG_DEBUG("Balancing in memory cells for bundle (Bundle: %v)",
                    bundle->GetName());
                ReassignInMemoryTablets(bundle, std::nullopt, false);
            }
        }

        PROFILE_TIMING("/balance_cells_ext_memory") {
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
        for (const auto& pair : tabletManager->TabletActions()) {
            const auto* action = pair.second;

            if (!action->IsFinished()) {
                for (const auto* tablet : action->Tablets()) {
                    TablesWithActiveActions_.insert(tablet->GetTable());
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
        for (const auto* cell : bundle->TabletCells()) {
            if (IsObjectAlive(cell) && !cell->DecommissionStarted() && cell->GetCellBundle() == bundle) {
                cells.push_back(cell);
            }
        }
        return cells;
    }

    TGuid GenerateCorrelationId(bool sync)
    {
        if (sync) {
            auto* mutationContext = NHydra::GetCurrentMutationContext();
            auto& gen = mutationContext->RandomGenerator();
            ui64 lo = gen.Generate<ui64>();
            ui64 hi = gen.Generate<ui64>();
            return TGuid(lo, hi);
        } else {
            return TGuid::Create();
        }
    }

    void CreateMoveAction(TTablet* tablet, TTabletCellId targetCellId, bool sync)
    {
        auto* table = tablet->GetTable();
        auto* srcCell = tablet->GetCell();

        auto correlationId = GenerateCorrelationId(sync);
        YT_LOG_DEBUG("Tablet balancer would like to move tablet "
            "(TableId: %v, InMemoryMode: %v, TabletId: %v, SrcCellId: %v, DstCellId: %v, "
            "Bundle: %v, TabletBalancerCorrelationId: %v)",
            table->GetId(),
            table->GetInMemoryMode(),
            tablet->GetId(),
            srcCell->GetId(),
            targetCellId,
            table->GetTabletCellBundle()->GetName(),
            correlationId);

        if (sync) {
            try {
                const auto& tabletManager = Bootstrap_->GetTabletManager();
                auto* dstCell = tabletManager->GetTabletCell(targetCellId);
                auto* action = tabletManager->CreateTabletAction(
                    TObjectId{},
                    ETabletActionKind::Move,
                    {tablet},
                    {dstCell},
                    {}, // pivotKeys
                    std::nullopt, // tabletCount
                    false, // skipFreezing
                    correlationId,
                    TInstant::Zero());
                SpawnedTabletActionIds_.push_back(action->GetId());
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Tablet balancer failed to create tablet action (Kind: %v, TabletBalancerCorrelationId: %v",
                    ETabletActionKind::Move,
                    correlationId);
            }
        } else {
            TReqCreateTabletAction request;
            request.set_kind(static_cast<int>(ETabletActionKind::Move));
            ToProto(request.mutable_tablet_ids(), std::vector<TTabletId>{tablet->GetId()});
            ToProto(request.mutable_cell_ids(), std::vector<TTabletCellId>{targetCellId});
            ToProto(request.mutable_correlation_id(), correlationId);

            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger);
        }
    }

    void CreateReshardAction(const std::vector<TTablet*>& tablets, int newTabletCount, i64 size, bool sync)
    {
        YCHECK(!tablets.empty());

        std::vector<TTabletId> tabletIds;
        for (const auto& tablet : tablets) {
            tabletIds.emplace_back(tablet->GetId());
        }

        auto* table = tablets[0]->GetTable();
        auto correlationId = GenerateCorrelationId(sync);
        YT_LOG_DEBUG("Tablet balancer would like to reshard tablets "
            "(TableId: %v, TabletIds: %v, NewTabletCount: %v, TotalSize: %v, Bundle: %v, TabletBalancerCorrelationId: %v)",
            table->GetId(),
            tabletIds,
            newTabletCount,
            size,
            table->GetTabletCellBundle()->GetName(),
            correlationId);

        if (sync) {
            try {
                const auto& tabletManager = Bootstrap_->GetTabletManager();
                auto* action = tabletManager->CreateTabletAction(
                    TObjectId{},
                    ETabletActionKind::Reshard,
                    tablets,
                    {}, // cells
                    {}, // pivotKeys
                    newTabletCount,
                    false, // skipFreezing
                    correlationId,
                    TInstant::Zero());
                SpawnedTabletActionIds_.push_back(action->GetId());
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Tablet balancer failed to create tablet action (Kind: %v, TabletBalancerCorrelationId: %v",
                    ETabletActionKind::Reshard,
                    correlationId);
            }
        } else {
            TReqCreateTabletAction request;
            request.set_kind(static_cast<int>(ETabletActionKind::Reshard));
            ToProto(request.mutable_tablet_ids(), tabletIds);
            request.set_tablet_count(newTabletCount);
            ToProto(request.mutable_correlation_id(), correlationId);

            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger);
        }
    }

    int ApplyMoveActions(bool sync)
    {
        int actionCount = 0;
        for (const auto& pair : TabletToTargetCellMap_) {
            if (pair.first->GetCell() != pair.second) {
                CreateMoveAction(pair.first, pair.second->GetId(), sync);
                ++actionCount;
            }
        }

        TabletToTargetCellMap_.clear();
        return actionCount;
    }

    void ReassignInMemoryTablets(
        const TTabletCellBundle* bundle,
        const std::optional<THashSet<const TTableNode*>>& movableTables,
        bool sync)
    {
        const auto& config = bundle->TabletBalancerConfig();
        if (!sync && !config->EnableInMemoryCellBalancer) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        auto softThresholdViolated = [&config] (i64 min, i64 max) {
            return max > 0 && 1.0 * (max - min) / max > config->SoftInMemoryCellBalanceThreshold;
        };

        auto hardThresholdViolated = [&config] (i64 min, i64 max) {
            return max > 0 && 1.0 * (max - min) / max > config->HardInMemoryCellBalanceThreshold;
        };

        auto cells = GetAliveCells(bundle);

        if (cells.empty()) {
            return;
        }

        struct TMemoryUsage {
            i64 Memory;
            const TTabletCell* TabletCell;

            bool operator<(const TMemoryUsage& other) const
            {
                if (Memory != other.Memory) {
                    return Memory < other.Memory;
                }
                return TabletCell->GetId() < other.TabletCell->GetId();
            }
            bool operator>(const TMemoryUsage& other) const
            {
                return other < *this;
            }
        };

        std::vector<TMemoryUsage> memoryUsage;
        i64 total = 0;
        memoryUsage.reserve(cells.size());
        for (const auto* cell : cells) {
            i64 size = cell->LocalStatistics().MemorySize;
            total += size;
            memoryUsage.push_back({size, cell});
        }

        auto minmaxCells = std::minmax_element(memoryUsage.begin(), memoryUsage.end());
        if (!hardThresholdViolated(minmaxCells.first->Memory, minmaxCells.second->Memory)) {
            return;
        }

        std::sort(memoryUsage.begin(), memoryUsage.end());
        i64 mean = total / cells.size();
        std::priority_queue<TMemoryUsage, std::vector<TMemoryUsage>, std::greater<TMemoryUsage>> queue;

        for (const auto& cell : memoryUsage) {
            if (cell.Memory >= mean) {
                break;
            }
            queue.push(cell);
        }

        int actionCount = 0;
        for (int index = memoryUsage.size() - 1; index >= 0; --index) {
            auto cellSize = memoryUsage[index].Memory;
            auto* cell = memoryUsage[index].TabletCell;

            std::vector<TTablet*> tablets(cell->Tablets().begin(), cell->Tablets().end());
            std::sort(tablets.begin(), tablets.end(), [] (TTablet* lhs, TTablet* rhs) {
                return lhs->GetId() < rhs->GetId();
            });

            for (auto* tablet : tablets) {
                if (tablet->GetInMemoryMode() == EInMemoryMode::None) {
                    continue;
                }

                if (!sync && !tablet->GetTable()->TabletBalancerConfig()->EnableAutoTabletMove) {
                    continue;
                }

                if (movableTables && !movableTables->contains(tablet->GetTable())) {
                    continue;
                }

                if (queue.empty() || cellSize <= mean) {
                    break;
                }

                auto top = queue.top();

                if (!softThresholdViolated(top.Memory, cellSize)) {
                    break;
                }

                auto statistics = tabletManager->GetTabletStatistics(tablet);
                auto tabletSize = statistics.MemorySize;

                if (tabletSize == 0) {
                    continue;
                }

                if (tabletSize < cellSize - top.Memory) {
                    queue.pop();
                    top.Memory += tabletSize;
                    cellSize -= tabletSize;
                    if (top.Memory < mean) {
                        queue.push(top);
                    }

                    CreateMoveAction(tablet, top.TabletCell->GetId(), sync);

                    ++actionCount;
                }
            }
        }

        if (!sync) {
            Profiler.Enqueue(
                "/in_memory_moves",
                actionCount,
                NProfiling::EMetricType::Gauge,
                {bundle->GetProfilingTag()});
        }
    }

    void ReassignExtMemoryTabletsOfTable(
        const std::vector<TTablet*>& tablets,
        const std::vector<const TTabletCell*>& bundleCells,
        THashMap<const TTabletCell*, std::vector<TTablet*>>* slackTablets)
    {
        THashMap<const TTabletCell*, std::vector<TTablet*>> tabletsByCell;
        for (auto* tablet : tablets) {
            tabletsByCell[tablet->GetCell()].push_back(tablet);
        }

        std::vector<std::pair<int, const TTabletCell*>> cells;
        for (const auto& pair : tabletsByCell) {
            cells.emplace_back(pair.second.size(), pair.first);
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
            if (!tabletsByCell.contains(cell)) {
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
            auto& srcTablets = tabletsByCell[cells[srcIndex].second];
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
            YCHECK(moveCount == limit);
        };

        YCHECK(!cells.empty());
        int dstIndex = cells.size() - 1;

        for (int srcIndex = 0; srcIndex < dstIndex; ++srcIndex) {
            int srcLimit = cells[srcIndex].first - getExpectedTabletCount(srcIndex);
            while (srcLimit > 0 && srcIndex < dstIndex) {
                int dstLimit = getExpectedTabletCount(dstIndex) - cells[dstIndex].first;
                int moveCount = std::min(srcLimit, dstLimit);
                YCHECK(moveCount >= 0);
                moveTablets(srcIndex, dstIndex, moveCount);
                if (moveCount == dstLimit) {
                    --dstIndex;
                }
                srcLimit -= moveCount;
            }
        }

        if (slackTablets) {
            for (const auto& pair : cells) {
                const auto* cell = pair.second;
                auto& tablets = tabletsByCell[cell];
                if (tablets.size() > minCellSize) {
                    slackTablets->at(cell).push_back(tabletsByCell[cell].back());
                } else {
                    break;
                }
            }
        }

        for (int cellIndex = 0; cellIndex < cells.size(); ++cellIndex) {
            Y_ASSERT(cells[cellIndex].first == getExpectedTabletCount(cellIndex));
        }
    }

    void ReassignSlackTablets(std::vector<std::pair<const TTabletCell*, std::vector<TTablet*>>> cellTablets)
    {
        std::sort(
            cellTablets.begin(),
            cellTablets.end(),
            [](const auto& lhs, const auto& rhs) {
                return lhs.second.size() > rhs.second.size();
            });

        std::vector<THashSet<const TTableNode*>> presentTables;
        std::vector<int> tabletCount;
        int totalTabletCount = 0;
        for (const auto& pair : cellTablets) {
            totalTabletCount += pair.second.size();
            tabletCount.push_back(pair.second.size());

            presentTables.emplace_back();
            for (auto* tablet : pair.second) {
                YCHECK(presentTables.back().insert(tablet->GetTable()).second);
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
            YCHECK(moveCount == limit);
        };

        YCHECK(!cellTablets.empty());
        int dstIndex = cellTablets.size() - 1;

        for (int srcIndex = 0; srcIndex < dstIndex; ++srcIndex) {
            int srcLimit = tabletCount[srcIndex] - getExpectedTabletCount(srcIndex);
            while (srcLimit > 0 && srcIndex < dstIndex) {
                int dstLimit = getExpectedTabletCount(dstIndex) - tabletCount[dstIndex];
                int moveCount = std::min(srcLimit, dstLimit);
                YCHECK(moveCount >= 0);
                moveTablets(srcIndex, dstIndex, moveCount);
                if (moveCount == dstLimit) {
                    --dstIndex;
                }
                srcLimit -= moveCount;
            }
        }

        for (int cellIndex = 0; cellIndex < cellTablets.size(); ++cellIndex) {
            Y_ASSERT(tabletCount[cellIndex] == getExpectedTabletCount(cellIndex));
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
                const auto* table = tablet->GetTable();

                // TODO(ifsmirnov): add '!sync && ' here.
                if (!table->TabletBalancerConfig()->EnableAutoTabletMove) {
                    continue;
                }

                if (movableTables && !movableTables->contains(table)) {
                    continue;
                }

                if (table->GetInMemoryMode() == EInMemoryMode::None) {
                    tabletsByTable[table].push_back(tablet);
                }
            }
        }

        THashMap<const TTabletCell*, std::vector<TTablet*>> slackTablets;
        for (const auto* cell : bundleCells) {
            slackTablets[cell] = {};
        }
        for (const auto& pair : tabletsByTable) {
            ReassignExtMemoryTabletsOfTable(
                pair.second,
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

        int actionCount = ApplyMoveActions(false /*sync*/);

        Profiler.Enqueue(
            "/ext_memory_moves",
            actionCount,
            NProfiling::EMetricType::Gauge,
            {bundle->GetProfilingTag()});
    }

    bool BalanceTablet(TTablet* tablet, bool sync)
    {
        if (!IsTabletReshardable(tablet, sync) ||
            TablesWithActiveActions_.contains(tablet->GetTable()))
        {
            return false;
        }

        auto size = GetTabletSize(tablet);
        auto bounds = GetTabletSizeConfig(tablet);
        if (size < bounds.MinTabletSize || size > bounds.MaxTabletSize) {
            if (MergeSplitTablet(tablet, bounds, sync)) {
                return true;
            }
        }
        return false;
    }

    void BalanceTablets(const TTableNode* table)
    {
        TouchedTablets_.clear();
        for (const auto& tablet : table->Tablets()) {
            if (tablet->GetAction()) {
                return;
            }
        }
        for (const auto& tablet : table->Tablets()) {
            BalanceTablet(tablet, true);
        }
    }

    void BalanceTablets(const TTabletCellBundle* bundle)
    {
        if (!TabletIdQueue_.contains(bundle->GetId())) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        int actionCount = 0;

        auto it = TabletIdQueue_.find(bundle->GetId());
        if (it == TabletIdQueue_.end()) {
            return;
        }
        auto& queue = it->second;
        while (!queue.empty()) {
            auto tabletId = queue.front();
            queue.pop_front();
            QueuedTabletIds_.erase(tabletId);

            auto* tablet = tabletManager->FindTablet(tabletId);
            actionCount += BalanceTablet(tablet, false /*sync*/);
        }

        Profiler.Enqueue(
            "/tablet_merges",
            actionCount,
            NProfiling::EMetricType::Gauge,
            {bundle->GetProfilingTag()});
    }

    i64 GetTabletSize(TTablet* tablet)
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto statistics = tabletManager->GetTabletStatistics(tablet);
        return tablet->GetInMemoryMode() == EInMemoryMode::None
            ? statistics.UncompressedDataSize
            : statistics.MemorySize;
    }

    bool IsTabletUntouched(const TTablet* tablet)
    {
        return TouchedTablets_.find(tablet) == TouchedTablets_.end();
    }

    bool MergeSplitTablet(TTablet* tablet, const TTabletSizeConfig& bounds, bool sync)
    {
        auto* table = tablet->GetTable();

        i64 desiredSize = bounds.DesiredTabletSize;
        i64 size = GetTabletSize(tablet);

        if (size < bounds.MinTabletSize && table->Tablets().size() == 1) {
            return false;
        }

        if (desiredSize == 0) {
            desiredSize = 1;
        }

        int startIndex = tablet->GetIndex();
        int endIndex = tablet->GetIndex();

        auto sizeGood = [&] () {
            int tabletCount = std::clamp<i64>(DivRound(size, desiredSize), 1, MaxTabletCount);
            i64 tabletSize = size / tabletCount;
            return tabletSize >= bounds.MinTabletSize && tabletSize <= bounds.MaxTabletSize;
        };

        while (!sizeGood() &&
            startIndex > 0 &&
            IsTabletUntouched(table->Tablets()[startIndex - 1]) &&
            table->Tablets()[startIndex - 1]->GetState() == tablet->GetState())
        {
            --startIndex;
            size += GetTabletSize(table->Tablets()[startIndex]);
        }
        while (!sizeGood() &&
            endIndex < table->Tablets().size() - 1 &&
            IsTabletUntouched(table->Tablets()[endIndex + 1]) &&
            table->Tablets()[endIndex + 1]->GetState() == tablet->GetState())
        {
            ++endIndex;
            size += GetTabletSize(table->Tablets()[endIndex]);
        }

        int newTabletCount = std::clamp<i64>(DivRound(size, desiredSize), 1, MaxTabletCount);

        if (endIndex == startIndex && tablet->NodeStatistics().partition_count() == 1) {
            return false;
        }


        if (newTabletCount == endIndex - startIndex + 1 && newTabletCount == 1) {
            YT_LOG_DEBUG("Tablet balancer is unable to reshard tablet (TableId: %v, TabletId: %v, TabletSize: %v)",
                table->GetId(),
                tablet->GetId(),
                size);
            return false;
        }

        std::vector<TTablet*> tablets;
        for (int index = startIndex; index <= endIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            tablets.push_back(tablet);
            TouchedTablets_.insert(tablet);
        }

        CreateReshardAction(tablets, newTabletCount, size, sync);

        return true;
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

    TTabletSizeConfig GetTabletSizeConfig(TTablet* tablet)
    {
        i64 minTabletSize;
        i64 maxTabletSize;
        i64 desiredTabletSize = 0;

        const auto& config = tablet->GetCell()->GetCellBundle()->TabletBalancerConfig();
        auto* table = tablet->GetTable();
        const auto& desiredTabletCount = table->GetDesiredTabletCount();
        auto statistics = table->ComputeTotalStatistics();
        i64 tableSize = tablet->GetInMemoryMode() == EInMemoryMode::Compressed
            ? statistics.compressed_data_size()
            : statistics.uncompressed_data_size();
        i64 cellCount = tablet->GetCell()->GetCellBundle()->TabletCells().size() *
            config->TabletToCellRatio;

        if (!desiredTabletCount) {
            minTabletSize = tablet->GetInMemoryMode() == EInMemoryMode::None
                ? config->MinTabletSize
                : config->MinInMemoryTabletSize;
            maxTabletSize = tablet->GetInMemoryMode() == EInMemoryMode::None
                ? config->MaxTabletSize
                : config->MaxInMemoryTabletSize;
            desiredTabletSize = tablet->GetInMemoryMode() == EInMemoryMode::None
                ? config->DesiredTabletSize
                : config->DesiredInMemoryTabletSize;

            auto tableMinTabletSize = table->GetMinTabletSize();
            auto tableMaxTabletSize = table->GetMaxTabletSize();
            auto tableDesiredTabletSize = table->GetDesiredTabletSize();

            if (tableMinTabletSize && tableMaxTabletSize && tableDesiredTabletSize &&
                *tableMinTabletSize < *tableDesiredTabletSize &&
                *tableDesiredTabletSize < *tableMaxTabletSize)
            {
                minTabletSize = *tableMinTabletSize;
                maxTabletSize = *tableMaxTabletSize;
                desiredTabletSize = *tableDesiredTabletSize;
            }
        } else {
            cellCount = *desiredTabletCount;
        }

        if (cellCount == 0) {
            cellCount = 1;
        }

        auto tabletSize = DivCeil(tableSize, cellCount);
        if (desiredTabletSize < tabletSize) {
            desiredTabletSize = tabletSize;
            minTabletSize = static_cast<i64>(desiredTabletSize / 1.9);
            maxTabletSize = static_cast<i64>(desiredTabletSize * 1.9);
        }

        return TTabletSizeConfig{minTabletSize, maxTabletSize, desiredTabletSize};
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
                YT_LOG_INFO("Tablet balancer is disabled, see master config");
            }
            Enabled_ = false;
            return;
        }

        const auto& balancerConfig = Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->TabletBalancer;

        if (!balancerConfig->EnableTabletBalancer) {
            if (Enabled_) {
                YT_LOG_INFO("Tablet balancer is disabled, see //sys/@config");
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

TTabletBalancer::TTabletBalancer(
    TTabletBalancerMasterConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
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

void TTabletBalancer::OnTabletHeartbeat(TTablet* tablet)
{
    Impl_->OnTabletHeartbeat(tablet);
}

std::vector<TTabletActionId> TTabletBalancer::SyncBalanceCells(
    TTabletCellBundle* bundle,
    const std::optional<std::vector<NTableServer::TTableNode*>>& tables)
{
    return Impl_->SyncBalanceCells(bundle, tables);
}

std::vector<TTabletActionId> TTabletBalancer::SyncBalanceTablets(NTableServer::TTableNode* table)
{
    return Impl_->SyncBalanceTablets(table);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

