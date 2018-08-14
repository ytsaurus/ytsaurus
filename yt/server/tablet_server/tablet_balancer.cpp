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

namespace NYT {
namespace NTabletServer {

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

// Formatter for TMemoryUsage.
template <class T, class U>
TString ToString(const std::pair<T, U>& pair)
{
    return Format("(%v, %v)", pair.first, pair.second);
}

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
            LOG_DEBUG("Tablet is put into balancer queue (TableId: %v, TabletId: %v)",
                tablet->GetTable()->GetId(),
                tablet->GetId());
        }
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
    THashSet<const TTabletCellBundle*> BundlesWithActiveActions_;
    THashSet<TTabletCellBundleId> BundlesPendingCellBalancing_;
    TTimeFormula FallbackBalancingSchedule_;
    THashMap<const TTablet*, const TTabletCell*> TabletToTargetCellMap_;

    const NProfiling::TProfiler Profiler;
    NProfiling::TSimpleGauge QueueSizeCounter_;

    TInstant LastBalancingTime_;
    TInstant CurrentTime_;

    bool IsTabletReshardable(const TTablet* tablet)
    {
        return tablet &&
            IsObjectAlive(tablet) &&
            !tablet->GetAction() &&
            IsObjectAlive(tablet->GetTable()) &&
            tablet->GetTable()->GetEnableTabletBalancer().Get(true) &&
            tablet->GetTable()->IsSorted() &&
            IsObjectAlive(tablet->GetCell()) &&
            IsObjectAlive(tablet->GetCell()->GetCellBundle()) &&
            tablet->GetCell()->GetCellBundle()->TabletBalancerConfig()->EnableTabletSizeBalancer &&
            tablet->Replicas().empty() &&
            IsTabletUntouched(tablet);
    }

    const TTimeFormula& GetBundleSchedule(const TTabletCellBundle* bundle)
    {
        const auto& local = bundle->TabletBalancerConfig()->TabletBalancerSchedule;
        if (!local.IsEmpty()) {
            LOG_DEBUG("Using local balancer schedule for bundle (BundleName: %v, ScheduleFormula: %Qv)",
                bundle->GetName(),
                local.GetFormula());
            return local;
        }
        LOG_DEBUG("Using global balancer schedule for bundle (BundleName: %v, ScheduleFormula: %Qv)",
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
            if (BundlesPendingCellBalancing_.has(bundleId) && !BundlesWithActiveActions_.has(bundle)) {
                LOG_DEBUG("Balancing cells for bundle (Bundle: %v)",
                    bundle->GetName());
                forMove.push_back(bundle);
            // ... else if time has come for reshard, do it.
            } else if (DidBundleBalancingTimeHappen(bundle)) {
                LOG_DEBUG("Balancing tablets for bundle (Bundle: %v)",
                    bundle->GetName());
                forReshard.push_back(bundle);
                bundlesForCellBalancingOnNextIteration.insert(bundleId);
            }

            // If it was nesessary but not possible to balance cells, postpone balancing to the next iteration and log it.
            if (BundlesPendingCellBalancing_.has(bundleId) && BundlesWithActiveActions_.has(bundle)) {
                LOG_DEBUG(
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
                BalanceTablets(bundle);
            }
        }

        PROFILE_TIMING("/balance_cells_in_memory") {
            for (auto* bundle : forMove) {
                ReassignInMemoryTablets(bundle);
            }
        }

        PROFILE_TIMING("/balance_cells_ext_memory") {
            for (auto* bundle : forMove) {
                ReassignExtMemoryTablets(bundle);
            }
        }
    }

    void FillActiveTabletActions()
    {
        TablesWithActiveActions_.clear();
        BundlesWithActiveActions_.clear();

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        for (const auto& pair : tabletManager->TabletActions()) {
            const auto* action = pair.second;

            if (action->GetState() != ETabletActionState::Completed &&
                action->GetState() != ETabletActionState::Failed)
            {
                for (const auto* tablet : action->Tablets()) {
                    TablesWithActiveActions_.insert(tablet->GetTable());
                    BundlesWithActiveActions_.insert(tablet->GetTable()->GetTabletCellBundle());
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
        } catch (TErrorException& ex) {
            LOG_ERROR("Failed to evaluate tablet balancer schedule formula: %v", ex.Error().GetMessage());
            return false;
        }
    }

    std::vector<const TTabletCell*> GetAliveCells(const TTabletCellBundle* bundle)
    {
        std::vector<const TTabletCell*> cells;
        for (const auto* cell : bundle->TabletCells()) {
            if (IsObjectAlive(cell) && !cell->GetDecommissioned() && cell->GetCellBundle() == bundle) {
                cells.push_back(cell);
            }
        }
        return cells;
    }

    void CreateMoveAction(const TTablet* tablet, TTabletCellId targetCellId)
    {
        auto* table = tablet->GetTable();
        auto* srcCell = tablet->GetCell();

        LOG_DEBUG("Tablet balancer would like to move tablet "
            "(TableId: %v, InMemoryMode: %v, TabletId: %v, SrcCellId: %v, DstCellId: %v, Bundle: %v)",
            table->GetId(),
            table->GetInMemoryMode(),
            tablet->GetId(),
            srcCell->GetId(),
            targetCellId,
            table->GetTabletCellBundle()->GetName());

        TReqCreateTabletAction request;
        request.set_kind(static_cast<int>(ETabletActionKind::Move));
        ToProto(request.mutable_tablet_ids(), std::vector<TTabletId>{tablet->GetId()});
        ToProto(request.mutable_cell_ids(), std::vector<TTabletCellId>{targetCellId});

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
    }

    int ApplyMoveActions()
    {
        int actionCount = 0;
        for (const auto& pair : TabletToTargetCellMap_) {
            if (pair.first->GetCell() != pair.second) {
                CreateMoveAction(pair.first, pair.second->GetId());
                ++actionCount;
            }
        }

        TabletToTargetCellMap_.clear();
        return actionCount;
    }

    void ReassignInMemoryTablets(const TTabletCellBundle* bundle)
    {
        const auto& config = bundle->TabletBalancerConfig();

        if (!config->EnableInMemoryCellBalancer) {
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

        using TMemoryUsage = std::pair<i64, const TTabletCell*>;
        std::vector<TMemoryUsage> memoryUsage;
        i64 total = 0;
        memoryUsage.reserve(cells.size());
        for (const auto* cell : cells) {
            i64 size = cell->LocalStatistics().MemorySize;
            total += size;
            memoryUsage.emplace_back(size, cell);
        }

        auto minmaxCells = std::minmax_element(memoryUsage.begin(), memoryUsage.end());
        if (!hardThresholdViolated(minmaxCells.first->first, minmaxCells.second->first)) {
            return;
        }

        std::sort(memoryUsage.begin(), memoryUsage.end());
        i64 mean = total / cells.size();
        std::priority_queue<TMemoryUsage, std::vector<TMemoryUsage>, std::greater<TMemoryUsage>> queue;

        for (const auto& pair : memoryUsage) {
            if (pair.first >= mean) {
                break;
            }
            queue.push(pair);
        }

        int actionCount = 0;
        for (int index = memoryUsage.size() - 1; index >= 0; --index) {
            auto cellSize = memoryUsage[index].first;
            auto* cell = memoryUsage[index].second;

            for (const auto* tablet : cell->Tablets()) {
                if (tablet->GetInMemoryMode() == EInMemoryMode::None) {
                    continue;
                }

                if (queue.empty() || cellSize <= mean) {
                    break;
                }

                auto top = queue.top();

                if (!softThresholdViolated(top.first, cellSize)) {
                    break;
                }

                auto statistics = tabletManager->GetTabletStatistics(tablet);
                auto tabletSize = statistics.MemorySize;

                if (tabletSize == 0) {
                    continue;
                }

                if (tabletSize < cellSize - top.first) {
                    queue.pop();
                    top.first += tabletSize;
                    cellSize -= tabletSize;
                    if (top.first < mean) {
                        queue.push(top);
                    }

                    CreateMoveAction(tablet, top.second->GetId());

                    ++actionCount;
                }
            }
        }

        Profiler.Enqueue(
            "/in_memory_moves",
            actionCount,
            NProfiling::EMetricType::Gauge,
            {bundle->GetProfilingTag()});
    }

    void ReassignExtMemoryTabletsOfTable(
        const std::vector<const TTablet*>& tablets,
        const std::vector<const TTabletCell*>& bundleCells,
        THashMap<const TTabletCell*, std::vector<const TTablet*>>* slackTablets)
    {
        THashMap<const TTabletCell*, std::vector<const TTablet*>> tabletsByCell;
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
            if (!tabletsByCell.has(cell)) {
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
                const auto* tablet = srcTablets.back();
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

    void ReassignSlackTablets(std::vector<std::pair<const TTabletCell*, std::vector<const TTablet*>>> cellTablets)
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
                if (!presentTables[dstIndex].has(tablet->GetTable())) {
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

    void ReassignExtMemoryTablets(const TTabletCellBundle* bundle)
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

        THashMap<const TTableNode*, std::vector<const TTablet*>> tabletsByTable;
        for (const auto* cell : bundleCells) {
            if (cell->Tablets().empty()) {
                haveEmptyCells = true;
            }
            for (const auto* tablet : cell->Tablets()) {
                const auto* table = tablet->GetTable();
                if (table->GetInMemoryMode() == EInMemoryMode::None) {
                    tabletsByTable[table].push_back(tablet);
                }
            }
        }

        THashMap<const TTabletCell*, std::vector<const TTablet*>> slackTablets;
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
            std::vector<std::pair<const TTabletCell*, std::vector<const TTablet*>>> slackTabletsVector;
            for (auto&& pair : slackTablets) {
                slackTabletsVector.emplace_back(pair.first, std::move(pair.second));
            }
            ReassignSlackTablets(slackTabletsVector);
        }

        int actionCount = ApplyMoveActions();

        Profiler.Enqueue(
            "/ext_memory_moves",
            actionCount,
            NProfiling::EMetricType::Gauge,
            {bundle->GetProfilingTag()});
    }

    void BalanceTablets(const TTabletCellBundle* bundle)
    {
        if (!TabletIdQueue_.has(bundle->GetId())) {
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
            if (!IsTabletReshardable(tablet) ||
                TablesWithActiveActions_.has(tablet->GetTable()))
            {
                continue;
            }

            auto size = GetTabletSize(tablet);
            auto bounds = GetTabletSizeConfig(tablet);
            if (size < bounds.MinTabletSize || size > bounds.MaxTabletSize) {
                if (MergeSplitTablet(tablet, bounds)) {
                    ++actionCount;
                }
            }
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

    bool MergeSplitTablet(TTablet* tablet, const TTabletSizeConfig& bounds)
    {
        auto* table = tablet->GetTable();

        i64 desiredSize = bounds.DesiredTabletSize;
        i64 size = GetTabletSize(tablet);

        if (desiredSize == 0) {
            desiredSize = 1;
        }

        int startIndex = tablet->GetIndex();
        int endIndex = tablet->GetIndex();

        auto sizeGood = [&] () {
            i64 tabletCount = size / desiredSize;
            if (tabletCount == 0) {
                return false;
            }

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

        int newTabletCount = size / desiredSize;
        if (newTabletCount == 0) {
            newTabletCount = 1;
        }

        if (newTabletCount == endIndex - startIndex + 1 && newTabletCount == 1) {
            LOG_DEBUG("Tablet balancer is unable to reshard tablet (TableId: %v, TabletId: %v)",
                table->GetId(),
                tablet->GetId());
            return false;
        }

        std::vector<TTabletId> tabletIds;
        for (int index = startIndex; index <= endIndex; ++index) {
            tabletIds.push_back(table->Tablets()[index]->GetId());
            TouchedTablets_.insert(table->Tablets()[index]);
        }

        LOG_DEBUG("Tablet balancer would like to reshard tablets (TableId: %v, TabletIds: %v, NewTabletCount: %v)",
            table->GetId(),
            tabletIds,
            newTabletCount);

        TReqCreateTabletAction request;
        request.set_kind(static_cast<int>(ETabletActionKind::Reshard));
        ToProto(request.mutable_tablet_ids(), tabletIds);
        request.set_tablet_count(newTabletCount);

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);

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
                LOG_INFO("Tablet balancer is disabled, see master config");
            }
            Enabled_ = false;
            return;
        }

        const auto& balancerConfig = Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->TabletBalancer;

        if (!balancerConfig->EnableTabletBalancer) {
            if (Enabled_) {
                LOG_INFO("Tablet balancer is disabled, see //sys/@config");
            }
            Enabled_ = false;
            return;
        }

        FallbackBalancingSchedule_ = balancerConfig->TabletBalancerSchedule;

        if (!Enabled_) {
            LOG_INFO("Tablet balancer enabled");
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

