#include "parameterized_move_helpers.h"

#include "balancing_helpers.h"
#include "bounded_priority_queue.h"
#include "config.h"
#include "metric.h"
#include "metrics_calculator.h"
#include "public.h"
#include "table.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

namespace NYT::NTabletBalancer {

using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

template <class T>
T Sqr(const T& value)
{
    return value * value;
}

template <int MetricSize>
class TFlat2DMetricGrid
{
    using TMetric = TGenericMetric<MetricSize>;

public:
    DEFINE_BYVAL_RO_PROPERTY(int, Width);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TMetric>, Storage);

    TFlat2DMetricGrid() = default;

    void Resize(int rowCount, int width)
    {
        Width_ = width;
        Storage_.assign(rowCount * Width_, TMetric::Zero());
    }

    Y_FORCE_INLINE TMetric& operator()(int row, int column)
    {
        return Storage_[row * Width_ + column];
    }

    Y_FORCE_INLINE TMetric operator()(int row, int column) const
    {
        return Storage_[row * Width_ + column];
    }

    Y_FORCE_INLINE TMutableRange<TMetric> Row(int row)
    {
        auto* begin = Storage_.data() + row * Width_;
        return TMutableRange<TMetric>(begin, Width_);
    }

    Y_FORCE_INLINE TRange<TMetric> Row(int row) const
    {
        const auto* begin = Storage_.data() + row * Width_;
        return TRange<TMetric>(begin, Width_);
    }

    Y_FORCE_INLINE int GetRowCount() const
    {
        return Width_ == 0
            ? 0
            : Storage_.size() / Width_;
    }
};

////////////////////////////////////////////////////////////////////////////////

bool IsTableMovable(TTableId tableId)
{
    return IsTableType(TypeFromId(tableId));
}

TParameterizedReassignSolverConfig TParameterizedReassignSolverConfig::MergeWith(
    const TParameterizedBalancingConfigPtr& groupConfig,
    std::optional<int> maxMoveActionHardLimit) const
{
    auto maxMoveActionCount = groupConfig->MaxActionCount.value_or(MaxMoveActionCount);
    if (maxMoveActionHardLimit) {
        maxMoveActionCount = std::min(maxMoveActionCount, *maxMoveActionHardLimit);
    }

    // Temporary. Verify that if uniform is enabled then factors were changed properly.
    auto factors = Factors->MergeWith(groupConfig->Factors);
    YT_VERIFY(!groupConfig->PerTableUniform.value_or(false) ||
        factors->TableCell > 0.0 && factors->TableNode > 0.0);

    auto groupMetrics = groupConfig->GetMetrics();
    return TParameterizedReassignSolverConfig{
        .MaxMoveActionCount = maxMoveActionCount,
        .BoundedPriorityQueueSize = groupConfig->BoundedPriorityQueueSize.value_or(BoundedPriorityQueueSize),
        .NodeDeviationThreshold = groupConfig->NodeDeviationThreshold.value_or(NodeDeviationThreshold),
        .CellDeviationThreshold = groupConfig->CellDeviationThreshold.value_or(CellDeviationThreshold),
        .MinRelativeMetricImprovement = groupConfig->MinRelativeMetricImprovement.value_or(
            MinRelativeMetricImprovement),
        .MinTabletsPerMoveRecomputationWorker = groupConfig->MinTabletsPerMoveRecomputationWorker.value_or(
            MinTabletsPerMoveRecomputationWorker),
        .Metrics = groupMetrics.empty()
            ? Metrics
            : std::move(groupMetrics),
        .Factors = std::move(factors),
    };
}

void FormatValue(TStringBuilderBase* builder, const TComponentFactorConfigPtr& config, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "CellFactor: %v, NodeFactor: %v, TableCellFactor: %v, TableNodeFactor: %v",
        config->Cell,
        config->Node,
        config->TableCell,
        config->TableNode);
}

void FormatValue(TStringBuilderBase* builder, const TParameterizedReassignSolverConfig& config, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "MaxMoveActionCount: %v, NodeDeviationThreshold: %v, CellDeviationThreshold: %v, "
        "MinRelativeMetricImprovement: %v, Metrics: %v, Factors: %v",
        config.MaxMoveActionCount,
        config.NodeDeviationThreshold,
        config.CellDeviationThreshold,
        config.MinRelativeMetricImprovement,
        config.Metrics,
        config.Factors);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMetricsCalculatorType,
    (Parameterized)
    (Replica)
);

template <int MetricSize>
class TParameterizedReassignSolver
    : public IParameterizedReassignSolver
{
    using TMetric = TGenericMetric<MetricSize>;

public:
    TParameterizedReassignSolver(
        TTabletCellBundlePtr bundle,
        std::vector<std::string> performanceCountersKeys,
        TParameterizedReassignSolverConfig config,
        TGroupName groupName,
        TTableParameterizedMetricTrackerPtr metricTracker,
        IThreadPoolPtr workerPool,
        EMetricsCalculatorType type,
        const TLogger& logger)
        : Bundle_(std::move(bundle))
        , Logger(logger
            .WithTag("BundleName", Bundle_->Name)
            .WithTag("Group", groupName))
        , Config_(std::move(config))
        , GroupName_(std::move(groupName))
        , WorkerPool_(std::move(workerPool))
        , MetricTracker_(std::move(metricTracker))
        , MoveActions_(Config_.BoundedPriorityQueueSize)
        , RecomputeWorkerMoveActions_(BuildRecomputeWorkerMoveActions(Config_.BoundedPriorityQueueSize))
    {
        YT_VERIFY(ssize(Config_.Metrics) == MetricSize);

        switch (type) {
            case EMetricsCalculatorType::Parameterized:
                Calculator_ = std::make_unique<TParameterizedMetricsCalculator<MetricSize>>(
                    Config_.Metrics,
                    std::move(performanceCountersKeys),
                    Bundle_->PerformanceCountersTableSchema,
                    Logger);
                break;

            case EMetricsCalculatorType::Replica:
                Calculator_ = CreateReplicaMetricsCalculator<MetricSize>(
                    Config_.Metrics,
                    std::move(performanceCountersKeys),
                    Bundle_->PerformanceCountersTableSchema,
                    Bundle_->PerClusterPerformanceCountersTableSchemas,
                    Logger,
                    Bundle_->Config->EnableVerboseLogging);
                break;
        }
    }

    std::vector<TMoveDescriptor> BuildActionDescriptors() override
    {
        YT_TLOG_DEBUG("Reporting parameterized balancing config")
            .With("Config", Config_);

        Initialize();

        if (!ShouldTrigger()) {
            YT_TLOG_DEBUG("Parameterized balancing was not triggered")
                .With("NodeDeviationThreshold", Config_.NodeDeviationThreshold)
                .With("CellDeviationThreshold", Config_.CellDeviationThreshold);
            return {};
        }

        int availableActionCount = Config_.MaxMoveActionCount;
        while (availableActionCount > 0) {
            LogMessageCount_ = 0;
            if (TryFindBestAction()) {
                if ((CurrentMetric_ * Config_.MinRelativeMetricImprovement / std::ssize(Nodes_)).GetTotalValue() >=
                    BestActionInfo_.MetricDiff.GetTotalValue())
                {
                    YT_TLOG_DEBUG("Metric-improving action is not better enough")
                        .WithFormat("CurrentMetric", "%e", CurrentMetric_)
                        .WithFormat("MetricAfterAction", "%e", BestActionInfo_.MetricDiff);
                    break;
                }

                ApplyBestAction(&availableActionCount);

                YT_TLOG_DEBUG("Total parameterized metric changed")
                    .WithFormat("Old", "%e", CurrentMetric_)
                    .WithFormat("Diff", "%e", BestActionInfo_.MetricDiff);
                CurrentMetric_ -= BestActionInfo_.MetricDiff;

                YT_VERIFY(CurrentMetric_.GetTotalValue() >= 0);
            } else {
                YT_TLOG_DEBUG("Metric-improving action was not found");
                break;
            }
        }

        YT_TLOG_INFO("Found all move actions")
            .With("FullRecomputeAttempts", FullRecomputeAttempts_)
            .With("PartialRecomputeAttempts", PartialRecomputeAttempts_);

        std::vector<TMoveDescriptor> descriptors;
        for (auto& tablet : Tablets_) {
            auto sourceCellId = tablet.Tablet->Cell.Lock()->Id;
            auto destinationCellId = Cells_[tablet.CellIndex].Id;
            if (sourceCellId != destinationCellId) {
                descriptors.emplace_back(TMoveDescriptor{
                    .TabletId = tablet.Tablet->Id,
                    .TabletCellId = destinationCellId,
                    .CorrelationId = TGuid::Create()
                });
            }
        }

        if (std::ssize(descriptors) > Config_.MaxMoveActionCount) {
            YT_TLOG_ALERT("Too many actions created during parametrized balancing")
                .With("DescriptorCount", std::ssize(descriptors))
                .With("MoveActionLimit", Config_.MaxMoveActionCount);
            return {};
        }

        YT_TLOG_DEBUG("Scheduled move actions for parameterized tablets balancing")
            .With("ActionCount", std::ssize(descriptors))
            .With("MoveActionLimit", Config_.MaxMoveActionCount);

        if (MetricTracker_) {
            MetricTracker_->AfterMetric.Update(CurrentMetric_.GetTotalValue());
        }

        YT_TLOG_INFO("Metric after iteration")
            .WithFormat("Metric", "%e", CurrentMetric_);

        return descriptors;
    }

private:
    using TApplyActionCallback = std::function<void(int*)>;

    struct TNodeInfo
    {
        const TNodeAddress Address;
        TMetric Metric;
        double MetricTotal = 0.0;
        i64 FreeNodeMemory = 0;
        i64 CellMemoryLimit;
        int Index;
        bool Overloaded = false;
        i64 SafeFreeMemoryAmount;
    };

    struct TTabletCellInfo
    {
        TTabletCellPtr Cell;
        TTabletCellId Id;
        TNodeInfo* Node;
        TMetric Metric;
        i64 FreeCellMemory = 0;
        int Index;
    };

    struct TTabletInfo
    {
        TTabletPtr Tablet;
        TTabletId Id;
        i64 MemorySize;
        EInMemoryMode InMemoryMode;
        TMetric Metric;
        int CellIndex;
        int TableIndex;
        int NodeIndex;
    };

    struct TMoveActionInfo
    {
        TTabletCellInfo* SourceCell;
        TTabletCellInfo* DestinationCell;
        TTabletInfo* Tablet;

        TMetric MetricDiff;
    };

    struct TTabletMoveBaseline
    {
        TMetric TabletNodeMetric;
        TMetric TabletTableNodeMetric;
        TMetric TabletCellMetric;
        TMetric TabletTableCellMetric;
        TMetric DoubledTabletMetric;
        TMetric TableCellFactor;
        TMetric TableNodeFactor;
    };

    // Reused only while evaluating destinations for one tablet against unchanged loads.
    struct TMoveTabletContext
    {
        TNodeInfo* DestinationNode = nullptr;
        TMetric MetricDiffBeforeDestinationCell;
        double MetricImprovementUpperBound = 0.0;
    };

    const TTabletCellBundlePtr Bundle_;
    const TLogger Logger;
    const TParameterizedReassignSolverConfig Config_;
    const TGroupName GroupName_;
    const IThreadPoolPtr WorkerPool_;
    TTableParameterizedMetricTrackerPtr MetricTracker_;
    std::unique_ptr<TParameterizedMetricsCalculator<MetricSize>> Calculator_;

    std::vector<TTabletInfo> Tablets_;
    std::vector<TTabletCellInfo> Cells_;
    std::vector<TTableId> TableIds_;
    std::vector<int> SortedCellIndexes_;
    THashMap<TNodeAddress, TNodeInfo> Nodes_;

    using TMoveActions = TBoundedPriorityQueue<TMoveActionInfo>;
    static constexpr int MaxRecomputeThreadCount = 4;

    TMoveActions MoveActions_;
    std::array<TMoveActions, MaxRecomputeThreadCount> RecomputeWorkerMoveActions_;

    TMoveActionInfo BestActionInfo_;

    double TableNormalizingCoefficient_ = 1.0;

    TFlat2DMetricGrid<MetricSize> TableByNodeMetric_;
    TFlat2DMetricGrid<MetricSize> TableByCellMetric_;
    std::vector<TMetric> TableCellFactors_;
    std::vector<TMetric> TableNodeFactors_;

    TMetric CurrentMetric_;
    TMetric CellFactor_ = TMetric::Unit();
    TMetric NodeFactor_ = TMetric::Unit();

    std::atomic<int> LogMessageCount_ = 0;

    int FullRecomputeAttempts_ = 0;
    int PartialRecomputeAttempts_ = 0;
    int MaxCellPerNodeCount_ = 0;

private:
    static std::array<TMoveActions, MaxRecomputeThreadCount> BuildRecomputeWorkerMoveActions(int queueSize)
    {
        return [queueSize] <size_t... Is> (std::index_sequence<Is...>) {
            return std::array<TMoveActions, MaxRecomputeThreadCount>{
                ((void)Is, TMoveActions(queueSize))...
            };
        }(std::make_index_sequence<MaxRecomputeThreadCount>{});
    };

    void Initialize()
    {
        auto cells = Bundle_->GetAliveCells();

        if (cells.empty()) {
            YT_TLOG_WARNING("There are no alive cells");
            return;
        }

        THashMap<TTabletCellId, int> cellInfoIndex;
        THashMap<TTableId, int> tableInfoIndex;
        THashMap<TNodeAddress, int> nodeInfoIndex;

        THashMap<TTableId, const TTable*> tablesToCalculateMetrics;
        for (const auto& cell : cells) {
            for (const auto& [tabletId, tablet] : cell->Tablets) {
                if (!IsTableMovable(tablet->Table->Id)) {
                    continue;
                }

                if (TypeFromId(tabletId) != EObjectType::Tablet) {
                    continue;
                }

                if (tablet->Table->GetBalancingGroup() != GroupName_) {
                    continue;
                }

                if (!tablet->Table->IsParameterizedMoveBalancingEnabled()) {
                    continue;
                }

                tablesToCalculateMetrics[tablet->Table->Id] = tablet->Table;
            }
        }

        THashMap<TTabletId, TMetric> tabletMetrics;
        for (const auto& [tableId, table] : tablesToCalculateMetrics) {
            auto metrics = Calculator_->GetTableMetrics(table);
            for (const auto& [tabletId, metric] : metrics) {
                EmplaceOrCrash(tabletMetrics, tabletId, metric);
            }
        }

        Cells_.reserve(std::ssize(cells));
        for (const auto& cell : cells) {
            int nodeIndex = nodeInfoIndex.try_emplace(cell->NodeAddress.value(), std::ssize(nodeInfoIndex)).first->second;
            auto* nodeInfo = &Nodes_.emplace(*cell->NodeAddress, TNodeInfo{
                .Address = *cell->NodeAddress,
                .Index = nodeIndex,
            }).first->second;

            int cellIndex = std::ssize(Cells_);

            EmplaceOrCrash(cellInfoIndex, cell->Id, cellIndex);
            Cells_.emplace_back(TTabletCellInfo{
                .Cell = cell,
                .Id = cell->Id,
                .Node = nodeInfo,
                .Index = cellIndex,
            });

            for (const auto& [tabletId, tablet] : cell->Tablets) {
                if (!tabletMetrics.contains(tabletId)) {
                    // For now let's verify that we didn't miss any tablet for no reason.
                    YT_VERIFY(
                        TypeFromId(tabletId) != EObjectType::Tablet ||
                        !IsTableMovable(tablet->Table->Id) ||
                        tablet->Table->GetBalancingGroup() != GroupName_ ||
                        !tablet->Table->IsParameterizedMoveBalancingEnabled());
                    continue;
                }

                auto tabletMetric = GetOrCrash(tabletMetrics, tabletId);

                if (tabletMetric.GetTotalValue() <= MinimumAcceptableMetricValue) {
                    YT_TLOG_DEBUG_IF(
                        Bundle_->Config->EnableVerboseLogging,
                        "Skipping tablet since its metric is below the minimum acceptable value")
                        .WithFormat("MinimumAcceptableMetricValue", "%e", MinimumAcceptableMetricValue)
                        .With("TabletId", tabletId)
                        .With("TableId", tablet->Table->Id);
                    continue;
                }

                auto [it, inserted] = tableInfoIndex.try_emplace(tablet->Table->Id, std::ssize(tableInfoIndex));
                int tableIndex = it->second;

                if (inserted) {
                    TableIds_.push_back(tablet->Table->Id);
                }

                Tablets_.push_back(TTabletInfo{
                    .Tablet = tablet,
                    .Id = tablet->Id,
                    .MemorySize = tablet->Statistics.MemorySize,
                    .InMemoryMode = tablet->Table->InMemoryMode,
                    .Metric = tabletMetric,
                    .CellIndex = cellIndex,
                    .TableIndex = tableIndex,
                    .NodeIndex = nodeIndex,
                });
            }
        }

        CalculateMemory(cellInfoIndex);

        for (const auto& node : Bundle_->NodeStatistics) {
            MaxCellPerNodeCount_ = std::max(MaxCellPerNodeCount_, node.second.TabletSlotCount);
        }

        int tableCount = std::ssize(tableInfoIndex);
        if (tableCount == 0) {
            YT_TLOG_DEBUG_IF(Bundle_->Config->EnableVerboseLogging, "There are no tables to balance");
            return;
        }

        TableNormalizingCoefficient_ = 1.0 / tableCount;

        CalculateModifyingFactors();

        TableByCellMetric_.Resize(tableCount, std::ssize(cellInfoIndex));
        TableByNodeMetric_.Resize(tableCount, std::ssize(nodeInfoIndex));
        TableCellFactors_.resize(tableCount);
        TableNodeFactors_.resize(tableCount);

        for (const auto& tablet : Tablets_) {
            const auto& nodeAddress = Cells_[tablet.CellIndex].Cell->NodeAddress.value();

            Cells_[tablet.CellIndex].Metric += tablet.Metric * CellFactor_;
            Nodes_[nodeAddress].Metric += tablet.Metric * NodeFactor_;
            TableByCellMetric_(tablet.TableIndex, tablet.CellIndex) += tablet.Metric;
            TableByNodeMetric_(tablet.TableIndex, tablet.NodeIndex) += tablet.Metric;
        }

        CalculateAndApplyTableFactors();

        for (auto& [address, node] : Nodes_) {
            node.MetricTotal = node.Metric.GetTotalValue();
        }

        for (int index = 0; index < std::ssize(Cells_); ++index) {
            SortedCellIndexes_.emplace_back(index);
        }

        if (Bundle_->Config->EnableVerboseLogging) {
            for (const auto& [nodeAddress, nodeInfo] : Nodes_) {
                YT_TLOG_DEBUG("Calculated node metric")
                    .With("NodeAddress", nodeAddress)
                    .WithFormat("NodeMetric", "%e", nodeInfo.Metric);
            }
        }

        CurrentMetric_ = CalculateTotalBundleMetric();

        double currentTotalMetric = CurrentMetric_.GetTotalValue();

        if (MetricTracker_) {
            MetricTracker_->BeforeMetric.Update(currentTotalMetric);
        }

        YT_TLOG_INFO("Metric before iteration")
            .WithFormat("Metric", "%e", CurrentMetric_);

        YT_VERIFY(currentTotalMetric >= 0);
    }

    void CalculateMemory(const THashMap<TTabletCellId, int>& cellInfoIndex)
    {
        if (Bundle_->NodeStatistics.empty()) {
            YT_TLOG_DEBUG("Don't calculate memory because there are no in-memory tables with parameterized balancing");
            return;
        }

        THashMap<TNodeAddress, int> cellCount;
        THashMap<TNodeAddress, i64> actualMemoryUsage;
        THashMap<const TTabletCell*, i64> cellMemoryUsage;
        for (const auto& cellInfo : Cells_) {
            ++cellCount[*cellInfo.Cell->NodeAddress];
            actualMemoryUsage[*cellInfo.Cell->NodeAddress] += cellInfo.Cell->Statistics.MemorySize;

            i64 usage = 0;
            for (const auto& [id, tablet] : cellInfo.Cell->Tablets) {
                usage += tablet->Statistics.MemorySize;
            }

            EmplaceOrCrash(cellMemoryUsage, cellInfo.Cell.Get(), std::max(cellInfo.Cell->Statistics.MemorySize, usage));
        }

        THashMap<TNodeAddress, i64> cellMemoryLimit;
        for (const auto& [address, statistics] : Bundle_->NodeStatistics) {
            if (!cellCount.contains(address)) {
                YT_TLOG_DEBUG("There are no alive cells on the node")
                    .With("Node", address);
                continue;
            }

            i64 actualUsage = GetOrCrash(actualMemoryUsage, address);
            i64 free = statistics.MemoryLimit - statistics.MemoryUsed;
            i64 unaccountedUsage = 0;
            auto count = GetOrCrash(cellCount, address);

            if (actualUsage > statistics.MemoryUsed) {
                YT_TLOG_DEBUG("Using total cell memory as node memory usage")
                    .With("Node", address)
                    .With("Used", statistics.MemoryUsed)
                    .With("Sum", actualUsage)
                    .With("Limit", statistics.MemoryLimit);
                if (statistics.MemoryLimit < actualUsage) {
                    YT_TLOG_WARNING("Node memory usage exceeds memory limit")
                        .With("MemoryLimit", statistics.MemoryLimit)
                        .With("MemoryUsage", statistics.MemoryUsed)
                        .With("ActualMemoryUsage", actualUsage)
                        .With("Node", address)
                        .With("CellCount", count)
                        .With("TabletSlotCount", statistics.TabletSlotCount);
                }
                free = statistics.MemoryLimit - actualUsage;
            } else {
                unaccountedUsage = statistics.MemoryUsed - actualUsage;
            }

            auto tabletSlotCount = std::max(statistics.TabletSlotCount, count);
            auto cellLimit = (statistics.MemoryLimit - unaccountedUsage) / tabletSlotCount;

            auto& node = GetOrCrash(Nodes_, address);
            node.FreeNodeMemory = free;
            node.Overloaded = free < 0;
            node.SafeFreeMemoryAmount = statistics.MemoryLimit * (1 - Bundle_->Config->SafeUsedTabletStaticRatio);
            node.CellMemoryLimit = cellLimit;

            EmplaceOrCrash(cellMemoryLimit, address, cellLimit);
        }

        for (const auto& [cell, usage] : cellMemoryUsage) {
            auto limit = GetOrCrash(cellMemoryLimit, *cell->NodeAddress);
            Cells_[GetOrCrash(cellInfoIndex, cell->Id)].FreeCellMemory = limit - usage;
        }
    }

    bool ShouldTrigger() const
    {
        if (Nodes_.empty()) {
            return false;
        }

        for (int metricIndex = 0; metricIndex < MetricSize; ++metricIndex) {
            auto [minNode, maxNode] = std::minmax_element(
                Nodes_.begin(),
                Nodes_.end(),
                [metricIndex] (const auto& lhs, const auto& rhs) {
                    return lhs.second.Metric[metricIndex] < rhs.second.Metric[metricIndex];
                });

            auto minNodeMetric = minNode->second.Metric[metricIndex];
            auto maxNodeMetric = maxNode->second.Metric[metricIndex];

            auto [minCell, maxCell] = std::minmax_element(
                Cells_.begin(),
                Cells_.end(),
                [metricIndex] (const auto& lhs, const auto& rhs) {
                    return lhs.Metric[metricIndex] < rhs.Metric[metricIndex];
                });

            auto minCellMetric = minCell->Metric[metricIndex];
            auto maxCellMetric = maxCell->Metric[metricIndex];

            // An all-zero component must not trigger balancing regardless of the thresholds.
            bool byNodeTrigger = maxNodeMetric > 0 &&
                maxNodeMetric >= minNodeMetric * (1 + Config_.NodeDeviationThreshold);
            bool byCellTrigger = maxCellMetric > 0 &&
                maxCellMetric >= minCellMetric * (1 + Config_.CellDeviationThreshold);

            YT_TLOG_DEBUG_IF(
                Bundle_->Config->EnableVerboseLogging,
                "Arguments for checking whether parameterized balancing should trigger have been calculated")
                .With("MetricIndex", metricIndex)
                .WithFormat("MinNodeMetric", "%e", minNodeMetric)
                .WithFormat("MaxNodeMetric", "%e", maxNodeMetric)
                .WithFormat("MinCellMetric", "%e", minCellMetric)
                .WithFormat("MaxCellMetric", "%e", maxCellMetric)
                .With("NodeDeviationThreshold", Config_.NodeDeviationThreshold)
                .With("CellDeviationThreshold", Config_.CellDeviationThreshold);

            if (byNodeTrigger || byCellTrigger) {
                return true;
            }
        }

        return false;
    }

    TMetric CalculateTotalBundleMetric() const
    {
        TMetric cellMetric;
        for (const auto& item : Cells_) {
            cellMetric += Sqr(item.Metric);
        }

        TMetric nodeMetric;
        for (const auto& item : Nodes_) {
            nodeMetric += Sqr(item.second.Metric);
        }

        TMetric tableCellMetric;
        for (const auto& metric : TableByCellMetric_.Storage()) {
            tableCellMetric += Sqr(metric);
        }
        tableCellMetric *= TableNormalizingCoefficient_;

        TMetric tableNodeMetric;
        for (const auto& metric : TableByNodeMetric_.Storage()) {
            tableNodeMetric += Sqr(metric);
        }
        tableNodeMetric *= TableNormalizingCoefficient_;

        YT_TLOG_DEBUG("Calculated total metrics")
            .WithFormat("CellMetric", "%e", cellMetric)
            .WithFormat("NodeMetric", "%e", nodeMetric)
            .WithFormat("TableCellMetric", "%e", tableCellMetric)
            .WithFormat("TableNodeMetric", "%e", tableNodeMetric);

        return cellMetric + nodeMetric + tableCellMetric + tableNodeMetric;
    }

    void CalculateAndApplyTableFactors()
    {
        for (int tableIndex = 0; tableIndex < TableByCellMetric_.GetRowCount(); ++tableIndex) {
            auto row = TableByCellMetric_.Row(tableIndex);
            auto tableMetric = std::accumulate(
                row.begin(),
                row.end(),
                TMetric::Zero(),
                [] (TMetric x, const auto& metric) {
                    return x + metric;
                });

            double cellCount = TableByCellMetric_.GetWidth();
            double nodeCount = TableByNodeMetric_.GetWidth();

            TableCellFactors_[tableIndex] = tableMetric.AsNormalizationFactor(cellCount);
            TableNodeFactors_[tableIndex] = tableMetric.AsNormalizationFactor(nodeCount);

            //  Per-cell dispersion is less important than per-node so we decrease its absolute value.
            TableCellFactors_[tableIndex] *= nodeCount / cellCount;

            TableCellFactors_[tableIndex] *= Config_.Factors->TableCell.value();
            TableNodeFactors_[tableIndex] *= Config_.Factors->TableNode.value();

            YT_TLOG_DEBUG_IF(Bundle_->Config->EnableVerboseLogging, "Calculated per-table factors for cells and nodes")
                .With("TableId", TableIds_[tableIndex])
                .With("TableCellFactor", TableCellFactors_[tableIndex])
                .With("TableNodeFactor", TableNodeFactors_[tableIndex]);

            for (auto& value : TableByCellMetric_.Row(tableIndex)) {
                value *= TableCellFactors_[tableIndex];
            }
            for (auto& value : TableByNodeMetric_.Row(tableIndex)) {
                value *= TableNodeFactors_[tableIndex];
            }
        }
    }

    void CalculateModifyingFactors()
    {
        YT_VERIFY(Cells_.size() > 0);
        YT_VERIFY(Nodes_.size() > 0);

        double cellCount = std::ssize(Cells_);
        double nodeCount = std::ssize(Nodes_);

        auto totalMetric = std::accumulate(
            Tablets_.begin(),
            Tablets_.end(),
            TMetric::Zero(),
            [] (TMetric x, const auto& metric) {
                return x + metric.Metric;
            });

        CellFactor_ = totalMetric.AsNormalizationFactor(cellCount);
        NodeFactor_ = totalMetric.AsNormalizationFactor(nodeCount);

        //  Per-cell dispersion is less important than per-node so we decrease its absolute value.
        CellFactor_ *= nodeCount / cellCount;

        CellFactor_ *= Config_.Factors->Cell.value();
        NodeFactor_ *= Config_.Factors->Node.value();

        YT_TLOG_DEBUG("Calculated modifying factors")
            .With("CellFactor", CellFactor_)
            .With("NodeFactor", NodeFactor_);
    }

    bool CheckMoveFollowsMemoryLimits(
        const TTabletInfo* tablet,
        const TTabletCellInfo* sourceCell,
        const TTabletCellInfo* destinationCell) const
    {
        if (tablet->InMemoryMode == EInMemoryMode::None) {
            return true;
        }

        auto size = tablet->MemorySize;
        if (size <= destinationCell->Node->CellMemoryLimit && destinationCell->FreeCellMemory < size) {
            return false;
        }

        return destinationCell->Node == sourceCell->Node ||
            (destinationCell->Node->FreeNodeMemory >= size &&
             !destinationCell->Node->Overloaded &&
             destinationCell->Node->SafeFreeMemoryAmount <= destinationCell->Node->FreeNodeMemory - size);
    }

    Y_FORCE_INLINE TTabletMoveBaseline ComputeTabletBaseline(const TTabletInfo& tablet) const
    {
        TTabletMoveBaseline baseline;

        baseline.TabletNodeMetric = tablet.Metric * NodeFactor_;
        baseline.TabletCellMetric = tablet.Metric * CellFactor_;
        baseline.DoubledTabletMetric = tablet.Metric * 2;

        baseline.TableCellFactor = TableCellFactors_[tablet.TableIndex];
        baseline.TableNodeFactor = TableNodeFactors_[tablet.TableIndex];

        baseline.TabletTableCellMetric = tablet.Metric * baseline.TableCellFactor;
        baseline.TabletTableNodeMetric = tablet.Metric * baseline.TableNodeFactor;

        return baseline;
    }

    //! Generates an action moving |tablet| to |cell|. Returns |false| when the pruning
    //! heuristic suggests stopping the iteration.
    Y_FORCE_INLINE bool TryMoveTablet(
        TTabletInfo* tablet,
        TTabletCellInfo* cell,
        const TTabletMoveBaseline& baseline,
        TMoveTabletContext* context,
        TBoundedPriorityQueue<TMoveActionInfo>* moveActions)
    {
        double bestDiscardedCost = moveActions->GetBestDiscardedCost();

        auto* sourceCell = &Cells_[tablet->CellIndex];

        if (cell == sourceCell) {
            // Trying to move the tablet from the cell to itself.
            return true;
        }

        auto* sourceNode = sourceCell->Node;
        auto* destinationNode = cell->Node;

        if (!CheckMoveFollowsMemoryLimits(tablet, sourceCell, cell)) {
            // Cannot move due to memory limits.
            YT_TLOG_DEBUG_IF(
                Bundle_->Config->EnableVerboseLogging && LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "Cannot move tablet")
                .With("TabletId", tablet->Id)
                .With("CellId", cell->Id)
                .With("SourceNode", sourceNode->Address)
                .With("DestinationNode", destinationNode->Address);
            return true;
        }

        int tableIndex = tablet->TableIndex;

        if (sourceNode == destinationNode &&
            sourceCell->Metric.IsLessOrEqualComponentwise(cell->Metric) &&
            (Config_.Factors->TableCell.value() == 0 ||
                TableByCellMetric_(tableIndex, sourceCell->Index).IsLessOrEqualComponentwise(
                    TableByCellMetric_(tableIndex, cell->Index))))
        {
            // Node metrics do not change, and neither cell nor per-table cell
            // components can improve. Skip this candidate, not the remaining cells.
            return true;
        }

        if (context->DestinationNode != destinationNode) {
            TMetric newMetricDiff;

            if (sourceNode != destinationNode) {
                newMetricDiff +=
                    (sourceNode->Metric - destinationNode->Metric - baseline.TabletNodeMetric) * NodeFactor_;

                newMetricDiff +=
                    (TableByNodeMetric_(tableIndex, sourceNode->Index) -
                        TableByNodeMetric_(tableIndex, destinationNode->Index) -
                        baseline.TabletTableNodeMetric) *
                    baseline.TableNodeFactor * TableNormalizingCoefficient_;
            }

            newMetricDiff += (sourceCell->Metric - baseline.TabletCellMetric) * CellFactor_;

            newMetricDiff +=
                (TableByCellMetric_(tableIndex, sourceCell->Index) - baseline.TabletTableCellMetric) *
                baseline.TableCellFactor * TableNormalizingCoefficient_;

            context->DestinationNode = destinationNode;
            context->MetricDiffBeforeDestinationCell = newMetricDiff;
            context->MetricImprovementUpperBound = (newMetricDiff * baseline.DoubledTabletMetric).GetTotalValue();
        }

        // NB(dave11ar, ifsmirnov): Sorting nodes by their total metric does not guarantee
        // that the bound below decreases for subsequent nodes, even with a single metric,
        // since per-table node metrics are not necessarily ordered the same way.
        // With multiple metrics, component weights can also break this monotonicity.
        // Stopping the search here may therefore miss a better action, but we consider
        // this heuristic good enough to keep the search inexpensive.
        if (context->MetricImprovementUpperBound < bestDiscardedCost) {
            // The cached metric difference takes into account the "positive" part
            // (a certain tablet was moved from a certain node&cell) and partly
            // the "negative" part (a certain tablet is moved to a certain node).
            // It overestimates the final metric improvement. If this overestimate
            // is below best discarded cost then the current action can be discarded.
            // Stopping the search for further actions is a heuristic; see the caveat above.
            return false;
        }

        auto newMetricDiff = context->MetricDiffBeforeDestinationCell;
        newMetricDiff -= cell->Metric * CellFactor_;

        newMetricDiff -=
            TableByCellMetric_(tableIndex, cell->Index) * baseline.TableCellFactor * TableNormalizingCoefficient_;

        newMetricDiff *= baseline.DoubledTabletMetric;

        YT_TLOG_DEBUG_IF(
            Bundle_->Config->EnableVerboseLogging && LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
            "Trying to move tablet to another cell")
            .With("TabletId", tablet->Id)
            .With("CellId", cell->Id)
            .WithFormat("CurrentMetric", "%e", CurrentMetric_)
            .WithFormat("NewMetricDiff", "%e", newMetricDiff)
            .WithFormat("TabletMetric", "%e", tablet->Metric)
            .WithFormat("SourceCellMetric", "%e", sourceCell->Metric)
            .WithFormat("DestinationCellMetric", "%e", cell->Metric)
            .WithFormat("SourceNodeMetric", "%e", sourceNode->Metric)
            .WithFormat("DestinationNodeMetric", "%e", destinationNode->Metric);

        double totalValue = newMetricDiff.GetTotalValue();
        if (totalValue > bestDiscardedCost) {
            moveActions->Insert(
                totalValue,
                {
                    .SourceCell = sourceCell,
                    .DestinationCell = cell,
                    .Tablet = tablet,
                    .MetricDiff = newMetricDiff,
                });
        }

        return true;
    }

    void ApplyBestAction(int* availableActionCount)
    {
        MoveActions_.Invalidate(
            [=, this] (const auto& moveActionInfo) {
                std::array bannedNodes = {
                    moveActionInfo.Payload.SourceCell->Node,
                    moveActionInfo.Payload.DestinationCell->Node,
                };

                for (auto nodeIndex : bannedNodes) {
                    if (nodeIndex == BestActionInfo_.SourceCell->Node) {
                        return true;
                    }

                    if (nodeIndex == BestActionInfo_.DestinationCell->Node) {
                        return true;
                    }
                }

                return false;
        });

        BestActionInfo_.Tablet->CellIndex = BestActionInfo_.DestinationCell->Index;
        BestActionInfo_.SourceCell->Metric -= BestActionInfo_.Tablet->Metric * CellFactor_;
        BestActionInfo_.DestinationCell->Metric += BestActionInfo_.Tablet->Metric * CellFactor_;

        TableByCellMetric_(BestActionInfo_.Tablet->TableIndex, BestActionInfo_.SourceCell->Index) -=
            BestActionInfo_.Tablet->Metric * TableCellFactors_[BestActionInfo_.Tablet->TableIndex];
        TableByCellMetric_(BestActionInfo_.Tablet->TableIndex, BestActionInfo_.DestinationCell->Index) +=
            BestActionInfo_.Tablet->Metric * TableCellFactors_[BestActionInfo_.Tablet->TableIndex];

        *availableActionCount -= 1;

        if (BestActionInfo_.SourceCell->Node != BestActionInfo_.DestinationCell->Node) {
            BestActionInfo_.Tablet->NodeIndex = BestActionInfo_.DestinationCell->Node->Index;
            BestActionInfo_.SourceCell->Node->Metric -= BestActionInfo_.Tablet->Metric * NodeFactor_;
            BestActionInfo_.SourceCell->Node->MetricTotal = BestActionInfo_.SourceCell->Node->Metric.GetTotalValue();
            BestActionInfo_.DestinationCell->Node->Metric += BestActionInfo_.Tablet->Metric * NodeFactor_;
            BestActionInfo_.DestinationCell->Node->MetricTotal = BestActionInfo_.DestinationCell->Node->Metric.GetTotalValue();

            TableByNodeMetric_(BestActionInfo_.Tablet->TableIndex, BestActionInfo_.SourceCell->Node->Index) -=
                BestActionInfo_.Tablet->Metric * TableNodeFactors_[BestActionInfo_.Tablet->TableIndex];
            TableByNodeMetric_(BestActionInfo_.Tablet->TableIndex, BestActionInfo_.DestinationCell->Node->Index) +=
                BestActionInfo_.Tablet->Metric * TableNodeFactors_[BestActionInfo_.Tablet->TableIndex];
        }

        YT_TLOG_DEBUG("Applying best action: moving tablet to another cell")
            .With("TabletId", BestActionInfo_.Tablet->Id)
            .With("SourceCellId", BestActionInfo_.SourceCell->Id)
            .With("DestinationCellId", BestActionInfo_.DestinationCell->Id)
            .With("SourceNode", BestActionInfo_.SourceCell->Node->Address)
            .With("DestinationNode", BestActionInfo_.DestinationCell->Node->Address);

        auto tabletSize = BestActionInfo_.Tablet->MemorySize;
        if (tabletSize == 0) {
            return;
        }

        BestActionInfo_.SourceCell->FreeCellMemory += tabletSize;
        BestActionInfo_.DestinationCell->FreeCellMemory -= tabletSize;

        if (BestActionInfo_.SourceCell->Node != BestActionInfo_.DestinationCell->Node) {
            BestActionInfo_.SourceCell->Node->FreeNodeMemory += tabletSize;
            BestActionInfo_.DestinationCell->Node->FreeNodeMemory -= tabletSize;
        }
    }

    template <class TRecomputator>
    void ExecuteActionRecomputation(TRecomputator&& recomputator)
    {
        // NB(dave11ar): Force |EnsureStarted| for correct work of |GetThreadCount|.
        auto recomputeInvoker = WorkerPool_->GetInvoker();
        int threadCount = WorkerPool_->GetThreadCount();
        int tabletCount = ssize(Tablets_);

        int workerCount = std::clamp(
            tabletCount / Config_.MinTabletsPerMoveRecomputationWorker,
            1,
            std::min(threadCount, MaxRecomputeThreadCount));

        // Optimization for small bundles.
        if (workerCount == 1) {
            recomputator(TMutableRange(Tablets_), &MoveActions_);
            return;
        }

        std::vector<TFuture<void>> futures;
        futures.reserve(workerCount);

        int chunkSize = DivCeil(tabletCount, workerCount);

        for (int workerIndex = 0; workerIndex < workerCount; ++workerIndex) {
            auto* moveActions = &RecomputeWorkerMoveActions_[workerIndex];
            moveActions->Reset();

            int tabletBeginIndex = workerIndex * chunkSize;
            int tabletEndIndex = std::min(tabletBeginIndex + chunkSize, tabletCount);

            futures.push_back(BIND(
                recomputator,
                TMutableRange(Tablets_.begin() + tabletBeginIndex, Tablets_.begin() + tabletEndIndex),
                moveActions)
                .AsyncVia(recomputeInvoker)
                .Run());
        }

        WaitFor(AllSucceeded(std::move(futures)))
            .ThrowOnError();

        for (int workerIndex = 0; workerIndex < workerCount; ++workerIndex) {
            for (auto&& element : RecomputeWorkerMoveActions_[workerIndex].Elements()) {
                MoveActions_.Insert(element.Cost, std::move(element.Payload));
            }
        }
    }

    void RecomputeInvalidatedActions()
    {
        std::array bannedNodes = {
            BestActionInfo_.SourceCell->Node,
            BestActionInfo_.DestinationCell->Node,
        };

        std::vector<TTabletCellInfo*> invalidatedCells;
        invalidatedCells.reserve(MaxCellPerNodeCount_ * 2);
        for (auto& cell : Cells_) {
            if (cell.Node == BestActionInfo_.SourceCell->Node || cell.Node == BestActionInfo_.DestinationCell->Node) {
                invalidatedCells.push_back(&cell);
            }
        }

        ExecuteActionRecomputation([&] (TMutableRange<TTabletInfo> tablets, TMoveActions* moveActions) {
            for (auto& tablet : tablets) {
                auto baseline = ComputeTabletBaseline(tablet);
                auto* sourceCell = &Cells_[tablet.CellIndex];

                if (std::find(bannedNodes.begin(), bannedNodes.end(), sourceCell->Node) != bannedNodes.end()) {
                    TMoveTabletContext context;
                    for (auto cellIndex : SortedCellIndexes_) {
                        if (!TryMoveTablet(&tablet, &Cells_[cellIndex], baseline, &context, moveActions)) {
                            break;
                        }
                    }
                } else {
                    std::array<TMoveTabletContext, 2> contexts;
                    for (auto* cell : invalidatedCells) {
                        auto* context = &contexts[cell->Node == bannedNodes[0] ? 0 : 1];
                        TryMoveTablet(&tablet, cell, baseline, context, moveActions);
                    }
                }
            }
        });
    }

    void RecomputeAllActions()
    {
        MoveActions_.Reset();

        ExecuteActionRecomputation([&] (TMutableRange<TTabletInfo> tablets, TMoveActions* moveActions) {
            for (auto& tablet : tablets) {
                auto baseline = ComputeTabletBaseline(tablet);
                TMoveTabletContext context;
                for (auto cellIndex : SortedCellIndexes_) {
                    if (!TryMoveTablet(&tablet, &Cells_[cellIndex], baseline, &context, moveActions)) {
                        break;
                    }
                }
            }
        });
    }

    bool TryFindBestAction()
    {
        std::sort(SortedCellIndexes_.begin(), SortedCellIndexes_.end(), [&] (auto lhs, auto rhs) {
            return Cells_[lhs].Node->MetricTotal < Cells_[rhs].Node->MetricTotal;
        });

        if (MoveActions_.IsEmpty()) {
            ++FullRecomputeAttempts_;
            RecomputeAllActions();
        } else {
            ++PartialRecomputeAttempts_;
            RecomputeInvalidatedActions();
        }

        if (MoveActions_.IsEmpty()) {
            return false;
        }

        BestActionInfo_ = MoveActions_.ExtractMax().Payload;

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <template <int> class TEntity, class TEntityInterface, class... TArgs>
TIntrusivePtr<TEntityInterface> MakeParametrizedEntity(
    int metricSize,
    TArgs&&... args)
{
    auto createEntity = [&] <int MetricSize>() -> TIntrusivePtr<TEntityInterface> {
        return New<TEntity<MetricSize>>(std::forward<TArgs>(args)...);
    };

    switch (metricSize) {
#define CREATE_ENTITY_FOR_METRIC_SIZE(size) \
        case size: \
            return createEntity.template operator()<size>();

        YT_FOR_EACH_METRIC_SIZE(CREATE_ENTITY_FOR_METRIC_SIZE)

#undef CREATE_ENTITY_FOR_METRIC_SIZE

        default:
            THROW_ERROR_EXCEPTION("Unsupported number of metrics: expected between 1 and %v",
                MaxMetricCount)
                .With("metric_count", metricSize);
    }
}

IParameterizedReassignSolverPtr CreateParameterizedReassignSolver(
    TTabletCellBundlePtr bundle,
    std::vector<std::string> performanceCountersKeys,
    TParameterizedReassignSolverConfig config,
    TGroupName groupName,
    TTableParameterizedMetricTrackerPtr metricTracker,
    IThreadPoolPtr workerPool,
    const NLogging::TLogger& logger)
{
    return MakeParametrizedEntity<TParameterizedReassignSolver, IParameterizedReassignSolver>(
        ssize(config.Metrics),
        std::move(bundle),
        std::move(performanceCountersKeys),
        std::move(config),
        std::move(groupName),
        std::move(metricTracker),
        std::move(workerPool),
        EMetricsCalculatorType::Parameterized,
        logger);
}

IParameterizedReassignSolverPtr CreateReplicaReassignSolver(
    TTabletCellBundlePtr bundle,
    std::vector<std::string> performanceCountersKeys,
    TParameterizedReassignSolverConfig config,
    TGroupName groupName,
    TTableParameterizedMetricTrackerPtr metricTracker,
    IThreadPoolPtr workerPool,
    const NLogging::TLogger& logger)
{
    return MakeParametrizedEntity<TParameterizedReassignSolver, IParameterizedReassignSolver>(
        ssize(config.Metrics),
        std::move(bundle),
        std::move(performanceCountersKeys),
        std::move(config),
        std::move(groupName),
        std::move(metricTracker),
        std::move(workerPool),
        EMetricsCalculatorType::Replica,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
