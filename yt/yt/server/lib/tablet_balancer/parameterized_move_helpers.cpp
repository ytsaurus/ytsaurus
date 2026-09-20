#include "parameterized_move_helpers.h"

#include "balancing_helpers.h"
#include "bounded_priority_queue.h"
#include "config.h"
#include "metrics_calculator.h"
#include "public.h"
#include "table.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

namespace NYT::NTabletBalancer {

using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

constexpr double MinimumAcceptableMetricValue = 1e-30;

////////////////////////////////////////////////////////////////////////////////

namespace {

double Sqr(double x)
{
    return x * x;
}

} // namespace

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

    return TParameterizedReassignSolverConfig{
        .MaxMoveActionCount = maxMoveActionCount,
        .BoundedPriorityQueueSize = groupConfig->BoundedPriorityQueueSize.value_or(BoundedPriorityQueueSize),
        .NodeDeviationThreshold = groupConfig->NodeDeviationThreshold.value_or(NodeDeviationThreshold),
        .CellDeviationThreshold = groupConfig->CellDeviationThreshold.value_or(CellDeviationThreshold),
        .MinRelativeMetricImprovement = groupConfig->MinRelativeMetricImprovement.value_or(
            MinRelativeMetricImprovement),
        .MinTabletsPerMoveRecomputationWorker = groupConfig->MinTabletsPerMoveRecomputationWorker.value_or(
            MinTabletsPerMoveRecomputationWorker),
        .Metric = groupConfig->Metric.empty()
            ? Metric
            : groupConfig->Metric,
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
        "MinRelativeMetricImprovement: %v, Metric: %v, Factors: %v",
        config.MaxMoveActionCount,
        config.NodeDeviationThreshold,
        config.CellDeviationThreshold,
        config.MinRelativeMetricImprovement,
        config.Metric,
        config.Factors);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMetricsCalculatorType,
    (Parameterized)
    (Replica)
);

class TParameterizedReassignSolver
    : public IParameterizedReassignSolver
{
public:
    TParameterizedReassignSolver(
        TTabletCellBundlePtr bundle,
        std::vector<std::string> performanceCountersKeys,
        TParameterizedReassignSolverConfig config,
        TGroupName groupName,
        TTableParameterizedMetricTrackerPtr metricTracker,
        IThreadPoolPtr recomputeThreadPool,
        EMetricsCalculatorType type,
        const TLogger& logger)
        : Bundle_(std::move(bundle))
        , Logger(logger
            .WithTag("BundleName", Bundle_->Name)
            .WithTag("Group", groupName))
        , Config_(std::move(config))
        , GroupName_(std::move(groupName))
        , RecomputeThreadPool_(std::move(recomputeThreadPool))
        , MetricTracker_(std::move(metricTracker))
        , MoveActions_(Config_.BoundedPriorityQueueSize)
        , RecomputeWorkerMoveActions_(BuildRecomputeWorkerMoveActions(Config_.BoundedPriorityQueueSize))
    {
        switch (type) {
            case EMetricsCalculatorType::Parameterized:
                Calculator_ = New<TParameterizedMetricsCalculator>(
                    Config_.Metric,
                    std::move(performanceCountersKeys),
                    Bundle_->PerformanceCountersTableSchema,
                    Logger);
                break;

            case EMetricsCalculatorType::Replica:
                Calculator_ = CreateReplicaMetricsCalculator(
                    Config_.Metric,
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
                if (CurrentMetric_ * Config_.MinRelativeMetricImprovement / std::ssize(Nodes_) >= BestActionInfo_.MetricDiff)
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

                YT_VERIFY(CurrentMetric_ >= 0);
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
            MetricTracker_->AfterMetric.Update(CurrentMetric_);
        }

        return descriptors;
    }

private:
    using TApplyActionCallback = std::function<void(int*)>;

    struct TNodeInfo
    {
        const TNodeAddress Address;
        double Metric = 0;
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
        double Metric = 0;
        i64 FreeCellMemory = 0;
        int Index;
    };

    struct TTabletInfo
    {
        const TTabletPtr Tablet;
        const TTabletId Id;
        const i64 MemorySize;
        const EInMemoryMode InMemoryMode;
        double Metric = 0;
        int CellIndex;
        int TableIndex;
        int NodeIndex;
    };

    struct TMoveActionInfo
    {
        TTabletCellInfo* SourceCell;
        TTabletCellInfo* DestinationCell;
        TTabletInfo* Tablet;

        double MetricDiff = 0;
    };

    const TTabletCellBundlePtr Bundle_;
    const TLogger Logger;
    const TParameterizedReassignSolverConfig Config_;
    const TGroupName GroupName_;
    const IThreadPoolPtr RecomputeThreadPool_;
    TTableParameterizedMetricTrackerPtr MetricTracker_;
    TParameterizedMetricsCalculatorPtr Calculator_;

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

    std::vector<std::vector<double>> TableByNodeMetric_;
    std::vector<std::vector<double>> TableByCellMetric_;
    std::vector<double> TableCellFactors_;
    std::vector<double> TableNodeFactors_;

    double CurrentMetric_;
    double CellFactor_ = 1.0;
    double NodeFactor_ = 1.0;

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

        THashMap<TTabletId, double> tabletMetrics;
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

                if (tabletMetric < 0.0) {
                    THROW_ERROR_EXCEPTION("Tablet metric must be nonnegative, got %v", tabletMetric)
                        .With("tablet_metric_value", tabletMetric)
                        .With("tablet_id", tabletId)
                        .With("table_id", tablet->Table->Id)
                        .With("metric_formula", Config_.Metric)
                        .With("group", GroupName_)
                        .With("bundle", Bundle_->Name);
                } else if (tabletMetric <= MinimumAcceptableMetricValue) {
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

        TableByCellMetric_.resize(tableCount, std::vector<double>(std::ssize(cellInfoIndex)));
        TableByNodeMetric_.resize(tableCount, std::vector<double>(std::ssize(nodeInfoIndex)));
        TableCellFactors_.resize(tableCount);
        TableNodeFactors_.resize(tableCount);

        for (const auto& tablet : Tablets_) {
            const auto& nodeAddress = Cells_[tablet.CellIndex].Cell->NodeAddress.value();

            Cells_[tablet.CellIndex].Metric += tablet.Metric * CellFactor_;
            Nodes_[nodeAddress].Metric += tablet.Metric * NodeFactor_;
            TableByCellMetric_[tablet.TableIndex][tablet.CellIndex] += tablet.Metric;
            TableByNodeMetric_[tablet.TableIndex][tablet.NodeIndex] += tablet.Metric;
        }

        CalculateAndApplyTableFactors();

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

        if (MetricTracker_) {
            MetricTracker_->BeforeMetric.Update(CurrentMetric_);
        }

        YT_VERIFY(CurrentMetric_ >= 0.);
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

        auto [minNode, maxNode] = std::minmax_element(
            Nodes_.begin(),
            Nodes_.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.second.Metric < rhs.second.Metric;
            });

        bool byNodeTrigger = maxNode->second.Metric >=
            minNode->second.Metric * (1 + Config_.NodeDeviationThreshold);

        auto [minCell, maxCell] = std::minmax_element(
            Cells_.begin(),
            Cells_.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.Metric < rhs.Metric;
            });

        bool byCellTrigger = maxCell->Metric >=
            minCell->Metric * (1 + Config_.CellDeviationThreshold);

        YT_TLOG_DEBUG_IF(
            Bundle_->Config->EnableVerboseLogging,
            "Arguments for checking whether parameterized balancing should trigger have been calculated")
            .WithFormat("MinNodeMetric", "%e", minNode->second.Metric)
            .WithFormat("MaxNodeMetric", "%e", maxNode->second.Metric)
            .WithFormat("MinCellMetric", "%e", minCell->Metric)
            .WithFormat("MaxCellMetric", "%e", maxCell->Metric)
            .With("NodeDeviationThreshold", Config_.NodeDeviationThreshold)
            .With("CellDeviationThreshold", Config_.CellDeviationThreshold);

        return byNodeTrigger || byCellTrigger;
    }

    double CalculateTotalBundleMetric() const
    {
        double cellMetric = 0;
        for (const auto& item : Cells_) {
            cellMetric += Sqr(item.Metric);
        }

        double nodeMetric = 0;
        for (const auto& item : Nodes_) {
            nodeMetric += Sqr(item.second.Metric);
        }

        double tableCellMetric = 0;
        for (const auto& tableMetrics : TableByCellMetric_) {
            for (auto metric : tableMetrics) {
                tableCellMetric += Sqr(metric);
            }
        }
        tableCellMetric *= TableNormalizingCoefficient_;

        double tableNodeMetric = 0;
        for (const auto& tableMetrics : TableByNodeMetric_) {
            for (auto metric : tableMetrics) {
                tableNodeMetric += Sqr(metric);
            }
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
        for (int tableIndex = 0; tableIndex < std::ssize(TableByCellMetric_); ++tableIndex) {
            double tableMetric = std::accumulate(
                TableByCellMetric_[tableIndex].begin(),
                TableByCellMetric_[tableIndex].end(),
                0.0,
                [] (double x, const auto& metric) {
                    return x + metric;
                });
            double cellCount = std::ssize(TableByCellMetric_.back());
            double nodeCount = std::ssize(TableByNodeMetric_.back());

            TableCellFactors_[tableIndex] = cellCount / tableMetric;
            TableNodeFactors_[tableIndex] = nodeCount / tableMetric;

            //  Per-cell dispersion is less important than per-node so we decrease its absolute value.
            TableCellFactors_[tableIndex] *= nodeCount / cellCount;

            TableCellFactors_[tableIndex] *= Config_.Factors->TableCell.value();
            TableNodeFactors_[tableIndex] *= Config_.Factors->TableNode.value();

            YT_TLOG_DEBUG_IF(Bundle_->Config->EnableVerboseLogging, "Calculated per-table factors for cells and nodes")
                .With("TableId", TableIds_[tableIndex])
                .With("TableCellFactor", TableCellFactors_[tableIndex])
                .With("TableNodeFactor", TableNodeFactors_[tableIndex]);

            for (auto& value : TableByCellMetric_[tableIndex]) {
                value *= TableCellFactors_[tableIndex];
            }
            for (auto& value : TableByNodeMetric_[tableIndex]) {
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

        double totalMetric = std::accumulate(
            Tablets_.begin(),
            Tablets_.end(),
            0.0,
            [] (double x, const auto &item) {
                return x + item.Metric;
            });

        CellFactor_ = cellCount / totalMetric;
        NodeFactor_ = nodeCount / totalMetric;

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

    //! Generates an action moving |tablet| to |cell|. Returns |false| if it can be proven
    //! that all further actions will be pruned and the iteration can be stopped.
    Y_FORCE_INLINE bool TryMoveTablet(
        TTabletInfo* tablet,
        TTabletCellInfo* cell,
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

        auto sourceNodeMetric = sourceNode->Metric;
        auto destinationNodeMetric = destinationNode->Metric;

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

        if (sourceNode == destinationNode && sourceCell->Metric < cell->Metric) {
            // Moving to larger cell on the same node will not make metric smaller.
            // Let's pretend that we can move to the cell so that we don't try to move it to the same node again.
            return true;
        }

        int tableIndex = tablet->TableIndex;
        double newMetricDiff = 0;

        if (sourceNode != destinationNode) {
            newMetricDiff +=
                (sourceNodeMetric - destinationNodeMetric -
                tablet->Metric * NodeFactor_) *
                NodeFactor_;

            newMetricDiff +=
                (TableByNodeMetric_[tableIndex][sourceNode->Index] -
                    TableByNodeMetric_[tableIndex][destinationNode->Index] -
                    tablet->Metric * TableNodeFactors_[tableIndex]) *
                TableNodeFactors_[tableIndex] * TableNormalizingCoefficient_;
        }

        newMetricDiff +=
            (sourceCell->Metric - tablet->Metric * CellFactor_) *
            CellFactor_;

        newMetricDiff +=
            (TableByCellMetric_[tableIndex][sourceCell->Index] -
                tablet->Metric * TableCellFactors_[tableIndex]) *
            TableCellFactors_[tableIndex] * TableNormalizingCoefficient_;

        if (newMetricDiff * (2.0 * tablet->Metric) < bestDiscardedCost) {
            // Current value of newMetricDiff takes into account the "positive" part
            // (a certain tablet was moved from a certain node&cell) and partly
            // the "negative" part (a certain tablet is moved to a certain node).
            // It overestimates the final newMetricDiff value. If this overestimate
            // is below zero (and even below best discarded cost) then the action
            // can be discarded. Furhermore, all further actions can be discarded
            // as well since nodes are sorted in ascending order.
            return false;
        }

        newMetricDiff -= cell->Metric * CellFactor_;

        newMetricDiff -=
            TableByCellMetric_[tableIndex][cell->Index] *
            TableCellFactors_[tableIndex] * TableNormalizingCoefficient_;

        newMetricDiff *= 2 * tablet->Metric;

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

        if (newMetricDiff > 0.0) {
            moveActions->Insert(
                newMetricDiff,
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

        TableByCellMetric_[BestActionInfo_.Tablet->TableIndex][BestActionInfo_.SourceCell->Index] -=
            BestActionInfo_.Tablet->Metric * TableCellFactors_[BestActionInfo_.Tablet->TableIndex];
        TableByCellMetric_[BestActionInfo_.Tablet->TableIndex][BestActionInfo_.DestinationCell->Index] +=
            BestActionInfo_.Tablet->Metric * TableCellFactors_[BestActionInfo_.Tablet->TableIndex];

        *availableActionCount -= 1;

        if (BestActionInfo_.SourceCell->Node != BestActionInfo_.DestinationCell->Node) {
            BestActionInfo_.Tablet->NodeIndex = BestActionInfo_.DestinationCell->Node->Index;
            BestActionInfo_.SourceCell->Node->Metric -= BestActionInfo_.Tablet->Metric * NodeFactor_;
            BestActionInfo_.DestinationCell->Node->Metric += BestActionInfo_.Tablet->Metric * NodeFactor_;

            TableByNodeMetric_[BestActionInfo_.Tablet->TableIndex][BestActionInfo_.SourceCell->Node->Index] -=
                BestActionInfo_.Tablet->Metric * TableNodeFactors_[BestActionInfo_.Tablet->TableIndex];
            TableByNodeMetric_[BestActionInfo_.Tablet->TableIndex][BestActionInfo_.DestinationCell->Node->Index] +=
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
        auto recomputeInvoker = RecomputeThreadPool_->GetInvoker();
        int threadCount = RecomputeThreadPool_->GetThreadCount();
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
                auto* sourceCell = &Cells_[tablet.CellIndex];

                if (std::find(bannedNodes.begin(), bannedNodes.end(), sourceCell->Node) != bannedNodes.end()) {
                    for (auto cellIndex : SortedCellIndexes_) {
                        if (!TryMoveTablet(&tablet, &Cells_[cellIndex], moveActions)) {
                            break;
                        }
                    }
                } else {
                    for (auto* cell : invalidatedCells) {
                        TryMoveTablet(&tablet, cell, moveActions);
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
                for (auto cellIndex : SortedCellIndexes_) {
                    if (!TryMoveTablet(&tablet, &Cells_[cellIndex], moveActions)) {
                        break;
                    }
                }
            }
        });
    }

    bool TryFindBestAction()
    {
        std::sort(SortedCellIndexes_.begin(), SortedCellIndexes_.end(), [&] (auto lhs, auto rhs) {
            return Cells_[lhs].Node->Metric < Cells_[rhs].Node->Metric;
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

IParameterizedReassignSolverPtr CreateParameterizedReassignSolver(
    TTabletCellBundlePtr bundle,
    std::vector<std::string> performanceCountersKeys,
    TParameterizedReassignSolverConfig config,
    TGroupName groupName,
    TTableParameterizedMetricTrackerPtr metricTracker,
    IThreadPoolPtr recomputeThreadPool,
    const NLogging::TLogger& logger)
{
    return New<TParameterizedReassignSolver>(
        std::move(bundle),
        std::move(performanceCountersKeys),
        std::move(config),
        std::move(groupName),
        std::move(metricTracker),
        std::move(recomputeThreadPool),
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
    return New<TParameterizedReassignSolver>(
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
