#include "parameterized_balancing_helpers.h"

#include "balancing_helpers.h"
#include "bounded_priority_queue.h"
#include "config.h"
#include "table.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_value.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/orm/library/query/expression_evaluator.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

namespace NYT::NTabletBalancer {

using namespace NCypressClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const std::vector<TString> ParameterizedBalancingAttributes = {
    "/statistics",
    "/performance_counters"
};

constexpr int MaxVerboseLogMessagesPerIteration = 2000;
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
    auto type = TypeFromId(tableId);
    return type == EObjectType::Table || type == EObjectType::ReplicatedTable;
}

TParameterizedReassignSolverConfig TParameterizedReassignSolverConfig::MergeWith(
    const TParameterizedBalancingConfigPtr& groupConfig,
    std::optional<int> maxMoveActionHardLimit) const
{
    auto maxMoveActionCount = groupConfig->MaxActionCount.value_or(MaxMoveActionCount);
    if (maxMoveActionHardLimit) {
        maxMoveActionCount = std::min(maxMoveActionCount, *maxMoveActionHardLimit);
    }

    // Temporary. Verify that if uniform is enable then factors was changed properly.
    auto factors = Factors->MergeWith(groupConfig->Factors);
    YT_VERIFY(!groupConfig->EnableUniform.value_or(false) ||
        factors->TableCell > 0.0 && factors->TableNode > 0.0);

    return TParameterizedReassignSolverConfig{
        .MaxMoveActionCount = maxMoveActionCount,
        .BoundedPriorityQueueSize = groupConfig->BoundedPriorityQueueSize.value_or(BoundedPriorityQueueSize),
        .NodeDeviationThreshold = groupConfig->NodeDeviationThreshold.value_or(NodeDeviationThreshold),
        .CellDeviationThreshold = groupConfig->CellDeviationThreshold.value_or(CellDeviationThreshold),
        .MinRelativeMetricImprovement = groupConfig->MinRelativeMetricImprovement.value_or(
            MinRelativeMetricImprovement),
        .Metric = groupConfig->Metric.empty()
            ? Metric
            : groupConfig->Metric,
        .Factors = std::move(factors),
    };
}

TParameterizedResharderConfig TParameterizedResharderConfig::MergeWith(
    const TParameterizedBalancingConfigPtr& groupConfig) const
{
    return TParameterizedResharderConfig{
        .Metric = groupConfig->Metric.empty()
            ? Metric
            : groupConfig->Metric
    };
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

void FormatValue(TStringBuilderBase* builder, const TComponentFactorConfigPtr& config, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "CellFactor: %v, NodeFactor: %v, TableCellFactor: %v, TableNodeFactor: %v",
        config->Cell,
        config->Node,
        config->TableCell,
        config->TableNode);
}

void FormatValue(TStringBuilderBase* builder, const TParameterizedResharderConfig& config, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "EnableReshardByDefault: %v, Metric: %v",
        config.EnableReshardByDefault,
        config.Metric);
}

////////////////////////////////////////////////////////////////////////////////

class TParameterizedMetricsCalculator
{
public:
    TParameterizedMetricsCalculator(
        TString metric,
        std::vector<TString> performanceCountersKeys,
        TTableSchemaPtr performanceCountersTableSchema)
    : PerformanceCountersKeys_(std::move(performanceCountersKeys))
    , PerformanceCountersTableSchema_(std::move(performanceCountersTableSchema))
    , Metric_(std::move(metric))
    , Evaluator_(NOrm::NQuery::CreateOrmExpressionEvaluator(
        Metric_,
        ParameterizedBalancingAttributes))
    { }

protected:
    const std::vector<TString> PerformanceCountersKeys_;
    const TTableSchemaPtr PerformanceCountersTableSchema_;
    const TString Metric_;
    NOrm::NQuery::IExpressionEvaluatorPtr Evaluator_;

    double GetTabletMetric(const TTabletPtr& tablet) const
    {
        auto value = Evaluator_->Evaluate({
            ConvertToYsonString(tablet->Statistics.OriginalNode),
            tablet->GetPerformanceCountersYson(PerformanceCountersKeys_, PerformanceCountersTableSchema_)
        }).ValueOrThrow();

        switch (value.Type) {
            case EValueType::Double:
                return value.Data.Double;

            case EValueType::Int64:
                return value.Data.Int64;

            case EValueType::Uint64:
                return value.Data.Uint64;

            default:
                THROW_ERROR_EXCEPTION(
                    "Tablet metric value type is not numerical: got %Qlv",
                    value.Type)
                    << TErrorAttribute("metric_formula", Metric_)
                    << TErrorAttribute("tablet_id", tablet->Id)
                    << TErrorAttribute("table_id", tablet->Table->Id);
        }
    }
};

class TParameterizedReassignSolver
    : public IParameterizedReassignSolver
    , public TParameterizedMetricsCalculator
{
public:
    TParameterizedReassignSolver(
        TTabletCellBundlePtr bundle,
        std::vector<TString> performanceCountersKeys,
        TTableSchemaPtr performanceCountersTableSchema,
        TParameterizedReassignSolverConfig config,
        TGroupName groupName,
        TTableParameterizedMetricTrackerPtr metricTracker,
        const TLogger& logger);

    std::vector<TMoveDescriptor> BuildActionDescriptors() override;

private:
    using TApplyActionCallback = std::function<void(int*)>;

    struct TNodeInfo
    {
        const TNodeAddress Address;
        double Metric = 0;
        i64 FreeNodeMemory = 0;
        int Index;
        bool Overloaded = false;
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
    TTableParameterizedMetricTrackerPtr MetricTracker_;

    std::vector<TTabletInfo> Tablets_;
    std::vector<TTabletCellInfo> Cells_;
    std::vector<TTableId> TableIds_;
    std::vector<int> SortedCellIndexes_;
    THashMap<TNodeAddress, TNodeInfo> Nodes_;

    TBoundedPriorityQueue<TMoveActionInfo> MoveActions_;
    TMoveActionInfo BestActionInfo_;

    double TableNormalizingCoefficient_ = 1.0;


    std::vector<std::vector<double>> TableByNodeMetric_;
    std::vector<std::vector<double>> TableByCellMetric_;
    std::vector<double> TableCellFactors_;
    std::vector<double> TableNodeFactors_;

    double CurrentMetric_;
    double CellFactor_ = 1.0;
    double NodeFactor_ = 1.0;
    int LogMessageCount_ = 0;

    int FullRecomputeAttempts_ = 0;
    int PartialRecomputeAttempts_= 0;
    int MaxCellPerNodeCount_ = 0;

    void Initialize();

    double CalculateTotalBundleMetric() const;
    void CalculateModifyingFactors();
    void CalculateAndApplyTableFactors();

    bool TryMoveTablet(
        TTabletInfo* tablet,
        TTabletCellInfo* cell);

    void ApplyBestAction(int* availableActionCount);
    void RecomputeInvalidatedActions();
    void RecomputeAllActions();
    bool TryFindBestAction();
    bool ShouldTrigger() const;

    bool CheckMoveFollowsMemoryLimits(
        const TTabletInfo* tablet,
        const TTabletCellInfo* sourceCell,
        const TTabletCellInfo* destinationCell) const;

    void CalculateMemory(const THashMap<TTabletCellId, int>& cellInfoIndex);
};

TParameterizedReassignSolver::TParameterizedReassignSolver(
    TTabletCellBundlePtr bundle,
    std::vector<TString> performanceCountersKeys,
    TTableSchemaPtr performanceCountersTableSchema,
    TParameterizedReassignSolverConfig config,
    TGroupName groupName,
    TTableParameterizedMetricTrackerPtr metricTracker,
    const TLogger& logger)
    : TParameterizedMetricsCalculator(
        config.Metric,
        std::move(performanceCountersKeys),
        std::move(performanceCountersTableSchema))
    , Bundle_(std::move(bundle))
    , Logger(logger
        .WithTag("BundleName: %v", Bundle_->Name)
        .WithTag("Group: %v", groupName))
    , Config_(std::move(config))
    , GroupName_(std::move(groupName))
    , MetricTracker_(std::move(metricTracker))
    , MoveActions_(Config_.BoundedPriorityQueueSize)
{ }

void TParameterizedReassignSolver::Initialize()
{
    auto cells = Bundle_->GetAliveCells();

    if (cells.empty()) {
        // Therefore nodes list will be empty and balancing will not be triggered.
        YT_LOG_WARNING("There are no alive cells");
        return;
    }

    THashMap<TTabletCellId, int> cellInfoIndex;
    THashMap<TTableId, int> tableInfoIndex;
    THashMap<TString, int> nodeInfoIndex;

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

            auto tabletMetric = GetTabletMetric(tablet);

            if (tabletMetric < 0.0) {
                THROW_ERROR_EXCEPTION("Tablet metric must be nonnegative, got %v", tabletMetric)
                    << TErrorAttribute("tablet_metric_value", tabletMetric)
                    << TErrorAttribute("tablet_id", tabletId)
                    << TErrorAttribute("table_id", tablet->Table->Id)
                    << TErrorAttribute("metric_formula", Config_.Metric)
                    << TErrorAttribute("group", GroupName_)
                    << TErrorAttribute("bundle", Bundle_->Name);
            } else if (tabletMetric <= MinimumAcceptableMetricValue) {
                YT_LOG_DEBUG_IF(
                    Bundle_->Config->EnableVerboseLogging,
                    "Skip tablet since it has metric less than %v"
                    "(TabletId: %v, TableId: %v)",
                    MinimumAcceptableMetricValue,
                    tabletId,
                    tablet->Table->Id);
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
        // Therefore there are no tables to balance.
        return;
    }

    TableNormalizingCoefficient_ = 1.0 / tableCount;

    CalculateModifyingFactors();

    TableByCellMetric_.resize(tableCount, std::vector<double>(std::ssize(cellInfoIndex)));
    TableByNodeMetric_.resize(tableCount, std::vector<double>(std::ssize(nodeInfoIndex)));
    TableCellFactors_.resize(tableCount);
    TableNodeFactors_.resize(tableCount);

    for (const auto& tablet : Tablets_) {
        const auto& nodeAdress = Cells_[tablet.CellIndex].Cell->NodeAddress.value();

        Cells_[tablet.CellIndex].Metric += tablet.Metric * CellFactor_;
        Nodes_[nodeAdress].Metric += tablet.Metric * NodeFactor_;
        TableByCellMetric_[tablet.TableIndex][tablet.CellIndex] += tablet.Metric;
        TableByNodeMetric_[tablet.TableIndex][tablet.NodeIndex] += tablet.Metric;
    }

    CalculateAndApplyTableFactors();

    for (int index = 0; index < std::ssize(Cells_); ++index) {
        SortedCellIndexes_.emplace_back(index);
    }

    if (Bundle_->Config->EnableVerboseLogging) {
        for (const auto& [nodeAddress, nodeInfo] : Nodes_) {
            YT_LOG_DEBUG("Calculated node metric (NodeAddress: %v, NodeMetric: %v)",
                nodeAddress,
                nodeInfo.Metric);
        }
    }

    CurrentMetric_ = CalculateTotalBundleMetric();

    if (MetricTracker_) {
        MetricTracker_->BeforeMetric.Update(CurrentMetric_);
    }

    YT_VERIFY(CurrentMetric_ >= 0.);
}

void TParameterizedReassignSolver::CalculateMemory(const THashMap<TTabletCellId, int>& cellInfoIndex)
{
    if (Bundle_->NodeStatistics.empty()) {
        YT_LOG_DEBUG("Don't calculate memory because there are no in-memory tables with parameterized balancing");
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
            YT_LOG_DEBUG("There are no alive cells on the node (Node: %v)",
                address);
            continue;
        }

        i64 actualUsage = GetOrCrash(actualMemoryUsage, address);
        i64 free = statistics.MemoryLimit - statistics.MemoryUsed;
        i64 unaccountedUsage = 0;
        auto count = GetOrCrash(cellCount, address);

        if (actualUsage > statistics.MemoryUsed) {
            YT_LOG_DEBUG("Using total cell memory as node memory usage (Node: %v, Used: %v, Sum: %v, Limit: %v)",
                address,
                statistics.MemoryUsed,
                actualUsage,
                statistics.MemoryLimit);
            if (statistics.MemoryLimit < actualUsage) {
                YT_LOG_WARNING("Node memory usage exceeds memory limit (MemoryLimit: %v, MemoryUsage: %v, "
                    "ActualMemoryUsage: %v, Node: %v, CellCount: %v, TabletSlotCount: %v)",
                    statistics.MemoryLimit,
                    statistics.MemoryUsed,
                    actualUsage,
                    address,
                    count,
                    statistics.TabletSlotCount);
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

        EmplaceOrCrash(cellMemoryLimit, address, cellLimit);
    }

    for (const auto& [cell, usage] : cellMemoryUsage) {
        auto limit = GetOrCrash(cellMemoryLimit, *cell->NodeAddress);
        Cells_[GetOrCrash(cellInfoIndex, cell->Id)].FreeCellMemory = limit - usage;
    }
}

bool TParameterizedReassignSolver::ShouldTrigger() const
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

    YT_LOG_DEBUG_IF(
        Bundle_->Config->EnableVerboseLogging,
        "Arguments for checking whether parameterized balancing should trigger have been calculated "
        "(MinNodeMetric: %v, MaxNodeMetric: %v, MinCellMetric: %v, MaxCellMetric: %v, "
        "NodeDeviationThreshold: %v, CellDeviationThreshold: %v)",
        minNode->second.Metric,
        maxNode->second.Metric,
        minCell->Metric,
        maxCell->Metric,
        Config_.NodeDeviationThreshold,
        Config_.CellDeviationThreshold);

    return byNodeTrigger || byCellTrigger;
}

double TParameterizedReassignSolver::CalculateTotalBundleMetric() const
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

    YT_LOG_DEBUG(
        "Calculated total metrics (CellMetric: %v, NodeMetric: %v, "
        "TableCellMetric: %v, TableNodeMetric: %v)",
        cellMetric,
        nodeMetric,
        tableCellMetric,
        tableNodeMetric);

    return cellMetric + nodeMetric + tableCellMetric + tableNodeMetric;
}

void TParameterizedReassignSolver::CalculateAndApplyTableFactors()
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

        YT_LOG_DEBUG_IF(
            Bundle_->Config->EnableVerboseLogging,
            "Calculated per-table factors for cells and nodes "
            "(TableId: %v, TableCellFactor: %v, TableNodeFactor: %v)",
            TableIds_[tableIndex],
            TableCellFactors_[tableIndex],
            TableNodeFactors_[tableIndex]);

        for (auto& value : TableByCellMetric_[tableIndex]) {
            value *= TableCellFactors_[tableIndex];
        }
        for (auto& value : TableByNodeMetric_[tableIndex]) {
            value *= TableNodeFactors_[tableIndex];
        }
    }
}

void TParameterizedReassignSolver::CalculateModifyingFactors()
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

    YT_LOG_DEBUG(
        "Calculated modifying factors (CellFactor: %v, NodeFactor: %v)",
        CellFactor_,
        NodeFactor_);
}

bool TParameterizedReassignSolver::CheckMoveFollowsMemoryLimits(
    const TTabletInfo* tablet,
    const TTabletCellInfo* sourceCell,
    const TTabletCellInfo* destinationCell) const
{
    if (tablet->InMemoryMode == EInMemoryMode::None) {
        return true;
    }

    auto size = tablet->MemorySize;
    if (destinationCell->FreeCellMemory < size) {
        return false;
    }

    return destinationCell->Node == sourceCell->Node ||
        destinationCell->Node->FreeNodeMemory >= size && !destinationCell->Node->Overloaded;
}

//! Generates an action moving |tablet| to |cell|. Returns |false| if it can be proven that all further actions will be pruned and the iteration can be stopped.
bool TParameterizedReassignSolver::TryMoveTablet(
    TTabletInfo* tablet,
    TTabletCellInfo* cell)
{
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
        YT_LOG_DEBUG_IF(
            Bundle_->Config->EnableVerboseLogging && LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
            "Cannot move tablet (TabletId: %v, CellId: %v, SourceNode: %v, DestinationNode: %v)",
            tablet->Id,
            cell->Id,
            sourceNode->Address,
            destinationNode->Address);
        return true;
    }

    if (sourceNode == destinationNode && sourceCell->Metric < cell->Metric) {
        // Moving to larger cell on the same node will not make metric smaller.
        // Let's pretend that we can move to the cell so that we don’t try to move it to the same node again.
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

    if (newMetricDiff * (2.0 * tablet->Metric) < MoveActions_.GetBestDiscardedCost()) {
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

    YT_LOG_DEBUG_IF(
        Bundle_->Config->EnableVerboseLogging && LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
        "Trying to move tablet to another cell (TabletId: %v, CellId: %v, CurrentMetric: %v, "
        "NewMetricDiff: %v, TabletMetric: %v, SourceCellMetric: %v, DestinationCellMetric: %v, "
        "SourceNodeMetric: %v, DestinationNodeMetric: %v)",
        tablet->Id,
        cell->Id,
        CurrentMetric_,
        newMetricDiff,
        tablet->Metric,
        sourceCell->Metric,
        cell->Metric,
        sourceNode->Metric,
        destinationNode->Metric);

    if (newMetricDiff > 0.0) {
        MoveActions_.Insert(
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

void TParameterizedReassignSolver::ApplyBestAction(int* availableActionCount)
{
    MoveActions_.Invalidate(
        [=, this] (const std::pair<double, TMoveActionInfo>& moveActionInfo) {
            std::array bannedNodes = {
                moveActionInfo.second.SourceCell->Node,
                moveActionInfo.second.DestinationCell->Node,
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
    } else {
        YT_LOG_WARNING("The best action is between cells on the same node "
            "(Node: %v, TabletId: %v)",
            BestActionInfo_.SourceCell->Node->Address,
            BestActionInfo_.Tablet->Id);
    }

    YT_LOG_DEBUG("Applying best action: moving tablet to another cell "
        "(TabletId: %v, SourceCellId: %v, DestinationCellId: %v, "
        "SourceNode: %v, DestinationNode: %v)",
        BestActionInfo_.Tablet->Id,
        BestActionInfo_.SourceCell->Id,
        BestActionInfo_.DestinationCell->Id,
        BestActionInfo_.SourceCell->Node->Address,
        BestActionInfo_.DestinationCell->Node->Address);

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

void TParameterizedReassignSolver::RecomputeInvalidatedActions()
{
    std::array bannedNodes = {
        BestActionInfo_.SourceCell->Node,
        BestActionInfo_.DestinationCell->Node,
    };

    std::vector<TTabletCellInfo*> invalidatedCells;
    invalidatedCells.reserve(MaxCellPerNodeCount_);
    for (auto& cell : Cells_) {
        if (cell.Node == BestActionInfo_.SourceCell->Node || cell.Node == BestActionInfo_.DestinationCell->Node) {
            invalidatedCells.push_back(&cell);
        }
    }

    for (auto& tablet : Tablets_) {
        auto* sourceCell = &Cells_[tablet.CellIndex];

        if (std::find(bannedNodes.begin(), bannedNodes.end(), sourceCell->Node) != bannedNodes.end()) {
            for (auto cellIndex : SortedCellIndexes_) {
                if (!TryMoveTablet(&tablet, &Cells_[cellIndex])) {
                    break;
                }
            }
        } else {
            for (auto* cell : invalidatedCells) {
                TryMoveTablet(&tablet, cell);
            }
        }
    }
}

void TParameterizedReassignSolver::RecomputeAllActions()
{
    MoveActions_.Reset();
    for (auto& tablet : Tablets_) {
        for (auto cellIndex : SortedCellIndexes_) {
            if (!TryMoveTablet(&tablet, &Cells_[cellIndex])) {
                break;
            }
        }
    }
}

bool TParameterizedReassignSolver::TryFindBestAction()
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

    BestActionInfo_ = MoveActions_.ExtractMax()->second;

    return true;
}

std::vector<TMoveDescriptor> TParameterizedReassignSolver::BuildActionDescriptors()
{
    YT_LOG_DEBUG("Reporting parameterized balancing config (Config: %v)",
        Config_);

    Initialize();

    if (!ShouldTrigger()) {
        YT_LOG_DEBUG("Parameterized balancing was not triggered "
            "(NodeDeviationThreshold: %v, CellDeviationThreshold: %v)",
            Config_.NodeDeviationThreshold,
            Config_.CellDeviationThreshold);
        return {};
    }

    int availableActionCount = Config_.MaxMoveActionCount;
    while (availableActionCount > 0) {
        LogMessageCount_ = 0;
        if (TryFindBestAction()) {
            if (CurrentMetric_ * Config_.MinRelativeMetricImprovement / std::ssize(Nodes_) >= BestActionInfo_.MetricDiff)
            {
                YT_LOG_DEBUG(
                    "Metric-improving action is not better enough (CurrentMetric: %v, MetricAfterAction: %v)",
                    CurrentMetric_,
                    BestActionInfo_.MetricDiff);
                break;
            }

            ApplyBestAction(&availableActionCount);

            YT_LOG_DEBUG(
                "Total parameterized metric changed (Old: %v, Diff: %v)",
                CurrentMetric_,
                BestActionInfo_.MetricDiff);
            CurrentMetric_ -= BestActionInfo_.MetricDiff;

            YT_VERIFY(CurrentMetric_ >= 0);
        } else {
            YT_LOG_DEBUG("Metric-improving action was not found");
            break;
        }
    }

    YT_LOG_INFO("Found all move actions (FullRecomputeAttempts: %v, PartialRecomputeAttempts: %v)",
        FullRecomputeAttempts_,
        PartialRecomputeAttempts_);

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
        YT_LOG_ALERT(
            "Too many actions created during parametrized balancing (DescriptorCount: %v, MoveActionLimit: %v)",
            std::ssize(descriptors),
            Config_.MaxMoveActionCount);
        return {};
    }

    YT_LOG_DEBUG(
        "Scheduled move actions for parameterized tablets balancing (ActionCount: %v, MoveActionLimit: %v)",
        std::ssize(descriptors),
        Config_.MaxMoveActionCount);

    if (MetricTracker_) {
        MetricTracker_->AfterMetric.Update(CurrentMetric_);
    }

    return descriptors;
}

////////////////////////////////////////////////////////////////////////////////

class TParameterizedResharder
    : public IParameterizedResharder
    , public TParameterizedMetricsCalculator
{
public:
    TParameterizedResharder(
        TTabletCellBundlePtr bundle,
        std::vector<TString> performanceCountersKeys,
        TTableSchemaPtr performanceCountersTableSchema,
        TParameterizedResharderConfig config,
        TGroupName groupName,
        const TLogger& logger);

    std::vector<TReshardDescriptor> BuildTableActionDescriptors(const TTablePtr& table) override;

private:
    struct TTableStatistics
    {
        int DesiredTabletCount;

        i64 MinTabletSize;
        i64 DesiredTabletSize;
        i64 MaxTabletSize;

        i64 TableSize;

        double MinTabletMetric;
        double DesiredTabletMetric;
        double MaxTabletMetric;

        double TableMetric;

        std::vector<i64> TabletSizes;
        std::vector<double> TabletMetrics;

        bool IsTooSmallBySomeMeasure(
            i64 tabletSize,
            double tabletMetric) const
        {
            return tabletSize <= MaxTabletSize && tabletMetric <= MaxTabletMetric &&
                (tabletSize < MinTabletSize || tabletMetric < MinTabletMetric);
        }

        bool IsLessThanDesiredByEachMeasure(
            i64 tabletSize,
            double tabletMetric) const
        {
            return tabletSize < DesiredTabletSize && tabletMetric < DesiredTabletMetric;
        }
    };

    const TTabletCellBundlePtr Bundle_;
    const TLogger Logger;
    const TParameterizedResharderConfig Config_;
    const TGroupName GroupName_;

    mutable int LogMessageCount_ = 0;

    bool IsParameterizedReshardEnabled(const TTablePtr& table) const;

    TTableStatistics GetTableStatistics(const TTablePtr& table, int desiredTabletCount) const;

    std::optional<TReshardDescriptor> TryMakeTabletFit(
        const TTablePtr& table,
        int tabletIndex,
        THashSet<int>* touchedTablets,
        const TTableStatistics& statistics);

    TReshardDescriptor SplitTablet(
        const TTablePtr& table,
        int tabletIndex,
        THashSet<int>* touchedTablets,
        const TTableStatistics& statistics);

    std::optional<TReshardDescriptor> MergeTablets(
        const TTablePtr& table,
        int tabletIndex,
        THashSet<int>* touchedTablets,
        const TTableStatistics& statistics);

    bool AreMoreTabletsNeeded(
        const TTableStatistics& statistics,
        i64 tabletSize,
        double tabletMetric) const;

    bool IsPossibleToAddTablet(
        const TTableStatistics& statistics,
        i64 tabletSize,
        double tabletMetric,
        int nextTabletIndex) const;

    void SortTabletActionsByUsefulness(std::vector<TReshardDescriptor>* actions) const;
    void TrimTabletActions(int currentTabletCount, std::vector<TReshardDescriptor>* actions) const;
};

TParameterizedResharder::TParameterizedResharder(
    TTabletCellBundlePtr bundle,
    std::vector<TString> performanceCountersKeys,
    TTableSchemaPtr performanceCountersTableSchema,
    TParameterizedResharderConfig config,
    TGroupName groupName,
    const TLogger& logger)
    : TParameterizedMetricsCalculator(
        config.Metric,
        std::move(performanceCountersKeys),
        std::move(performanceCountersTableSchema))
    , Bundle_(std::move(bundle))
    , Logger(logger
        .WithTag("BundleName: %v", Bundle_->Name)
        .WithTag("Group: %v", groupName))
    , Config_(std::move(config))
    , GroupName_(std::move(groupName))
{
    YT_LOG_DEBUG("Reporting parameterized resharder config (Config: %v)",
        Config_);
}

void TParameterizedResharder::SortTabletActionsByUsefulness(std::vector<TReshardDescriptor>* actions) const
{
    std::sort(
        actions->begin(),
        actions->end(),
        [] (auto lhs, auto rhs) {
            if (lhs.TabletCount == 1 || rhs.TabletCount == 1) {
                return lhs.TabletCount < rhs.TabletCount;
            }
            return lhs.TabletCount > rhs.TabletCount;
    });
}

void TParameterizedResharder::TrimTabletActions(
    int currentTabletCount,
    std::vector<TReshardDescriptor>* actions) const
{
    // We calculate the tablet count in the worst case, when all split actions are executed before merge actions.
    for (int actionIndex = 0; actionIndex < std::ssize(*actions); ++actionIndex) {
        const auto& action = actions->at(actionIndex);
        if (action.TabletCount == 1) {
            continue;
        }

        currentTabletCount += action.TabletCount - 1;
        if (currentTabletCount > NTabletClient::MaxTabletCount) {
            actions->resize(actionIndex);
            return;
        }
    }
}

std::vector<TReshardDescriptor> TParameterizedResharder::BuildTableActionDescriptors(const TTablePtr& table)
{
    LogMessageCount_ = 0;

    if (!IsParameterizedReshardEnabled(table)) {
        YT_LOG_DEBUG_IF(
            (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
            LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
            "Parameterized balancing via reshard is not enabled (TableId: %v)",
            table->Id);
        return {};
    }

    YT_VERIFY(table->TableConfig->DesiredTabletCount.has_value());
    auto desiredTabletCount = *table->TableConfig->DesiredTabletCount;

    if (desiredTabletCount <= 0) {
        YT_LOG_WARNING("Table desired tablet count is not positive "
            "(TableId: %v, TablePath: %v, DesiredTabletCount: %v)",
            table->Id,
            table->Path,
            desiredTabletCount);
        return {};
    }

    auto statistics = GetTableStatistics(table, desiredTabletCount);
    YT_VERIFY(statistics.DesiredTabletMetric > 0);
    std::vector<TReshardDescriptor> actions;
    THashSet<int> touchedTabletIndexes;

    int tabletCount = std::ssize(table->Tablets);
    for (int tabletIndex = 0; tabletIndex < std::ssize(table->Tablets); ++tabletIndex) {
        if (touchedTabletIndexes.contains(tabletIndex)) {
            continue;
        }

        auto action = TryMakeTabletFit(table, tabletIndex, &touchedTabletIndexes, statistics);
        if (action) {
            actions.push_back(*action);
            tabletCount += action->TabletCount - std::ssize(action->Tablets);
        }
    }

    YT_LOG_DEBUG_UNLESS(actions.empty(),
        "Parameterized reshard action creation requested "
        "(TabletCount: %v, NewTabletCount: %v, DesiredTabletCount: %v)",
        std::ssize(table->Tablets),
        tabletCount,
        desiredTabletCount);

    SortTabletActionsByUsefulness(&actions);
    TrimTabletActions(std::ssize(table->Tablets), &actions);

    return actions;
}

std::optional<TReshardDescriptor> TParameterizedResharder::TryMakeTabletFit(
    const TTablePtr& table,
    int tabletIndex,
    THashSet<int>* touchedTabletIndexes,
    const TTableStatistics& statistics)
{
    auto tabletMetric = statistics.TabletMetrics[tabletIndex];
    auto tabletSize = statistics.TabletSizes[tabletIndex];

    // Tablet is too large by at least one of the metrics.
    if (tabletMetric > statistics.MaxTabletMetric ||
        tabletSize > statistics.MaxTabletSize)
    {
        if (tabletSize == 0) {
            // Should not happen othen.
            YT_LOG_WARNING_IF(
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "Trying to split an empty tablet. Skip it "
                "(TableId: %v, TabletId: %v, TabletMetric: %v, TableSize: %v, DesiredTabletSize: %v, MaxTabletSize: %v)",
                table->Id,
                table->Tablets[tabletIndex]->Id,
                tabletMetric,
                statistics.TableSize,
                statistics.DesiredTabletSize,
                statistics.MaxTabletSize);
            return std::nullopt;
        }

        return SplitTablet(table, tabletIndex, touchedTabletIndexes, statistics);
    }

    // Tablet is just right.
    if (tabletMetric >= statistics.MinTabletMetric &&
        tabletSize >= statistics.MinTabletSize)
    {
        YT_LOG_DEBUG_IF(
            (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
            LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
            "Tablet is just right (TabletId: %v, TabletMetric: %v, TabletSize: %v)",
            table->Tablets[tabletIndex]->Id,
            tabletMetric,
            tabletSize);
        return std::nullopt;
    }

    return MergeTablets(table, tabletIndex, touchedTabletIndexes, statistics);
}

TReshardDescriptor TParameterizedResharder::SplitTablet(
    const TTablePtr& table,
    int tabletIndex,
    THashSet<int>* touchedTabletIndexes,
    const TTableStatistics& statistics)
{
    EmplaceOrCrash(*touchedTabletIndexes, tabletIndex);
    auto tabletSize = statistics.TabletSizes[tabletIndex];
    auto tabletMetric = statistics.TabletMetrics[tabletIndex];
    auto tabletId = table->Tablets[tabletIndex]->Id;

    auto tabletCount = static_cast<int>(std::ceil(tabletMetric / statistics.DesiredTabletMetric));
    tabletCount = std::max<i64>({
        DivCeil(tabletSize, statistics.DesiredTabletSize),
        tabletCount,
        1});

    YT_VERIFY(tabletCount > 0);

    auto correlationId = TGuid::Create();
    YT_LOG_DEBUG("Splitting tablet (Tablet: %v, TabletSize: %v, TabletMetric: %v, CorrelationId: %v)",
        tabletId,
        DivCeil<i64>(tabletSize, tabletCount),
        tabletMetric / tabletCount,
        correlationId);

    auto deviation = std::max(
        tabletMetric / statistics.DesiredTabletMetric,
        static_cast<double>(tabletSize) / statistics.DesiredTabletSize);

    return TReshardDescriptor{
        .Tablets = std::vector<TTabletId>{tabletId},
        .TabletCount = tabletCount,
        .DataSize = tabletSize,
        .CorrelationId = correlationId,
        .Priority = std::tuple(/*IsSplit*/ true, -tabletCount, -deviation)
    };
}

std::optional<TReshardDescriptor> TParameterizedResharder::MergeTablets(
    const TTablePtr& table,
    int tabletIndex,
    THashSet<int>* touchedTabletIndexes,
    const TTableStatistics& statistics)
{
    auto enlargedTabletMetric = statistics.TabletMetrics[tabletIndex];
    auto enlargedTabletSize = statistics.TabletSizes[tabletIndex];

    auto leftTabletIndex = tabletIndex;
    auto rightTabletIndex = tabletIndex + 1;

    auto tabletCount = std::ssize(table->Tablets);

    auto deviation = std::min(
        statistics.TabletMetrics[tabletIndex] / statistics.DesiredTabletMetric,
        static_cast<double>(statistics.TabletSizes[tabletIndex]) / statistics.DesiredTabletSize);

    while (AreMoreTabletsNeeded(statistics, enlargedTabletSize, enlargedTabletMetric) &&
        leftTabletIndex > 0 &&
        !touchedTabletIndexes->contains(leftTabletIndex - 1) &&
        IsPossibleToAddTablet(statistics, enlargedTabletSize, enlargedTabletMetric, leftTabletIndex - 1))
    {
        --leftTabletIndex;
        enlargedTabletSize += statistics.TabletSizes[leftTabletIndex];
        enlargedTabletMetric += statistics.TabletMetrics[leftTabletIndex];

        deviation = std::min({
            deviation,
            statistics.TabletMetrics[leftTabletIndex] / statistics.DesiredTabletMetric,
            static_cast<double>(statistics.TabletSizes[leftTabletIndex]) / statistics.DesiredTabletSize});
    }

    while (AreMoreTabletsNeeded(statistics, enlargedTabletSize, enlargedTabletMetric) &&
        rightTabletIndex < tabletCount &&
        !touchedTabletIndexes->contains(rightTabletIndex) &&
        IsPossibleToAddTablet(statistics, enlargedTabletSize, enlargedTabletMetric, rightTabletIndex))
    {
        enlargedTabletSize += statistics.TabletSizes[rightTabletIndex];
        enlargedTabletMetric += statistics.TabletMetrics[rightTabletIndex];
        ++rightTabletIndex;

        deviation = std::min({
            deviation,
            statistics.TabletMetrics[rightTabletIndex] / statistics.DesiredTabletMetric,
            static_cast<double>(statistics.TabletSizes[rightTabletIndex]) / statistics.DesiredTabletSize});
    }

    if (rightTabletIndex - leftTabletIndex == 1) {
        YT_LOG_DEBUG_IF(
            (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
            LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
            "The tablet is too small, but there are no tablets to merge with it "
            "(TabletId: %v, TabletIndex: %v, TabletSize: %v, TabletMetric: %v)",
            table->Tablets[tabletIndex]->Id,
            tabletIndex,
            enlargedTabletSize,
            enlargedTabletMetric);
        return std::nullopt;
    }

    std::vector<TTabletId> tabletsToMerge;
    for (int index = leftTabletIndex; index < rightTabletIndex; ++index) {
        tabletsToMerge.push_back(table->Tablets[index]->Id);
        EmplaceOrCrash(*touchedTabletIndexes, index);
    }

    auto correlationId = TGuid::Create();
    YT_LOG_DEBUG("Merging tablets (Tablets: %v, TabletSize: %v, TabletMetric: %v, CorrelationId: %v)",
        tabletsToMerge,
        enlargedTabletSize,
        enlargedTabletMetric,
        correlationId);

    return TReshardDescriptor{
        .Tablets = std::move(tabletsToMerge),
        .TabletCount = 1,
        .DataSize = enlargedTabletSize,
        .CorrelationId = correlationId,
        .Priority = std::tuple(false, -(rightTabletIndex - leftTabletIndex), deviation)
    };
}

bool TParameterizedResharder::AreMoreTabletsNeeded(
    const TTableStatistics& statistics,
    i64 tabletSize,
    double tabletMetric) const
{
    return statistics.IsTooSmallBySomeMeasure(tabletSize, tabletMetric) ||
        statistics.IsLessThanDesiredByEachMeasure(tabletSize, tabletMetric);
}

bool TParameterizedResharder::IsPossibleToAddTablet(
    const TTableStatistics& statistics,
    i64 tabletSize,
    double tabletMetric,
    int nextTabletIndex) const
{
    return tabletSize + statistics.TabletSizes[nextTabletIndex] <= statistics.MaxTabletSize &&
        tabletMetric + statistics.TabletMetrics[nextTabletIndex] <= statistics.MaxTabletMetric;
}

bool TParameterizedResharder::IsParameterizedReshardEnabled(const TTablePtr& table) const
{
    if (TypeFromId(table->Id) != EObjectType::Table) {
        return false;
    }

    if (table->GetBalancingGroup() != GroupName_) {
        return false;
    }

    if (!table->IsParameterizedReshardBalancingEnabled(Config_.EnableReshardByDefault)) {
        return false;
    }

    return true;
}

TParameterizedResharder::TTableStatistics TParameterizedResharder::GetTableStatistics(
    const TTablePtr& table,
    int desiredTabletCount) const
{
    TTableStatistics statistics{.DesiredTabletCount = desiredTabletCount};

    for (const auto& tablet : table->Tablets) {
        statistics.TabletSizes.push_back(GetTabletBalancingSize(tablet));
        statistics.TableSize += statistics.TabletSizes.back();

        auto tabletMetric = GetTabletMetric(tablet);
        if (tabletMetric < 0.0) {
            THROW_ERROR_EXCEPTION("Tablet metric must be nonnegative, got %v", tabletMetric)
                << TErrorAttribute("tablet_metric_value", tabletMetric)
                << TErrorAttribute("tablet_id", tablet->Id)
                << TErrorAttribute("metric_formula", Config_.Metric);
        }

        statistics.TabletMetrics.push_back(tabletMetric);
        statistics.TableMetric += tabletMetric;

        YT_LOG_DEBUG_IF(
            (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
            LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
            "Reporting tablet statistics (TabletId: %v, Size: %v, Metric: %v, TableId: %v)",
            tablet->Id,
            statistics.TabletSizes.back(),
            tabletMetric,
            table->Id);
    }

    statistics.DesiredTabletSize = statistics.TableSize / statistics.DesiredTabletCount;
    statistics.MinTabletSize = statistics.DesiredTabletSize / 1.9;
    statistics.MaxTabletSize = statistics.DesiredTabletSize * 1.9;

    statistics.DesiredTabletMetric = statistics.TableMetric / statistics.DesiredTabletCount;
    statistics.MinTabletMetric = statistics.DesiredTabletMetric / 1.9;

    if (statistics.TableMetric == 0.0 || statistics.DesiredTabletMetric == 0.0) {
        YT_LOG_DEBUG("Calculated table metric for parameterized balancing via reshard is zero or almost zero "
            "(TableId: %v, TablePath: %v, TableMetric: %v)",
            table->Id,
            table->Path,
            statistics.TableMetric);
        statistics.DesiredTabletMetric = 1;
    }

    statistics.MaxTabletMetric = statistics.DesiredTabletMetric * 1.9;

    YT_LOG_DEBUG_IF(
        Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging,
        "Reporting reshard limits and statistics "
        "(MinTabletSize: %v, DesiredTabletSize: %v, MaxTabletSize: %v, TableSize: %v, "
        "MinTabletMetric: %v, DesiredTabletMetric: %v, MaxTabletMetric: %v, TableMetric: %v, TableId: %v)",
        statistics.MinTabletSize,
        statistics.DesiredTabletSize,
        statistics.MaxTabletSize,
        statistics.TableSize,
        statistics.MinTabletMetric,
        statistics.DesiredTabletMetric,
        statistics.MaxTabletMetric,
        statistics.TableMetric,
        table->Id);

    return statistics;
}

////////////////////////////////////////////////////////////////////////////////

IParameterizedReassignSolverPtr CreateParameterizedReassignSolver(
    TTabletCellBundlePtr bundle,
    std::vector<TString> performanceCountersKeys,
    TTableSchemaPtr performanceCountersTableSchema,
    TParameterizedReassignSolverConfig config,
    TGroupName groupName,
    TTableParameterizedMetricTrackerPtr metricTracker,
    const NLogging::TLogger& logger)
{
    return New<TParameterizedReassignSolver>(
        std::move(bundle),
        std::move(performanceCountersKeys),
        std::move(performanceCountersTableSchema),
        std::move(config),
        std::move(groupName),
        std::move(metricTracker),
        logger);
}

IParameterizedResharderPtr CreateParameterizedResharder(
    TTabletCellBundlePtr bundle,
    std::vector<TString> performanceCountersKeys,
    TTableSchemaPtr performanceCountersTableSchema,
    TParameterizedResharderConfig config,
    TGroupName groupName,
    const NLogging::TLogger& logger)
{
    return New<TParameterizedResharder>(
        std::move(bundle),
        std::move(performanceCountersKeys),
        std::move(performanceCountersTableSchema),
        std::move(config),
        std::move(groupName),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
