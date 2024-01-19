#include "parameterized_balancing_helpers.h"

#include "balancing_helpers.h"
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
#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/orm/library/query/expression_evaluator.h>

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
    const TParameterizedBalancingConfigPtr& groupConfig) const
{
    return TParameterizedReassignSolverConfig{
        .EnableSwaps = groupConfig->EnableSwaps.value_or(EnableSwaps),
        .MaxMoveActionCount = groupConfig->MaxActionCount.value_or(MaxMoveActionCount),
        .NodeDeviationThreshold = groupConfig->NodeDeviationThreshold.value_or(NodeDeviationThreshold),
        .CellDeviationThreshold = groupConfig->CellDeviationThreshold.value_or(CellDeviationThreshold),
        .MinRelativeMetricImprovement = groupConfig->MinRelativeMetricImprovement.value_or(
            MinRelativeMetricImprovement),
        .Metric = groupConfig->Metric.Empty()
            ? Metric
            : groupConfig->Metric
    };
}

TParameterizedResharderConfig TParameterizedResharderConfig::MergeWith(
    const TParameterizedBalancingConfigPtr& groupConfig) const
{
    return TParameterizedResharderConfig{
        .Metric = groupConfig->Metric.Empty()
            ? Metric
            : groupConfig->Metric
    };
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
    , Evaluator_(NOrm::NQuery::CreateExpressionEvaluator(
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
                    << TErrorAttribute("metric_formula", Metric_);
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

    struct TBestAction
    {
        double Metric;
        TApplyActionCallback Callback;
    };

    struct TNodeInfo
    {
        const TNodeAddress Address;
        double Metric = 0;
        i64 FreeNodeMemory = 0;
    };

    struct TTabletCellInfo
    {
        const TTabletCellPtr Cell;
        TNodeInfo* const Node;
        double Metric = 0;
        i64 FreeCellMemory = 0;
    };

    struct TTabletInfo
    {
        const TTabletPtr Tablet;
        double Metric = 0;
        TTabletCellInfo* Cell;
    };

    const TTabletCellBundlePtr Bundle_;
    const TLogger Logger;
    const TParameterizedReassignSolverConfig Config_;
    const TGroupName GroupName_;
    TTableParameterizedMetricTrackerPtr MetricTracker_;

    std::vector<TTabletInfo> Tablets_;
    THashMap<TTabletCellId, TTabletCellInfo> Cells_;
    THashMap<TNodeAddress, TNodeInfo> Nodes_;

    double CurrentMetric_;
    TBestAction BestAction_;
    int LogMessageCount_ = 0;

    void Initialize();

    double CalculateTotalBundleMetric() const;

    void TryMoveTablet(
        TTabletInfo* tablet,
        TTabletCellInfo* cell);

    void TrySwapTablets(
        TTabletInfo* lhsTablet,
        TTabletInfo* rhsTablet);

    bool TryFindBestAction(bool canMakeSwap);
    bool ShouldTrigger() const;

    bool CheckMoveFollowsMemoryLimits(
        const TTabletInfo* tablet,
        const TTabletCellInfo* cell) const;

    bool CheckSwapFollowsMemoryLimits(
        const TTabletInfo* lhsTablet,
        const TTabletInfo* rhsTablet) const;

    void CalculateMemory();
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
{ }

void TParameterizedReassignSolver::Initialize()
{
    auto cells = Bundle_->GetAliveCells();

    for (const auto& cell : cells) {
        auto* nodeInfo = &Nodes_.emplace(*cell->NodeAddress, TNodeInfo{
            .Address = *cell->NodeAddress
        }).first->second;

        auto* cellInfo = &EmplaceOrCrash(Cells_, cell->Id, TTabletCellInfo{
            .Cell = cell,
            .Node = nodeInfo
        })->second;

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
            } else if (tabletMetric == 0.0) {
                YT_LOG_DEBUG_IF(
                    Bundle_->Config->EnableVerboseLogging,
                    "Skip tablet since it has a zero metric (TabletId: %v, TableId: %v)",
                    tabletId,
                    tablet->Table->Id);
                continue;
            }

            Tablets_.push_back(TTabletInfo{
                .Tablet = tablet,
                .Metric = tabletMetric,
                .Cell = cellInfo
            });
            cellInfo->Metric += tabletMetric;
        }

        nodeInfo->Metric += cellInfo->Metric;

        YT_LOG_DEBUG_IF(
            Bundle_->Config->EnableVerboseLogging,
            "Calculated cell metric (CellId: %v, CellMetric: %v)",
            cell->Id,
            cellInfo->Metric);
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

    CalculateMemory();
}

void TParameterizedReassignSolver::CalculateMemory()
{
    if (Bundle_->NodeStatistics.empty()) {
        YT_LOG_DEBUG("Don't calculate memory because there are no in-memory tables with parameterized balancing");
        return;
    }

    THashMap<TNodeAddress, int> cellCount;
    THashMap<TNodeAddress, i64> actualMemoryUsage;
    THashMap<const TTabletCell*, i64> cellMemoryUsage;
    for (const auto& [cellId, cellInfo] : Cells_) {
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
                THROW_ERROR_EXCEPTION(
                    "Node memory usage exceeds memory limit")
                    << TErrorAttribute("memory_limit", statistics.MemoryLimit)
                    << TErrorAttribute("memory_usage", statistics.MemoryUsed)
                    << TErrorAttribute("actual_memory_usage", actualUsage)
                    << TErrorAttribute("node", address)
                    << TErrorAttribute("cell_count", count)
                    << TErrorAttribute("tablet_slot_count", statistics.TabletSlotCount)
                    << TErrorAttribute("group", GroupName_)
                    << TErrorAttribute("bundle", Bundle_->Name);
            }
            free = statistics.MemoryLimit - actualUsage;
        } else {
            unaccountedUsage = statistics.MemoryUsed - actualUsage;
        }

        auto tabletSlotCount = std::max(statistics.TabletSlotCount, count);
        auto cellLimit = (statistics.MemoryLimit - unaccountedUsage) / tabletSlotCount;

        GetOrCrash(Nodes_, address).FreeNodeMemory = free;
        EmplaceOrCrash(cellMemoryLimit, address, cellLimit);
    }

    for (const auto& [cell, usage] : cellMemoryUsage) {
        auto limit = GetOrCrash(cellMemoryLimit, *cell->NodeAddress);
        GetOrCrash(Cells_, cell->Id).FreeCellMemory = limit - usage;
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
        [] (auto lhs, auto rhs) {
            return lhs.second.Metric < rhs.second.Metric;
        });

    bool byNodeTrigger = maxNode->second.Metric >=
        minNode->second.Metric * (1 + Config_.NodeDeviationThreshold);

    auto [minCell, maxCell] = std::minmax_element(
        Cells_.begin(),
        Cells_.end(),
        [] (auto lhs, auto rhs) {
            return lhs.second.Metric < rhs.second.Metric;
        });

    bool byCellTrigger = maxCell->second.Metric >=
        minCell->second.Metric * (1 + Config_.CellDeviationThreshold);

    YT_LOG_DEBUG_IF(
        Bundle_->Config->EnableVerboseLogging,
        "Arguments for checking whether parameterized balancing should trigger have been calculated "
        "(MinNodeMetric: %v, MaxNodeMetric: %v, MinCellMetric: %v, MaxCellMetric: %v, "
        "NodeDeviationThreshold: %v, CellDeviationThreshold: %v)",
        minNode->second.Metric,
        maxNode->second.Metric,
        minCell->second.Metric,
        maxCell->second.Metric,
        Config_.NodeDeviationThreshold,
        Config_.CellDeviationThreshold);

    return byNodeTrigger || byCellTrigger;
}

double TParameterizedReassignSolver::CalculateTotalBundleMetric() const
{
    double cellMetric = std::accumulate(
        Cells_.begin(),
        Cells_.end(),
        0.0,
        [] (double x, auto item) {
            return x + Sqr(item.second.Metric);
        });

    double nodeMetric = std::accumulate(
        Nodes_.begin(),
        Nodes_.end(),
        0.0,
        [] (double x, auto item) {
            return x + Sqr(item.second.Metric);
        });

    return cellMetric + nodeMetric;
}

bool TParameterizedReassignSolver::CheckMoveFollowsMemoryLimits(
    const TTabletInfo* tablet,
    const TTabletCellInfo* cell) const
{
    if (tablet->Tablet->Table->InMemoryMode == EInMemoryMode::None) {
        return true;
    }

    auto size = tablet->Tablet->Statistics.MemorySize;
    if (cell->FreeCellMemory < size) {
        return false;
    }

    return cell->Node->Address == tablet->Cell->Node->Address || cell->Node->FreeNodeMemory >= size;
}

void TParameterizedReassignSolver::TryMoveTablet(
    TTabletInfo* tablet,
    TTabletCellInfo* cell)
{
    auto newMetric = CurrentMetric_;
    auto* sourceCell = tablet->Cell;

    if (cell->Cell->Id == sourceCell->Cell->Id) {
        // Trying to move the tablet from the cell to itself.
        return;
    }

    auto* sourceNode = sourceCell->Node;
    auto* destonationNode = cell->Node;

    if (!CheckMoveFollowsMemoryLimits(tablet, cell)) {
        // Cannot move due to memory limits.
        YT_LOG_DEBUG_IF(
            Bundle_->Config->EnableVerboseLogging && LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
            "Cannot move tablet (TabletId: %v, CellId: %v, SourceNode: %v, DestinationNode: %v)",
            tablet->Tablet->Id,
            cell->Cell->Id,
            sourceNode->Address,
            destonationNode->Address);
        return;
    }

    if (sourceNode->Address != destonationNode->Address) {
        auto sourceNodeMetric = sourceNode->Metric;
        auto destinationNodeMetric = destonationNode->Metric;

        newMetric -= Sqr(sourceNodeMetric) - Sqr(sourceNodeMetric - tablet->Metric);
        newMetric += Sqr(destinationNodeMetric + tablet->Metric) - Sqr(destinationNodeMetric);
    }

    newMetric -= Sqr(sourceCell->Metric) - Sqr(sourceCell->Metric - tablet->Metric);
    newMetric += Sqr(cell->Metric + tablet->Metric) - Sqr(cell->Metric);

    YT_LOG_DEBUG_IF(
        Bundle_->Config->EnableVerboseLogging && LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
        "Trying to move tablet to another cell (TabletId: %v, CellId: %v, CurrentMetric: %v, CurrentBestMetric: %v, "
        "NewMetric: %v, TabletMetric: %v, SourceCellMetric: %v, DestinationCellMetric: %v, "
        "SourceNodeMetric: %v, DestinationNodeMetric: %v)",
        tablet->Tablet->Id,
        cell->Cell->Id,
        CurrentMetric_,
        BestAction_.Metric,
        newMetric,
        tablet->Metric,
        sourceCell->Metric,
        cell->Metric,
        sourceNode->Metric,
        destonationNode->Metric);

    if (newMetric < BestAction_.Metric) {
        BestAction_.Metric = newMetric;

        BestAction_.Callback = [=, this] (int* availableActionCount) {
            tablet->Cell = cell;
            sourceCell->Metric -= tablet->Metric;
            cell->Metric += tablet->Metric;
            *availableActionCount -= 1;

            if (sourceNode->Address != destonationNode->Address) {
                sourceNode->Metric -= tablet->Metric;
                destonationNode->Metric += tablet->Metric;
            } else {
                YT_LOG_WARNING("The best action is between cells on the same node "
                    "(Node: %v, TabletId: %v)",
                    sourceNode->Address,
                    tablet->Tablet->Id);
            }

            YT_LOG_DEBUG("Applying best action: moving tablet to another cell "
                "(TabletId: %v, SourceCellId: %v, DestinationCellId: %v, "
                "SourceNode: %v, DestinationNode: %v)",
                tablet->Tablet->Id,
                sourceCell->Cell->Id,
                cell->Cell->Id,
                sourceNode->Address,
                destonationNode->Address);

            auto tabletSize = tablet->Tablet->Statistics.MemorySize;
            if (tabletSize == 0) {
                return;
            }

            sourceCell->FreeCellMemory += tabletSize;
            cell->FreeCellMemory -= tabletSize;

            if (sourceNode->Address != destonationNode->Address) {
                sourceNode->FreeNodeMemory += tabletSize;
                destonationNode->FreeNodeMemory -= tabletSize;
            }
        };
    }
}

bool TParameterizedReassignSolver::CheckSwapFollowsMemoryLimits(
    const TTabletInfo* lhsTablet,
    const TTabletInfo* rhsTablet) const
{
    i64 lhsTabletSize = lhsTablet->Tablet->Statistics.MemorySize;
    i64 rhsTabletSize = rhsTablet->Tablet->Statistics.MemorySize;

    i64 diff = lhsTabletSize - rhsTabletSize;
    if (diff == 0) {
        // Same size or both with in_memory_mode=none.
        return true;
    }

    i64 freeLhsCellMemory = lhsTablet->Cell->FreeCellMemory;
    i64 freeRhsCellMemory = rhsTablet->Cell->FreeCellMemory;

    i64 freeLhsNodeMemory = lhsTablet->Cell->Node->FreeNodeMemory;
    i64 freeRhsNodeMemory = rhsTablet->Cell->Node->FreeNodeMemory;

    const auto& lhsNode = lhsTablet->Cell->Node->Address;
    const auto& rhsNode = rhsTablet->Cell->Node->Address;

    if (freeLhsCellMemory < 0 && freeRhsCellMemory < 0) {
        // Both are overloaded from the beginning.
        return false;
    }

    if (lhsNode != rhsNode && (freeLhsNodeMemory + diff < 0 || freeRhsNodeMemory - diff < 0)) {
        return false;
    }

    if (freeLhsCellMemory + diff >= 0 && freeRhsCellMemory - diff >= 0)
    {
        // Perfect case.
        return true;
    }

    // Check if one of them are overloaded from the beginning but it's better than before.
    if (freeLhsCellMemory < 0) {
        return diff > 0 && (freeRhsCellMemory - diff > freeLhsCellMemory);
    } else if (freeRhsCellMemory < 0) {
        return diff < 0 && (freeLhsCellMemory + diff > freeRhsCellMemory);
    }

    return false;
}

void TParameterizedReassignSolver::TrySwapTablets(
    TTabletInfo* lhsTablet,
    TTabletInfo* rhsTablet)
{
    if (lhsTablet->Tablet->Id == rhsTablet->Tablet->Id) {
        // It makes no sense to swap tablet with itself.
        return;
    }

    auto* lhsCell = lhsTablet->Cell;
    auto* rhsCell = rhsTablet->Cell;

    if (lhsCell->Cell->Id == rhsCell->Cell->Id) {
        // It makes no sense to swap tablets that are already on the same cell.
        return;
    }

    auto newMetric = CurrentMetric_;
    newMetric -= Sqr(lhsCell->Metric) + Sqr(rhsCell->Metric);
    newMetric += Sqr(lhsCell->Metric - lhsTablet->Metric + rhsTablet->Metric);
    newMetric += Sqr(rhsCell->Metric + lhsTablet->Metric - rhsTablet->Metric);

    auto lhsNode = lhsCell->Node;
    auto rhsNode = rhsCell->Node;

    if (!CheckSwapFollowsMemoryLimits(lhsTablet, rhsTablet)) {
        // Cannot swap due to memory limits.
        YT_LOG_DEBUG_IF(
            Bundle_->Config->EnableVerboseLogging && LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
            "Cannot swap tablets (LhsTabletId: %v, RhsTabletId: %v, "
            "LhsCellId: %v, RhsCellId: %v, LhsNode: %v, RhsNode: %v)",
            lhsTablet->Tablet->Id,
            rhsTablet->Tablet->Id,
            lhsCell->Cell->Id,
            rhsCell->Cell->Id,
            lhsNode->Address,
            rhsNode->Address);
        return;
    }

    if (lhsNode->Address != rhsNode->Address) {
        newMetric -= Sqr(lhsNode->Metric) + Sqr(rhsNode->Metric);
        newMetric += Sqr(lhsNode->Metric - lhsTablet->Metric + rhsTablet->Metric);
        newMetric += Sqr(rhsNode->Metric + lhsTablet->Metric - rhsTablet->Metric);
    }

    YT_LOG_DEBUG_IF(
        Bundle_->Config->EnableVerboseLogging && LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
        "Trying to swap tablets (LhsTabletId: %v, RhsTabletId: %v, LhsCellId: %v, RhsCellId: %v, "
        "CurrentMetric: %v, CurrentBestMetric: %v, NewMetric: %v, LhsTabletMetric: %v, "
        "RhsTabletMetric: %v, LhsCellMetric: %v, RhsCellMetric: %v, LhsNodeMetric: %v, RhsNodeMetric: %v)",
        lhsTablet->Tablet->Id,
        rhsTablet->Tablet->Id,
        lhsCell->Cell->Id,
        rhsCell->Cell->Id,
        CurrentMetric_,
        BestAction_.Metric,
        newMetric,
        lhsTablet->Metric,
        rhsTablet->Metric,
        lhsCell->Metric,
        rhsCell->Metric,
        lhsNode->Metric,
        rhsTablet->Metric);

    if (newMetric < BestAction_.Metric) {
        BestAction_.Metric = newMetric;

        BestAction_.Callback = [=, this] (int* availableActionCount) {
            lhsTablet->Cell = rhsCell;
            rhsTablet->Cell = lhsCell;

            lhsCell->Metric -= lhsTablet->Metric;
            lhsCell->Metric += rhsTablet->Metric;
            rhsCell->Metric += lhsTablet->Metric;
            rhsCell->Metric -= rhsTablet->Metric;
            *availableActionCount -= 2;

            if (lhsNode->Address != rhsNode->Address) {
                lhsNode->Metric -= lhsTablet->Metric;
                lhsNode->Metric += rhsTablet->Metric;
                rhsNode->Metric += lhsTablet->Metric;
                rhsNode->Metric -= rhsTablet->Metric;
            } else {
                YT_LOG_WARNING("The best action is between cells on the same node "
                    "(Node: %v, LhsTabletId: %v, RhsTabletId: %v)",
                    lhsNode->Address,
                    lhsTablet->Tablet->Id,
                    rhsTablet->Tablet->Id);
            }

            YT_LOG_DEBUG("Applying best action: swapping tablets "
                "(LhsTabletId: %v, RhsTabletId: %v, LhsCellId: %v, RhsCellId: %v, "
                "LhsNode: %v, RhsNode: %v)",
                lhsTablet->Tablet->Id,
                rhsTablet->Tablet->Id,
                lhsCell->Cell->Id,
                rhsCell->Cell->Id,
                lhsNode->Address,
                rhsNode->Address);

            i64 tabletSizeDiff = lhsTablet->Tablet->Statistics.MemorySize -
                rhsTablet->Tablet->Statistics.MemorySize;
            if (tabletSizeDiff == 0) {
                return;
            }

            lhsCell->FreeCellMemory += tabletSizeDiff;
            rhsCell->FreeCellMemory -= tabletSizeDiff;

            if (lhsNode->Address != rhsNode->Address) {
                lhsNode->FreeNodeMemory += tabletSizeDiff;
                rhsNode->FreeNodeMemory -= tabletSizeDiff;
            }
        };
    }
}

bool TParameterizedReassignSolver::TryFindBestAction(bool canMakeSwap)
{
    BestAction_ = TBestAction{.Metric = CurrentMetric_};

    if (!Config_.EnableSwaps) {
        YT_LOG_DEBUG("Swap actions are forbidden");
    }

    for (auto& tablet : Tablets_) {
        for (auto& [_, cell] : Cells_) {
            TryMoveTablet(&tablet, &cell);
        }

        if (!canMakeSwap) {
            // Swap two tablets takes two actions.
            YT_LOG_DEBUG_IF(
                Bundle_->Config->EnableVerboseLogging,
                "Swap cannot be done because there are not enough actions available");
            continue;
        }

        if (!Config_.EnableSwaps) {
            continue;
        }

        for (auto& anotherTablet : Tablets_) {
            TrySwapTablets(&tablet, &anotherTablet);
        }
    }

    return BestAction_.Metric < CurrentMetric_;
}

std::vector<TMoveDescriptor> TParameterizedReassignSolver::BuildActionDescriptors()
{
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
        if (TryFindBestAction(/*canMakeSwap*/ availableActionCount >= 2)) {
            YT_VERIFY(BestAction_.Callback);
            if (BestAction_.Metric >= CurrentMetric_ * (1 - Config_.MinRelativeMetricImprovement / std::ssize(Nodes_)))
            {
                YT_LOG_DEBUG(
                    "Metric-improving action is not better enough (CurrentMetric: %v, MetricAfterAction: %v)",
                    CurrentMetric_,
                    BestAction_.Metric);
                break;
            }

            BestAction_.Callback(&availableActionCount);

            YT_LOG_DEBUG(
                "Total parameterized metric changed (Old: %v, New: %v)",
                CurrentMetric_,
                BestAction_.Metric);
            CurrentMetric_ = BestAction_.Metric;

            YT_VERIFY(CurrentMetric_ >= 0);
        } else {
            YT_LOG_DEBUG("Metric-improving action was not found");
            break;
        }
    }

    std::vector<TMoveDescriptor> descriptors;
    for (auto& tablet : Tablets_) {
        auto sourceCellId = tablet.Tablet->Cell.Lock()->Id;
        auto destinationCellId = tablet.Cell->Cell->Id;
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
{ }

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
        .Priority = std::make_tuple(/*IsSplit*/ true, -tabletCount, -deviation)
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
        enlargedTabletMetric);

    return TReshardDescriptor{
        .Tablets = std::move(tabletsToMerge),
        .TabletCount = 1,
        .DataSize = enlargedTabletSize,
        .CorrelationId = correlationId,
        .Priority = std::make_tuple(false, -(rightTabletIndex - leftTabletIndex), deviation)
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

    if (statistics.TableMetric == 0.0) {
        YT_LOG_DEBUG("Calculated table metric for parameterized balancing via reshard is zero "
            "(TableId: %v, TablePath: %v)",
            table->Id,
            table->Path);
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
