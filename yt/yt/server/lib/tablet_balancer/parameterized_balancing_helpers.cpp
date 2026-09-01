#include "parameterized_balancing_helpers.h"

#include "balancing_helpers.h"
#include "bounded_priority_queue.h"
#include "config.h"
#include "helpers.h"
#include "replica_balancing_helpers.h"
#include "table.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell.h"
#include "tablet.h"

#include <yt/yt/orm/library/query/heavy/expression_evaluator.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

namespace NYT::NTabletBalancer {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

const std::string DefaultParameterizedMetricFormula = "double([/performance_counters/dynamic_row_write_data_weight_10m_rate])";

////////////////////////////////////////////////////////////////////////////////

const std::vector<NYPath::TYPath> ParameterizedBalancingAttributes = {
    "/statistics",
    "/performance_counters"
};

double ExtractMetricValue(
    const NTableClient::TUnversionedValue& value,
    const std::string& metric,
    TTabletId tabletId,
    TTableId tableId)
{
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
                .With("metric_formula", metric)
                .With("tablet_id", tabletId)
                .With("table_id", tableId);
    }
}

////////////////////////////////////////////////////////////////////////////////

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

TParameterizedResharderConfig TParameterizedResharderConfig::MergeWith(
    const TParameterizedBalancingConfigPtr& groupConfig) const
{
    return TParameterizedResharderConfig{
        .Metric = groupConfig->Metric.empty()
            ? Metric
            : groupConfig->Metric
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

void FormatValue(TStringBuilderBase* builder, const TParameterizedResharderConfig& config, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "EnableReshardByDefault: %v, Metric: %v",
        config.EnableReshardByDefault,
        config.Metric);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMetricsCalculatorType,
    (Parameterized)
    (Replica)
);

class TParameterizedMetricsCalculator
    : public TRefCounted
{
public:
    TParameterizedMetricsCalculator(
        std::string metric,
        std::vector<std::string> performanceCountersKeys,
        TTableSchemaPtr performanceCountersTableSchema,
        const TLogger& logger)
        : PerformanceCountersKeys_(std::move(performanceCountersKeys))
        , PerformanceCountersTableSchema_(std::move(performanceCountersTableSchema))
        , Metric_(std::move(metric))
        , Logger(logger)
    {
        auto newMetric = ReplaceAliases(Metric_);
        YT_TLOG_DEBUG_IF(newMetric != Metric_, "Replaced aliases in parameterized balancing metric")
            .With("OldMetric", Metric_)
            .With("NewMetric", newMetric);
        Evaluator_ = NOrm::NQuery::CreateOrmExpressionEvaluator(
            ParseSource(newMetric, EParseMode::Expression),
            ParameterizedBalancingAttributes);
    }

    virtual THashMap<TTabletId, double> GetTableMetrics(const TTable* table) const
    {
        THashMap<TTabletId, double> tabletToMetric;
        for (const auto& tablet : table->Tablets) {
            EmplaceOrCrash(tabletToMetric, tablet->Id, GetTabletMetric(tablet));
        }
        return tabletToMetric;
    }

    virtual double GetTabletMetric(const TTabletPtr& tablet) const
    {
        return GetTabletMetric(tablet, PerformanceCountersTableSchema_);
    }

protected:
    const std::vector<std::string> PerformanceCountersKeys_;
    const TTableSchemaPtr PerformanceCountersTableSchema_;
    const std::string Metric_;
    const TLogger Logger;
    NOrm::NQuery::IExpressionEvaluatorPtr Evaluator_;

    double GetTabletMetric(const TTabletPtr& tablet, const TTableSchemaPtr& schema) const
    {
        if (tablet->State == ETabletState::Unmounted) {
            return 0.0;
        }

        auto rowBuffer = New<TRowBuffer>();
        auto value = Evaluator_->Evaluate({
                ConvertToYsonString(tablet->Statistics.OriginalNode),
                tablet->GetPerformanceCountersYson(PerformanceCountersKeys_, schema)
            },
            rowBuffer)
            .ValueOrThrow();

        auto tableId = tablet->Table
            ? tablet->Table->Id
            : NullObjectId;

        return ExtractMetricValue(value, Metric_, tablet->Id, tableId);
    }
};

DEFINE_REFCOUNTED_TYPE(TParameterizedMetricsCalculator)
DECLARE_REFCOUNTED_CLASS(TParameterizedMetricsCalculator)

////////////////////////////////////////////////////////////////////////////////

class TReplicaMetricsCalculator
    : public TParameterizedMetricsCalculator
{
public:
    TReplicaMetricsCalculator(
        std::string metric,
        std::vector<std::string> performanceCountersKeys,
        TTableSchemaPtr performanceCountersTableSchema,
        THashMap<TClusterName, TTableSchemaPtr> perClusterPerformanceCountersTableSchemas,
        const TLogger& logger,
        bool enableVerboseLogging)
        : TParameterizedMetricsCalculator(
            std::move(metric),
            std::move(performanceCountersKeys),
            std::move(performanceCountersTableSchema),
            logger)
        , ClusterPerformanceCountersTableSchemas_(std::move(perClusterPerformanceCountersTableSchemas))
        , Logger(logger)
        , EnableVerboseLogging_(enableVerboseLogging)
    { }

    THashMap<TTabletId, double> GetTableMetrics(const TTable* table) const override
    {
        if (table->AlienTables.empty()) {
            YT_TLOG_DEBUG_IF(EnableVerboseLogging_, "Calculating replica table metrics as only major table metrics")
                .With("TableId", table->Id);
            return TParameterizedMetricsCalculator::GetTableMetrics(table);
        }

        if (DoMinorTablesHaveSamePivotKeys(table)) {
            return TParameterizedMetricsCalculator::GetTableMetrics(table);
        }

        YT_TLOG_DEBUG_IF(EnableVerboseLogging_, "Calculating replica table metrics by approximate metrics of minor tables")
            .With("TableId", table->Id);

        auto getTabletSizes = [] (const auto& table) {
            std::vector<i64> sizes;
            for (const auto& tablet : table->Tablets) {
                sizes.push_back(tablet->Statistics.CompressedDataSize);
            }
            return sizes;
        };

        auto majorTabletSizes = getTabletSizes(table);
        auto majorMetrics = GetTabletMetrics(
            static_cast<const TTableBase*>(table),
            PerformanceCountersTableSchema_);

        for (const auto& [cluster, minorTables] : table->AlienTables) {
            auto schema = GetOrCrash(ClusterPerformanceCountersTableSchemas_, cluster);
            for (const auto& minorTable : minorTables) {
                auto minorMetrics = CalculateMajorMetrics(
                    GetTabletMetrics(minorTable.Get(), schema),
                    majorTabletSizes,
                    getTabletSizes(minorTable),
                    table->PivotKeys,
                    minorTable->PivotKeys,
                    Logger.WithTag("TableId", minorTable->Id),
                    EnableVerboseLogging_);

                YT_VERIFY(std::ssize(minorMetrics) == std::ssize(majorMetrics));
                for (int index = 0; index < std::ssize(minorMetrics); ++index) {
                    majorMetrics[index] += minorMetrics[index];
                }
            }
        }

        THashMap<TTabletId, double> metrics;
        for (int index = 0; index < std::ssize(table->Tablets); ++index) {
            EmplaceOrCrash(metrics, table->Tablets[index]->Id, majorMetrics[index]);
        }

        return metrics;
    }

private:
    THashMap<TClusterName, TTableSchemaPtr> ClusterPerformanceCountersTableSchemas_;
    const NLogging::TLogger Logger;
    const bool EnableVerboseLogging_;
    mutable int LogMessageCount_ = 0;

    double GetTabletMetric(const TTabletPtr& tablet) const override
    {
        YT_VERIFY(tablet->Table);

        double metric = TParameterizedMetricsCalculator::GetTabletMetric(tablet);
        if (tablet->Table->AlienTables.empty()) {
            return metric;
        }

        for (const auto& [cluster, minorTables] : tablet->Table->AlienTables) {
            auto schema = GetOrCrash(ClusterPerformanceCountersTableSchemas_, cluster);
            for (const auto& minorTable : minorTables) {
                YT_VERIFY(std::ssize(tablet->Table->Tablets) == std::ssize(minorTable->Tablets));
                metric += TParameterizedMetricsCalculator::GetTabletMetric(
                    minorTable->Tablets[tablet->Index],
                    schema);
            }
        }

        YT_TLOG_DEBUG_IF(
            EnableVerboseLogging_ && LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
            "Calculated tablet metric as sum of minor table tablet metrics and major table tablet metric")
            .With("TableId", tablet->Table->Id)
            .With("TabletId", tablet->Id)
            .With("Metric", metric);

        return metric;
    }

    std::vector<double> GetTabletMetrics(const TTableBase* table, const TTableSchemaPtr& schema) const
    {
        std::vector<double> metrics;
        for (const auto& tablet : table->Tablets) {
            YT_VERIFY(std::ssize(metrics) == tablet->Index);
            metrics.push_back(TParameterizedMetricsCalculator::GetTabletMetric(tablet, schema));
        }
        return metrics;
    }

    bool DoMinorTablesHaveSamePivotKeys(const TTable* table) const
    {
        for (const auto& [cluster, minorTables] : table->AlienTables) {
            for (const auto& minorTable : minorTables) {
                if (minorTable->PivotKeys != table->PivotKeys) {
                    YT_TLOG_DEBUG_IF(EnableVerboseLogging_, "Pivots of minor and major tables are different")
                        .With("MinorTableId", minorTable->Id)
                        .With("MajorTableId", table->Id)
                        .With("MinorPivotKeys", minorTable->PivotKeys)
                        .With("MajorPivotKeys", table->PivotKeys);
                    return false;
                }
            }
        }

        YT_TLOG_DEBUG_IF(EnableVerboseLogging_, "Pivot keys of minor tables and major table are the same")
            .With("MajorTableId", table->Id);
        return true;
    }
};

DEFINE_REFCOUNTED_TYPE(TReplicaMetricsCalculator)
DECLARE_REFCOUNTED_CLASS(TReplicaMetricsCalculator)

////////////////////////////////////////////////////////////////////////////////

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
                Calculator_ = New<TReplicaMetricsCalculator>(
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
            YT_TLOG_DEBUG_IF(Bundle_->Config->EnableVerboseLogging && LogMessageCount_++ < MaxVerboseLogMessagesPerIteration, "Cannot move tablet")
                .With("TabletId", tablet->Id)
                .With("CellId", cell->Id)
                .With("SourceNode", sourceNode->Address)
                .With("DestinationNode", destinationNode->Address);
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

class TParameterizedResharder
    : public IParameterizedResharder
{
public:
    TParameterizedResharder(
        TTabletCellBundlePtr bundle,
        std::vector<std::string> performanceCountersKeys,
        TParameterizedResharderConfig config,
        TGroupName groupName,
        const TLogger& logger)
        : Bundle_(std::move(bundle))
        , Logger(logger
            .WithTag("BundleName", Bundle_->Name)
            .WithTag("Group", groupName))
        , Config_(std::move(config))
        , GroupName_(std::move(groupName))
        , Calculator_(New<TParameterizedMetricsCalculator>(
            Config_.Metric,
            std::move(performanceCountersKeys),
            Bundle_->PerformanceCountersTableSchema,
            Logger))
    {
        YT_TLOG_DEBUG("Reporting parameterized resharder config")
            .With("Config", Config_);
    }

    std::vector<TReshardDescriptor> BuildTableActionDescriptors(const TTablePtr& table) override
    {
        LogMessageCount_ = 0;

        if (!IsParameterizedReshardEnabled(table)) {
            YT_TLOG_DEBUG_IF(
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "Parameterized balancing via reshard is not enabled")
                .With("TableId", table->Id);
            return {};
        }

        YT_VERIFY(table->TableConfig->DesiredTabletCount.has_value() ||
            table->TableConfig->DesiredTabletMetric.has_value());

        if (table->TableConfig->DesiredTabletCount.has_value() && *table->TableConfig->DesiredTabletCount <= 0) {
            YT_TLOG_WARNING("Table desired tablet count is not positive")
                .With("TableId", table->Id)
                .With("TablePath", table->Path)
                .With("DesiredTabletCount", table->TableConfig->DesiredTabletCount);
            return {};
        }

        auto statistics = GetTableStatistics(table, table->TableConfig);
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

        YT_TLOG_DEBUG_UNLESS(actions.empty(), "Parameterized reshard action creation requested")
            .With("TabletCount", std::ssize(table->Tablets))
            .With("NewTabletCount", tabletCount)
            .With("DesiredTabletCount", statistics.DesiredTabletCount);

        SortTabletActionsByUsefulness(&actions);
        TrimTabletActions(std::ssize(table->Tablets), &actions);

        return actions;
    }

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
    TParameterizedMetricsCalculatorPtr Calculator_;

    mutable int LogMessageCount_ = 0;

    void SortTabletActionsByUsefulness(std::vector<TReshardDescriptor>* actions) const
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

    void TrimTabletActions(int currentTabletCount, std::vector<TReshardDescriptor>* actions) const
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

    std::optional<TReshardDescriptor> TryMakeTabletFit(
        const TTablePtr& table,
        int tabletIndex,
        THashSet<int>* touchedTabletIndexes,
        const TTableStatistics& statistics)
    {
        const auto& tablet = table->Tablets[tabletIndex];
        if (tablet->State != ETabletState::Mounted) {
            YT_TLOG_DEBUG_IF(
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "Tablet is not mounted, skipping reshard")
                .With("TabletId", tablet->Id)
                .With("TabletState", tablet->State);
            return std::nullopt;
        }

        auto tabletMetric = statistics.TabletMetrics[tabletIndex];
        auto tabletSize = statistics.TabletSizes[tabletIndex];

        // Tablet is too large by at least one of the metrics.
        if (tabletMetric > statistics.MaxTabletMetric ||
            tabletSize > statistics.MaxTabletSize)
        {
            if (tabletSize == 0) {
                // Should not happen othen.
                YT_TLOG_WARNING_IF(
                    (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                    LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                    "Trying to split an empty tablet; skipping it")
                    .With("TableId", table->Id)
                    .With("TabletId", tablet->Id)
                    .WithFormat("TabletMetric", "%e", tabletMetric)
                    .With("TableSize", statistics.TableSize)
                    .With("DesiredTabletSize", statistics.DesiredTabletSize)
                    .With("MaxTabletSize", statistics.MaxTabletSize);
                return std::nullopt;
            }

            return SplitTablet(table, tabletIndex, touchedTabletIndexes, statistics);
        }

        // Tablet is just right.
        if (tabletMetric >= statistics.MinTabletMetric &&
            tabletSize >= statistics.MinTabletSize)
        {
            YT_TLOG_DEBUG_IF(
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "Tablet is just right")
                .With("TabletId", tablet->Id)
                .WithFormat("TabletMetric", "%e", tabletMetric)
                .With("TabletSize", tabletSize);
            return std::nullopt;
        }

        return MergeTablets(table, tabletIndex, touchedTabletIndexes, statistics);
    }

    TReshardDescriptor SplitTablet(
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
        YT_TLOG_DEBUG("Splitting tablet")
            .With("Tablet", tabletId)
            .With("TabletSize", DivCeil<i64>(tabletSize, tabletCount))
            .WithFormat("TabletMetric", "%e", tabletMetric / tabletCount)
            .With("CorrelationId", correlationId);

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

    std::optional<TReshardDescriptor> MergeTablets(
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

        auto isMergeableNeighbor = [&] (int index) {
            return index >= 0 &&
                index < tabletCount &&
                !touchedTabletIndexes->contains(index) &&
                table->Tablets[index]->State == ETabletState::Mounted;
        };

        while (AreMoreTabletsNeeded(statistics, enlargedTabletSize, enlargedTabletMetric) &&
            isMergeableNeighbor(leftTabletIndex - 1) &&
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
            isMergeableNeighbor(rightTabletIndex) &&
            IsPossibleToAddTablet(statistics, enlargedTabletSize, enlargedTabletMetric, rightTabletIndex))
        {
            enlargedTabletSize += statistics.TabletSizes[rightTabletIndex];
            enlargedTabletMetric += statistics.TabletMetrics[rightTabletIndex];

            deviation = std::min({
                deviation,
                statistics.TabletMetrics[rightTabletIndex] / statistics.DesiredTabletMetric,
                static_cast<double>(statistics.TabletSizes[rightTabletIndex]) / statistics.DesiredTabletSize});

            ++rightTabletIndex;
        }

        if (rightTabletIndex - leftTabletIndex == 1) {
            YT_TLOG_DEBUG_IF(
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "The tablet is too small, but there are no tablets to merge with it")
                .With("TabletId", table->Tablets[tabletIndex]->Id)
                .With("TabletIndex", tabletIndex)
                .With("TabletSize", enlargedTabletSize)
                .WithFormat("TabletMetric", "%e", enlargedTabletMetric);
            return std::nullopt;
        }

        std::vector<TTabletId> tabletsToMerge;
        for (int index = leftTabletIndex; index < rightTabletIndex; ++index) {
            tabletsToMerge.push_back(table->Tablets[index]->Id);
            EmplaceOrCrash(*touchedTabletIndexes, index);
        }

        auto correlationId = TGuid::Create();
        YT_TLOG_DEBUG("Merging tablets")
            .With("Tablets", tabletsToMerge)
            .With("TabletSize", enlargedTabletSize)
            .WithFormat("TabletMetric", "%e", enlargedTabletMetric)
            .With("CorrelationId", correlationId);

        return TReshardDescriptor{
            .Tablets = std::move(tabletsToMerge),
            .TabletCount = 1,
            .DataSize = enlargedTabletSize,
            .CorrelationId = correlationId,
            .Priority = std::tuple(false, -(rightTabletIndex - leftTabletIndex), deviation)
        };
    }

    bool AreMoreTabletsNeeded(
        const TTableStatistics& statistics,
        i64 tabletSize,
        double tabletMetric) const
    {
        return statistics.IsTooSmallBySomeMeasure(tabletSize, tabletMetric) ||
            statistics.IsLessThanDesiredByEachMeasure(tabletSize, tabletMetric);
    }

    bool IsPossibleToAddTablet(
        const TTableStatistics& statistics,
        i64 tabletSize,
        double tabletMetric,
        int nextTabletIndex) const
    {
        return tabletSize + statistics.TabletSizes[nextTabletIndex] <= statistics.MaxTabletSize &&
            tabletMetric + statistics.TabletMetrics[nextTabletIndex] <= statistics.MaxTabletMetric;
    }

    bool IsParameterizedReshardEnabled(const TTablePtr& table) const
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

    TTableStatistics GetTableStatistics(
        const TTablePtr& table,
        const TTableTabletBalancerConfigPtr& config) const
    {
        TTableStatistics statistics {};

        for (const auto& tablet : table->Tablets) {
            statistics.TabletSizes.push_back(GetTabletBalancingSize(tablet));
            statistics.TableSize += statistics.TabletSizes.back();

            auto tabletMetric = Calculator_->GetTabletMetric(tablet);
            if (tabletMetric < 0.0) {
                THROW_ERROR_EXCEPTION("Tablet metric must be nonnegative, got %v", tabletMetric)
                    .With("tablet_metric_value", tabletMetric)
                    .With("tablet_id", tablet->Id)
                    .With("metric_formula", Config_.Metric);
            }

            statistics.TabletMetrics.push_back(tabletMetric);
            statistics.TableMetric += tabletMetric;

            YT_TLOG_DEBUG_IF(
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "Reporting tablet statistics")
                .With("TabletId", tablet->Id)
                .With("Size", statistics.TabletSizes.back())
                .WithFormat("Metric", "%e", tabletMetric)
                .With("TableId", table->Id);
        }

        if (config->DesiredTabletCount.has_value()) {
            YT_TLOG_DEBUG_IF(
                config->DesiredTabletMetric.has_value() &&
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "Desired tablet count and desired tablet metric both set in config, use desired tablet count")
                .With("TableId", table->Id)
                .With("DesiredTabletCount", config->DesiredTabletCount)
                .With("DesiredTabletMetric", config->DesiredTabletMetric);

            statistics.DesiredTabletCount = config->DesiredTabletCount.value();
            statistics.DesiredTabletMetric = statistics.TableMetric / statistics.DesiredTabletCount;

            statistics.DesiredTabletSize = statistics.TableSize / statistics.DesiredTabletCount;
        } else {
            statistics.DesiredTabletMetric = config->DesiredTabletMetric.value();
            statistics.DesiredTabletCount = statistics.TableMetric / statistics.DesiredTabletMetric;

            // NB(dave11ar): For accuracy purposes.
            statistics.DesiredTabletSize = statistics.DesiredTabletMetric * statistics.TableSize / statistics.TableMetric;
        }

        statistics.MinTabletSize = statistics.DesiredTabletSize / 1.9;
        statistics.MaxTabletSize = statistics.DesiredTabletSize * 1.9;

        statistics.MinTabletMetric = statistics.DesiredTabletMetric / 1.9;

        if (statistics.TableMetric == 0.0 || statistics.DesiredTabletMetric == 0.0) {
            YT_TLOG_DEBUG("Calculated table metric for parameterized balancing via reshard is zero or almost zero")
                .With("TableId", table->Id)
                .With("TablePath", table->Path)
                .WithFormat("TableMetric", "%e", statistics.TableMetric);
            statistics.DesiredTabletMetric = 1;
        }

        statistics.MaxTabletMetric = statistics.DesiredTabletMetric * 1.9;

        YT_TLOG_DEBUG_IF(Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging, "Reporting reshard limits and statistics")
            .With("MinTabletSize", statistics.MinTabletSize)
            .With("DesiredTabletSize", statistics.DesiredTabletSize)
            .With("MaxTabletSize", statistics.MaxTabletSize)
            .With("TableSize", statistics.TableSize)
            .WithFormat("MinTabletMetric", "%e", statistics.MinTabletMetric)
            .WithFormat("DesiredTabletMetric", "%e", statistics.DesiredTabletMetric)
            .WithFormat("MaxTabletMetric", "%e", statistics.MaxTabletMetric)
            .WithFormat("TableMetric", "%e", statistics.TableMetric)
            .With("TableId", table->Id);

        return statistics;
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

IParameterizedResharderPtr CreateParameterizedResharder(
    TTabletCellBundlePtr bundle,
    std::vector<std::string> performanceCountersKeys,
    TParameterizedResharderConfig config,
    TGroupName groupName,
    const NLogging::TLogger& logger)
{
    return New<TParameterizedResharder>(
        std::move(bundle),
        std::move(performanceCountersKeys),
        std::move(config),
        std::move(groupName),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
