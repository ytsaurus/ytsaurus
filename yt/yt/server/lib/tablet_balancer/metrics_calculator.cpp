#include "metrics_calculator.h"

#include "helpers.h"
#include "public.h"
#include "replica_balancing_helpers.h"
#include "table.h"
#include "tablet.h"

#include <yt/yt/orm/library/query/heavy/expression_evaluator.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NTabletBalancer {

using namespace NCypressClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

const std::string DefaultParameterizedMetricFormula = "double([/performance_counters/dynamic_row_write_data_weight_10m_rate])";
const std::vector<NYPath::TYPath> ParameterizedBalancingAttributes = {
    "/statistics",
    "/performance_counters"
};


/////////////////////////////////////////////////////////////////////////////////

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

TParameterizedMetricsEvaluator::TParameterizedMetricsEvaluator(
    std::vector<std::string> metrics,
    std::vector<std::string> performanceCountersKeys,
    const TLogger& logger)
    : PerformanceCountersKeys_(std::move(performanceCountersKeys))
    , Logger(logger)
    , Metrics_(std::move(metrics))
{
    if (Metrics_.empty() || std::ssize(Metrics_) > MaxMetricCount) {
        THROW_ERROR_EXCEPTION("Unsupported number of metrics: expected between 1 and %v",
            MaxMetricCount)
            .With("metric_count", Metrics_.size());
    }

    Evaluators_.reserve(Metrics_.size());

    for (const auto& metric : Metrics_) {
        auto newMetric = ReplaceAliases(metric);

        YT_TLOG_DEBUG_IF(newMetric != metric, "Replaced aliases in parameterized balancing metric")
            .With("OldMetric", metric)
            .With("NewMetric", newMetric);

        Evaluators_.emplace_back(
            NOrm::NQuery::CreateOrmExpressionEvaluator(
                ParseSource(newMetric, EParseMode::Expression),
                ParameterizedBalancingAttributes));
    }
}

std::array<double, MaxMetricCount> TParameterizedMetricsEvaluator::EvaluateTabletMetrics(
    const TTabletPtr& tablet,
    const TTableSchemaPtr& schema) const
{
    std::array<double, MaxMetricCount> values = {};

    if (tablet->State == ETabletState::Unmounted) {
        return values;
    }

    auto tableId = tablet->Table
        ? tablet->Table->Id
        : NullObjectId;

    auto rowBuffer = New<TRowBuffer>();

    for (int index = 0; index < std::ssize(Evaluators_); ++index) {
        auto rawValue = Evaluators_[index]->Evaluate({
            ConvertToYsonString(tablet->Statistics.OriginalNode),
            tablet->GetPerformanceCountersYson(PerformanceCountersKeys_, schema)
        },
        rowBuffer).ValueOrThrow();

        auto value = ExtractMetricValue(rawValue, Metrics_[index], tablet->Id, tableId);
        if (value < 0.0) {
            THROW_ERROR_EXCEPTION("Tablet metric must be nonnegative, got %v", value)
                .With("tablet_metric_value", value)
                .With("tablet_id", tablet->Id)
                .With("table_id", tableId)
                .With("metric_index", index)
                .With("metric_formula", Metrics_[index])
                .With("metric_formulas", Metrics_);
        }

        values[index] = value;
    }

    return values;
}

////////////////////////////////////////////////////////////////////////////////

template <int MetricSize>
TParameterizedMetricsCalculator<MetricSize>::TParameterizedMetricsCalculator(
    std::vector<std::string> metrics,
    std::vector<std::string> performanceCountersKeys,
    TTableSchemaPtr performanceCountersTableSchema,
    const TLogger& logger)
    : TParameterizedMetricsEvaluator(
        std::move(metrics),
        std::move(performanceCountersKeys),
        logger)
    , PerformanceCountersTableSchema_(std::move(performanceCountersTableSchema))
{
    YT_VERIFY(std::ssize(Metrics_) == MetricSize);
}

template <int MetricSize>
THashMap<TTabletId, TGenericMetric<MetricSize>> TParameterizedMetricsCalculator<MetricSize>::GetTableMetrics(const TTable* table) const
{
    THashMap<TTabletId, TMetric> tabletToMetric;
    for (const auto& tablet : table->Tablets) {
        EmplaceOrCrash(tabletToMetric, tablet->Id, GetTabletMetric(tablet));
    }

    return tabletToMetric;
}

template <int MetricSize>
TGenericMetric<MetricSize> TParameterizedMetricsCalculator<MetricSize>::GetTabletMetric(const TTabletPtr& tablet) const
{
    return CalculateTabletMetric(tablet, PerformanceCountersTableSchema_);
}

template <int MetricSize>
TGenericMetric<MetricSize> TParameterizedMetricsCalculator<MetricSize>::CalculateTabletMetric(const TTabletPtr& tablet, const TTableSchemaPtr& schema) const
{
    if (tablet->State == ETabletState::Unmounted) {
        return TMetric::Zero();
    }

    auto metricValues = EvaluateTabletMetrics(tablet, schema);
    std::array<double, MetricSize> values;
    std::copy_n(metricValues.begin(), MetricSize, values.begin());

    return TMetric(values);
}

////////////////////////////////////////////////////////////////////////////////

template <int MetricSize>
class TReplicaMetricsCalculator
    : public TParameterizedMetricsCalculator<MetricSize>
{
    using TBase = TParameterizedMetricsCalculator<MetricSize>;
    using TMetric = typename TBase::TMetric;

public:
    TReplicaMetricsCalculator(
        std::vector<std::string> metrics,
        std::vector<std::string> performanceCountersKeys,
        TTableSchemaPtr performanceCountersTableSchema,
        THashMap<TClusterName, TTableSchemaPtr> perClusterPerformanceCountersTableSchemas,
        const TLogger& logger,
        bool enableVerboseLogging)
        : TBase(
            std::move(metrics),
            std::move(performanceCountersKeys),
            std::move(performanceCountersTableSchema),
            logger)
        , ClusterPerformanceCountersTableSchemas_(std::move(perClusterPerformanceCountersTableSchemas))
        , Logger(logger)
        , EnableVerboseLogging_(enableVerboseLogging)
    { }

    THashMap<TTabletId, TMetric> GetTableMetrics(const TTable* table) const override
    {
        if (table->AlienTables.empty()) {
            YT_TLOG_DEBUG_IF(EnableVerboseLogging_, "Calculating replica table metrics as only major table metrics")
                .With("TableId", table->Id);
            return TBase::GetTableMetrics(table);
        }

        if (DoMinorTablesHaveSamePivotKeys(table)) {
            return TBase::GetTableMetrics(table);
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
            TBase::PerformanceCountersTableSchema_);

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

        THashMap<TTabletId, TMetric> metrics;
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

    TMetric GetTabletMetric(const TTabletPtr& tablet) const override
    {
        YT_VERIFY(tablet->Table);

        auto metric = TBase::GetTabletMetric(tablet);
        if (tablet->Table->AlienTables.empty()) {
            return metric;
        }

        for (const auto& [cluster, minorTables] : tablet->Table->AlienTables) {
            auto schema = GetOrCrash(ClusterPerformanceCountersTableSchemas_, cluster);
            for (const auto& minorTable : minorTables) {
                YT_VERIFY(std::ssize(tablet->Table->Tablets) == std::ssize(minorTable->Tablets));
                metric += TBase::CalculateTabletMetric(
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

    std::vector<TMetric> GetTabletMetrics(const TTableBase* table, const TTableSchemaPtr& schema) const
    {
        std::vector<TMetric> metrics;
        for (const auto& tablet : table->Tablets) {
            YT_VERIFY(std::ssize(metrics) == tablet->Index);
            metrics.push_back(TBase::CalculateTabletMetric(tablet, schema));
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


////////////////////////////////////////////////////////////////////////////////

template <int MetricSize>
std::unique_ptr<TParameterizedMetricsCalculator<MetricSize>> CreateReplicaMetricsCalculator(
    std::vector<std::string> metrics,
    std::vector<std::string> performanceCountersKeys,
    NTableClient::TTableSchemaPtr performanceCountersTableSchema,
    THashMap<TClusterName, NTableClient::TTableSchemaPtr> perClusterPerformanceCountersTableSchemas,
    const NLogging::TLogger& logger,
    bool enableVerboseLogging)
{
    return std::make_unique<TReplicaMetricsCalculator<MetricSize>>(
        std::move(metrics),
        std::move(performanceCountersKeys),
        std::move(performanceCountersTableSchema),
        std::move(perClusterPerformanceCountersTableSchemas),
        logger,
        enableVerboseLogging);
}

////////////////////////////////////////////////////////////////////////////////

#define INSTANTIATE_METRICS_CALCULATOR(size) \
    template class TParameterizedMetricsCalculator<size>; \
    template std::unique_ptr<TParameterizedMetricsCalculator<size>> CreateReplicaMetricsCalculator<size>( \
        std::vector<std::string>, \
        std::vector<std::string>, \
        NTableClient::TTableSchemaPtr, \
        THashMap<TClusterName, NTableClient::TTableSchemaPtr>, \
        const NLogging::TLogger&, \
        bool);

YT_FOR_EACH_METRIC_SIZE(INSTANTIATE_METRICS_CALCULATOR)

#undef INSTANTIATE_METRICS_CALCULATOR

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
