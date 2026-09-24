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

TParameterizedMetricsCalculator::TParameterizedMetricsCalculator(
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

THashMap<TTabletId, double> TParameterizedMetricsCalculator::GetTableMetrics(const TTable* table) const
{
    THashMap<TTabletId, double> tabletToMetric;
    for (const auto& tablet : table->Tablets) {
        EmplaceOrCrash(tabletToMetric, tablet->Id, GetTabletMetric(tablet));
    }
    return tabletToMetric;
}

double TParameterizedMetricsCalculator::GetTabletMetric(const TTabletPtr& tablet) const
{
    return GetTabletMetric(tablet, PerformanceCountersTableSchema_);
}

double TParameterizedMetricsCalculator::GetTabletMetric(const TTabletPtr& tablet, const TTableSchemaPtr& schema) const
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

DEFINE_REFCOUNTED_TYPE(TParameterizedMetricsCalculator)

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

TParameterizedMetricsCalculatorPtr CreateReplicaMetricsCalculator(
    std::string metric,
    std::vector<std::string> performanceCountersKeys,
    NTableClient::TTableSchemaPtr performanceCountersTableSchema,
    THashMap<TClusterName, NTableClient::TTableSchemaPtr> perClusterPerformanceCountersTableSchemas,
    const NLogging::TLogger& logger,
    bool enableVerboseLogging)
{
    return New<TReplicaMetricsCalculator>(
        std::move(metric),
        std::move(performanceCountersKeys),
        std::move(performanceCountersTableSchema),
        std::move(perClusterPerformanceCountersTableSchemas),
        logger,
        enableVerboseLogging);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
