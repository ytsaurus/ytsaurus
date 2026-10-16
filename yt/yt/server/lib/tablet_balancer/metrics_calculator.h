#pragma once

#include "metric.h"
#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/orm/library/query/heavy/public.h>

#include <library/cpp/yt/logging/logger.h>

#include <memory>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

extern const std::string DefaultParameterizedMetricFormula;
extern const std::vector<NYPath::TYPath> ParameterizedBalancingAttributes;

///////////////////////////////////////////////////////////////////////////////

double ExtractMetricValue(
    const NTableClient::TUnversionedValue& value,
    const std::string& metric,
    TTabletId tabletId,
    TTableId tableId);

////////////////////////////////////////////////////////////////////////////////

class TParameterizedMetricsEvaluator
{
public:
    TParameterizedMetricsEvaluator(
        std::vector<std::string> metrics,
        std::vector<std::string> performanceCountersKeys,
        const NLogging::TLogger& logger);

    std::array<double, MaxMetricCount> EvaluateTabletMetrics(
        const TTabletPtr& tablet,
        const NTableClient::TTableSchemaPtr& schema) const;

protected:
    const std::vector<std::string> PerformanceCountersKeys_;
    const NLogging::TLogger Logger;
    const std::vector<std::string> Metrics_;

    std::vector<NOrm::NQuery::IExpressionEvaluatorPtr> Evaluators_;
};

////////////////////////////////////////////////////////////////////////////////

template <int MetricSize>
class TParameterizedMetricsCalculator
    : public TParameterizedMetricsEvaluator
{
protected:
    using TMetric = TGenericMetric<MetricSize>;

public:
    TParameterizedMetricsCalculator(
        std::vector<std::string> metrics,
        std::vector<std::string> performanceCountersKeys,
        NTableClient::TTableSchemaPtr performanceCountersTableSchema,
        const NLogging::TLogger& logger);

    virtual ~TParameterizedMetricsCalculator() = default;

    virtual THashMap<TTabletId, TMetric> GetTableMetrics(const TTable* table) const;

protected:
    const NTableClient::TTableSchemaPtr PerformanceCountersTableSchema_;

    virtual TMetric GetTabletMetric(const TTabletPtr& tablet) const;

    TMetric CalculateTabletMetric(const TTabletPtr& tablet, const NTableClient::TTableSchemaPtr& schema) const;
};

////////////////////////////////////////////////////////////////////////////////

template <int MetricSize>
std::unique_ptr<TParameterizedMetricsCalculator<MetricSize>> CreateReplicaMetricsCalculator(
    std::vector<std::string> metrics,
    std::vector<std::string> performanceCountersKeys,
    NTableClient::TTableSchemaPtr performanceCountersTableSchema,
    THashMap<TClusterName, NTableClient::TTableSchemaPtr> perClusterPerformanceCountersTableSchemas,
    const NLogging::TLogger& logger,
    bool enableVerboseLogging);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
