#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/orm/library/query/heavy/public.h>

#include <library/cpp/yt/logging/logger.h>

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

class TParameterizedMetricsCalculator
    : public TRefCounted
{
public:
    TParameterizedMetricsCalculator(
        std::string metric,
        std::vector<std::string> performanceCountersKeys,
        NTableClient::TTableSchemaPtr performanceCountersTableSchema,
        const NLogging::TLogger& logger);

    virtual THashMap<TTabletId, double> GetTableMetrics(const TTable* table) const;

    virtual double GetTabletMetric(const TTabletPtr& tablet) const;

protected:
    const std::vector<std::string> PerformanceCountersKeys_;
    const NTableClient::TTableSchemaPtr PerformanceCountersTableSchema_;
    const std::string Metric_;
    const NLogging::TLogger Logger;
    NOrm::NQuery::IExpressionEvaluatorPtr Evaluator_;

    double GetTabletMetric(const TTabletPtr& tablet, const NTableClient::TTableSchemaPtr& schema) const;
};

////////////////////////////////////////////////////////////////////////////////

TParameterizedMetricsCalculatorPtr CreateReplicaMetricsCalculator(
    std::string metric,
    std::vector<std::string> performanceCountersKeys,
    NTableClient::TTableSchemaPtr performanceCountersTableSchema,
    THashMap<TClusterName, NTableClient::TTableSchemaPtr> perClusterPerformanceCountersTableSchemas,
    const NLogging::TLogger& logger,
    bool enableVerboseLogging);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
