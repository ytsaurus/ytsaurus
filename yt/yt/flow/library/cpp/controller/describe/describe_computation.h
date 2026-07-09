#pragma once

#include "common.h"

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

struct TPerformanceMetricsWithExamplePartitions
    : public TPerformanceMetricsBase
{
    std::optional<TPartitionId> CpuUsageExamplePartition;
    std::optional<TPartitionId> MemoryUsageExamplePartition;
    std::optional<TPartitionId> MessagesPerSecondExamplePartition;
    std::optional<TPartitionId> BytesPerSecondExamplePartition;

    REGISTER_YSON_STRUCT_LITE(TPerformanceMetricsWithExamplePartitions);

    static void Register(TRegistrar registrar);
};

struct TComputationMultidimensionPerformanceMetrics
    : public NYTree::TYsonStructLite
{
    TPerformanceMetricsBase Total;
    TPerformanceMetricsWithExamplePartitions Average;
    TPerformanceMetricsWithExamplePartitions Max;
    TPerformanceMetricsWithExamplePartitions Min;

    REGISTER_YSON_STRUCT_LITE(TComputationMultidimensionPerformanceMetrics);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TComputationPartitionCountWithExample
    : public NYTree::TYsonStructLite
{
    int Count{};
    std::optional<TPartitionId> ExamplePartition;

    REGISTER_YSON_STRUCT_LITE(TComputationPartitionCountWithExample);

    static void Register(TRegistrar registrar);
};

struct TComputationPartitionErrorsByReason
    : public NYTree::TYsonStructLite
{
    TComputationPartitionCountWithExample RestartBecauseFail;
    TComputationPartitionCountWithExample RestartBecauseRebalancing;
    TComputationPartitionCountWithExample HasRetryableError;
    TComputationPartitionCountWithExample TotalWithProblems;

    REGISTER_YSON_STRUCT_LITE(TComputationPartitionErrorsByReason);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TExtendedComputationDescription
    : public TComputationDescription
{
    TComputationMultidimensionPerformanceMetrics PerformanceMetrics;

    std::vector<TPartitionDescription> Partitions;

    THashMap<std::string, TComputationPartitionErrorsByReason> PartitionWithErrorByTimeAndType;

    // TODO: Bottle neck flags.

    REGISTER_YSON_STRUCT_LITE(TExtendedComputationDescription);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

void FillComputationPerformanceMetrics(TExtendedComputationDescription& description);
void FillComputationPartitionErrorMetrics(TExtendedComputationDescription& description, TInstant now);
void FillPerformanceMessage(
    TExtendedComputationDescription& description,
    const std::vector<TPartitionIntermediateDescription>& partitions);

TExtendedComputationDescription DescribeComputation(
    const TFlowViewPtr& flowView,
    const TComputationId& computationId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
