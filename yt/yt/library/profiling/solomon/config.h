#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct TShardConfig
    : public NYTree::TYsonStruct
{
    std::vector<std::string> Filter;

    std::optional<TDuration> GridStep;

    bool StripSensorsNamePrefix;

    REGISTER_YSON_STRUCT(TShardConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TShardConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSolomonExporterDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<int> ThreadPoolSize;
    std::optional<TDuration> ThreadPoolPollingPeriod;

    REGISTER_YSON_STRUCT(TSolomonExporterDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSolomonExporterDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSolomonExporterConfig
    : public NYTree::TYsonStruct
{
    TDuration GridStep;

    TDuration LingerTimeout;

    int WindowSize;

    int ThreadPoolSize;
    int EncodingThreadPoolSize;
    TDuration ThreadPoolPollingPeriod;
    TDuration EncodingThreadPoolPollingPeriod;

    bool ConvertCountersToRateForSolomon;
    bool RenameConvertedCounters;
    bool ConvertCountersToDeltaGauge;
    bool EnableHistogramCompat;
    bool SplitRateHistogramIntoGauges;
    bool ReportTimestampsForRateMetrics;

    bool ExportSummary;
    bool ExportSummaryAsSum;
    bool ExportSummaryAsMax;
    bool ExportSummaryAsMin;
    bool ExportSummaryAsAvg;

    bool MarkAggregates;
    // Enable support of all available solomon aggregation methods.
    bool EnableSolomonAggregates;
    // Export all global metrics as memonly.
    bool ExportGlobalsAsMemOnly;

    bool StripSensorsNamePrefix;

    bool EnableCoreProfilingCompatibility;

    bool EnableSelfProfiling;

    bool ReportBuildInfo;

    bool ReportKernelVersion;

    bool ReportRestart;

    TDuration ResponseCacheTtl;

    TDuration ReadDelay;

    std::optional<std::string> Host;

    THashMap<std::string, std::string> InstanceTags;

    THashMap<std::string, TShardConfigPtr> Shards;

    TDuration UpdateSensorServiceTreePeriod;

    int ProducerCollectionBatchSize;

    ELabelSanitizationPolicy LabelSanitizationPolicy;

    TShardConfigPtr MatchShard(const std::string& sensorName);

    TSolomonExporterConfigPtr ApplyDynamic(const TSolomonExporterDynamicConfigPtr& dynamicConfig) const;

    ESummaryPolicy GetSummaryPolicy() const;

    REGISTER_YSON_STRUCT(TSolomonExporterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSolomonExporterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
