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

    REGISTER_YSON_STRUCT(TShardConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TShardConfig)

////////////////////////////////////////////////////////////////////////////////

//! This class defines externally and statically configurable options for fetching metrics.
//! You can see how to pass these options externally in integration tests. Keep in mind that the passed values are
//! parsed as YSON, so you need to follow YSON escaping/parsing rules. Most parameters are boolean, which you can
//! pass as just "true"/"false" strings. Be careful when passing strings and pass additional surrounding quotes if
//! needed. Parsing errors can be retrieved from YT error headers (X-YT-Error).
//! NB: This class is used both as a base for TSolomonExporterConfig as well as a parser for arbitrary GET parameters
//! passed to metric fetch requests (scrapes). It is hacky, but it allows us to save option-copying boilerplate all
//! over the place. This is class is ref-counted because it is used as a base for other ref-counted YSON structs.
struct TScrapeOptions
    : public NYTree::TYsonStruct
{
    //! If true, counter values will be converted to rates per second based on the requested window of measurements and internal grid step.
    //! NB: If you are not using windowed reads this will return the rate during the last internal collection interval, which is
    //! 5 seconds by default. Typically that is not acceptable: if the scrape interval is larger than 5 seconds, you will miss some deltas.
    bool ConvertCountersToRateGauge;
    //! If true, the option above will be set to true for all reads requesting Solomon-formats: SPACK and JSON (for historical reasons).
    //! NB: If this is true by default in your static config, but you want to disable conversions for a specific Solomon-format read, you need to
    //! set this value to false, not just the value above.
    bool ForceConvertCountersToRateGaugeForSolomon;

    //! If true, counter values will be converted to deltas based on the requested window of measurements and internal grid step.
    //! NB: If you are not using windowed reads this will return the delta during the last internal collection interval, which is
    //! 5 seconds by default. Typically that is not acceptable: if the scrape interval is larger than 5 seconds, you will miss some deltas.
    bool ConvertCountersToDeltaGauge;
    //! If true, the option above will be set to true for all reads requesting Solomon-formats: SPACK and JSON (for historical reasons).
    //! NB: If this is true by default in your static config, but you want to disable conversions for a specific Solomon-format read, you need to
    //! set this value to false, not just the value above.
    bool ForceConvertCountersToDeltaGaugeForSolomon;

    //! If true, and sensor names will be suffixed with `rate` or `delta` in case the corresponding options above are enabled.
    //! This options respects the `sensor_component_delimiter` option defined below, it will be used as the delimiter before
    //! the suffix.
    bool RenameConvertedCounters;

    // TODO(eshcherbin, achulkov2, gritukan?): Find out what this option actually entails. For now, we have no idea,
    // other than the fact that it is defaulted for Solomon pulls.
    bool EnableAggregationWorkaround;
    bool ForceEnableAggregationWorkaroundForSolomon;

    //! If true, sensors that can be aggregated by host (i.e. not marked as global) will be marked with the yt_aggr="1" label.
    bool MarkAggregates;

    //! If true, sensor names are stripped of everything leading up to and including the last `/` character.
    //! Note that a suffix may still be appended if `rename_converted_counters` and any of the conversion options are set to true.
    bool StripSensorsNamePrefix;

    //! List of allowed special characters in delimiters between sensor name components.
    //! We can expand this list if needed, but let's keep it short for now.
    //! Prometheus only allows [a-zA-Z_:][a-zA-Z0-9_:]* for metric names and : are reserved for recording rules.
    //! For Solomon we use . as a delimiter.
    static constexpr auto SensorComponentDelimiterSpecialCharacterWhitelist = ".-_:";
    //! This string will be used as a delimiter between sensor name components, e.g. between "yt", "resource_tracker" and "total_cpu".
    //! Please keep in mind that Prometheus encoder will change all non-alphanumeric characters to underscores.
    std::string SensorComponentDelimiter;
    //! If true, all sensor name components (strings separated by `/`, e.g. "yt", "resource_tracker", "total_cpu") will
    //! be converted to camel case, assuming they are in underscore case initially.
    bool ConvertSensorComponentNamesToCamelCase;

    //! If true, a label `metric_type` will be added to all sensors equal to the monlib monitoring type of the sensor.
    //! This means that the type for counters will be `RATE`, not `COUNTER`.
    //! This is useful for Prometheus to distinguish between counters and gauges, because we
    //! typically cannot use the `convert_counters_to_rate_gauge` option.
    bool AddMetricTypeLabel;

    //! This config is simultaneously used for yson-configuration and parsed from parameters in GET requests.
    //! To avoid conflicts, we forbid parameters that are already used as GET parameters with another meaning.
    constexpr static const char* ForbiddenParameterNames[] = {"period", "now"};

    REGISTER_YSON_STRUCT(TScrapeOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TScrapeOptions)

////////////////////////////////////////////////////////////////////////////////

//! Inheriting from TScrapeOptions allows specifying default scrape options across the whole exporter.
struct TSolomonExporterConfig
    : public TScrapeOptions
{
    TDuration GridStep;

    TDuration LingerTimeout;

    int WindowSize;

    int ThreadPoolSize;
    int EncodingThreadPoolSize;
    TDuration ThreadPoolPollingPeriod;
    TDuration EncodingThreadPoolPollingPeriod;

    //! The options below control how summaries are exported by default.
    bool ExportSummary;
    bool ExportSummaryAsMax;
    bool ExportSummaryAsAvg;

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

    ESummaryPolicy GetSummaryPolicy() const;

    REGISTER_YSON_STRUCT(TSolomonExporterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSolomonExporterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
