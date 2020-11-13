#pragma once

#include "public.h"
#include "registry.h"

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/actions/public.h>

#include <yt/core/http/public.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/producer.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct TShardConfig
    : public NYTree::TYsonSerializable
{
    std::vector<TString> Filter;

    TShardConfig()
    {
        RegisterParameter("filter", Filter)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TShardConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSolomonExporterConfig
    : public NYTree::TYsonSerializable
{
    TDuration GridStep;

    int WindowSize;

    bool ConvertCountersToRate;

    bool ExportSummaryAsMax;

    bool MarkAggregates;

    bool EnableCoreProfilingCompatibility;

    bool ReportBuildInfo;

    TDuration ReadDelay;

    std::optional<TString> Host;

    THashMap<TString, TString> InstanceTags;

    THashMap<TString, TShardConfigPtr> Shards;

    TSolomonExporterConfig()
    {
        RegisterParameter("grid_step", GridStep)
            .Default(TDuration::Seconds(5));

        RegisterParameter("window_size", WindowSize)
            .Default(12);

        RegisterParameter("convert_counters_to_rate", ConvertCountersToRate)
            .Default(true);

        RegisterParameter("export_summary_as_max", ExportSummaryAsMax)
            .Default(true);

        RegisterParameter("mark_aggregates", MarkAggregates)
            .Default(true);

        RegisterParameter("enable_core_profiling_compatibility", EnableCoreProfilingCompatibility)
            .Default(false);

        RegisterParameter("report_build_info", ReportBuildInfo)
            .Default(true);

        RegisterParameter("read_delay", ReadDelay)
            .Default(TDuration::Seconds(5));

        RegisterParameter("host", Host)
            .Default();

        RegisterParameter("instance_tags", InstanceTags)
            .Default();

        RegisterParameter("shards", Shards)
            .Default();
    }


    bool Filter(const TString& shardName, const TString& sensorName);
};

DEFINE_REFCOUNTED_TYPE(TSolomonExporterConfig)

////////////////////////////////////////////////////////////////////////////////

class TSolomonExporter
    : public TRefCounted
{
public:
    TSolomonExporter(
        const TSolomonExporterConfigPtr& config,
        const IInvokerPtr& invoker,
        const TSolomonRegistryPtr& registry = nullptr);

    void Register(const TString& prefix, const NHttp::IServerPtr& server);

    // There must be at most 1 running exporter per registry.
    void Start();

private:
    const TSolomonExporterConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const TSolomonRegistryPtr Registry_;

    TFuture<void> Collector_;
    std::vector<std::pair<int, TInstant>> Window_;

    const NConcurrency::TPeriodicExecutorPtr CoreProfilingPusher_;

    std::optional<TInstant> LastFetch_;
    THashMap<TString, std::optional<TInstant>> LastShardFetch_;

    TEventTimer CollectionStartDelay_;
    TCounter WindowErrors_;
    TCounter ReadDelays_;

    void DoCollect();
    void DoPushCoreProfiling();

    void HandleIndex(const TString& prefix, const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);
    void HandleStatus(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);

    void HandleDebugSensors(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);
    void HandleDebugTags(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);

    void HandleShard(
        const std::optional<TString>& name,
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp);

    void ValidatePeriodAndGrid(std::optional<TDuration> period, std::optional<TDuration> grid);
};

DEFINE_REFCOUNTED_TYPE(TSolomonExporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
