#pragma once

#include "public.h"
#include "registry.h"

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>
#include <yt/yt/core/ytree/ypath_detail.h>

#include <library/cpp/monlib/encode/format.h>

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

    TDuration LingerTimeout;

    int WindowSize;

    std::optional<int> ThreadPoolSize;

    bool ConvertCountersToRate;

    bool ExportSummary;
    bool ExportSummaryAsMax;
    bool ExportSummaryAsAvg;

    bool MarkAggregates;

    bool EnableCoreProfilingCompatibility;

    bool EnableSelfProfiling;

    bool ReportBuildInfo;

    bool ReportRestart;

    TDuration ResponseCacheTtl;

    TDuration ReadDelay;

    std::optional<TString> Host;

    THashMap<TString, TString> InstanceTags;

    THashMap<TString, TShardConfigPtr> Shards;

    TSolomonExporterConfig()
    {
        RegisterParameter("grid_step", GridStep)
            .Default(TDuration::Seconds(5));

        RegisterParameter("linger_timeout", LingerTimeout)
            .Default(TDuration::Minutes(5));

        RegisterParameter("window_size", WindowSize)
            .Default(12);

        RegisterParameter("thread_pool_size", ThreadPoolSize)
            .Default();

        RegisterParameter("convert_counters_to_rate", ConvertCountersToRate)
            .Default(true);

        RegisterParameter("export_summary", ExportSummary)
            .Default(false);
        RegisterParameter("export_summary_as_max", ExportSummaryAsMax)
            .Default(true);
        RegisterParameter("export_summary_as_avg", ExportSummaryAsAvg)
            .Default(false);

        RegisterParameter("mark_aggregates", MarkAggregates)
            .Default(true);

        RegisterParameter("enable_core_profiling_compatibility", EnableCoreProfilingCompatibility)
            .Default(false);

        RegisterParameter("enable_self_profiling", EnableSelfProfiling)
            .Default(true);

        RegisterParameter("report_build_info", ReportBuildInfo)
            .Default(true);

        RegisterParameter("report_restart", ReportRestart)
            .Default(true);

        RegisterParameter("read_delay", ReadDelay)
            .Default(TDuration::Seconds(5));

        RegisterParameter("host", Host)
            .Default();

        RegisterParameter("instance_tags", InstanceTags)
            .Default();

        RegisterParameter("shards", Shards)
            .Default();

        RegisterParameter("response_cache_ttl", ResponseCacheTtl)
            .Default(TDuration::Minutes(2));

        RegisterPostprocessor([this] {
            if (LingerTimeout.GetValue() % GridStep.GetValue() != 0) {
                THROW_ERROR_EXCEPTION("\"linger_timeout\" must be multiple of \"grid_step\"");
            }
        });
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

    // ReadJson is compatibility function for bulatman@.
    //
    // Returns std::nullopt, if exporter is not ready.
    //
    // You should not use this function directly. Use Register() instead.
    std::optional<TString> ReadJson(const TReadOptions& options = {});

    // There must be at most 1 running exporter per registry.
    void Start();
    void Stop();

    NYTree::IYPathServicePtr GetService() const;

private:
    const TSolomonExporterConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const TSolomonRegistryPtr Registry_;
    
    NConcurrency::TThreadPoolPtr ThreadPool_;

    TFuture<void> Collector_;
    std::vector<std::pair<int, TInstant>> Window_;

    const NConcurrency::TPeriodicExecutorPtr CoreProfilingPusher_;

    TInstant StartTime_ = TInstant::Now();
    std::optional<TInstant> LastFetch_;
    THashMap<TString, std::optional<TInstant>> LastShardFetch_;

    struct TCacheKey
    {
        std::optional<TString> Shard;
        ::NMonitoring::EFormat Format;
        ::NMonitoring::ECompression Compression;

        TInstant Now;
        TDuration Period;
        std::optional<TDuration> Grid;

        bool operator == (const TCacheKey& other) const = default;

        operator size_t () const;
    };

    THashMap<TCacheKey, TSharedRef> ResponseCache_;

    TEventTimer CollectionStartDelay_;
    TCounter WindowErrors_;
    TCounter ReadDelays_;
    TCounter ResponseCacheHit_, ResponseCacheMiss_;

    class TSensorService
        : public NYTree::TYPathServiceBase
        , public NYTree::TSupportsGet
        , public NYTree::TSupportsList
    {
    public:
        TSensorService(TSolomonRegistryPtr registry, IInvokerPtr invoker);

    private:
        using TTagMap = THashMap<TString, TString>;

        const TSolomonRegistryPtr Registry_;
        const IInvokerPtr Invoker_;

        virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;
        virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;
        virtual void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override;
    };

    const TIntrusivePtr<TSensorService> Root_;

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

    TErrorOr<TReadWindow> SelectReadWindow(TInstant now, TDuration period, std::optional<TDuration> grid);

    bool TryReplyFromCache(const TCacheKey& cacheKey, const NHttp::IResponseWriterPtr& rsp);

    void CleanResponseCache();
};

DEFINE_REFCOUNTED_TYPE(TSolomonExporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
