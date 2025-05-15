#include "config.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

void TStructuredLoggingMethodDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);

    registrar.Parameter("max_request_byte_size", &TThis::MaxRequestByteSize)
        .Default({});
}

////////////////////////////////////////////////////////////////////////////////

void TStructuredLoggingTopicDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);

    registrar.Parameter("suppressed_methods", &TThis::SuppressedMethods)
        .Default()
        .ResetOnLoad();

    registrar.Parameter("methods", &TThis::Methods)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TQueryCorpusReporterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("max_batch_size", &TThis::MaxBatchSize)
        .Default(1'000)
        .GreaterThan(0);
    registrar.Parameter("period", &TThis::Period)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("splay", &TThis::Splay)
        .Default(TDuration::MilliSeconds(5));
    registrar.Parameter("jitter", &TThis::Jitter)
        .Default(0.2)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(1);
    registrar.Parameter("report_backoff_time", &TThis::ReportBackoffTime)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("table_path", &TThis::TablePath);
}

////////////////////////////////////////////////////////////////////////////////

void TApiTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("heap_profiler", &TThis::HeapProfiler)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TApiServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("client_cache", &TThis::ClientCache)
        .DefaultNew();

    registrar.Parameter("user_access_validator", &TThis::UserAccessValidator)
        .Alias("security_manager")
        .DefaultNew();

    registrar.Parameter("testing", &TThis::TestingOptions)
        .Default();

    registrar.Parameter("enable_large_columnar_statistics", &TThis::EnableLargeColumnarStatistics)
        .Default(false);

    registrar.Preprocessor([] (TThis* config) {
        config->ClientCache->Capacity = 1'000;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TMultiproxyPresetDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled_methods", &TThis::EnabledMethods)
        .Default();

    registrar.Parameter("method_overrides", &TThis::MethodOverrides)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TMultiproxyDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("presets", &TThis::Presets)
        .Default();

    registrar.Parameter("cluster_presets", &TThis::ClusterPresets)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TApiServiceDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("client_cache", &TThis::ClientCache)
        .DefaultNew();
    registrar.Parameter("verbose_logging", &TThis::VerboseLogging)
        .Default(false);
    registrar.Parameter("enable_modify_rows_request_reordering", &TThis::EnableModifyRowsRequestReordering)
        .Default(true);
    registrar.Parameter("force_tracing", &TThis::ForceTracing)
        .Default(false);
    registrar.Parameter("read_buffer_row_count", &TThis::ReadBufferRowCount)
        .Default(10000);
    registrar.Parameter("read_buffer_data_weight", &TThis::ReadBufferDataWeight)
        .Default(16_MB);
    registrar.Parameter("user_access_validator", &TThis::UserAccessValidator)
        .Alias("security_manager")
        .DefaultNew();
    registrar.Parameter("structured_logging_main_topic", &TThis::StructuredLoggingMainTopic)
        .DefaultCtor([] {
            auto structuredLoggingMainTopic = New<TStructuredLoggingTopicDynamicConfig>();

            structuredLoggingMainTopic->SuppressedMethods =
                THashSet<TString>{
                    "ModifyRows",
                    "BatchModifyRows",
                    "LookupRows",
                    "VersionedLookupRows"
                };

            return structuredLoggingMainTopic;
        });
    registrar.Parameter("structured_logging_error_topic", &TThis::StructuredLoggingErrorTopic)
        .DefaultNew();
    registrar.Parameter("structured_logging_max_request_byte_size", &TThis::StructuredLoggingMaxRequestByteSize)
        .Default(10_KB);
    registrar.Parameter("structured_logging_query_truncation_size", &TThis::StructuredLoggingQueryTruncationSize)
        .Default(256);
    registrar.Parameter("query_corpus_reporter", &TThis::QueryCorpusReporter)
        .DefaultNew();
    registrar.Parameter("formats", &TThis::Formats)
        .Default();
    registrar.Parameter("enable_allocation_tags", &TThis::EnableAllocationTags)
        .Default(false);
    registrar.Parameter("multiproxy", &TThis::Multiproxy)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
