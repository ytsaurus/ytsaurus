#include "config.h"

#include "private.h"

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

void TShardConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("filter", &TThis::Filter)
        .Default();

    registrar.Parameter("grid_step", &TThis::GridStep)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TScrapeOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("convert_counters_to_rate_gauge", &TThis::ConvertCountersToRateGauge)
        .Default(false);
    // NB: Even though the aliases are similar to the options above, their intent is different and corresponds to the new option below.
    registrar.Parameter("force_convert_counters_to_rate_gauge_for_solomon", &TThis::ForceConvertCountersToRateGaugeForSolomon)
        .Alias("convert_counters_to_rate_for_solomon")
        .Alias("convert_counters_to_rate")
        .Default(true);

    registrar.Parameter("convert_counters_to_delta_gauge", &TThis::ConvertCountersToDeltaGauge)
        .Default(false);
    registrar.Parameter("force_convert_counters_to_delta_gauge_for_solomon", &TThis::ForceConvertCountersToDeltaGaugeForSolomon)
        .Default(false);

    registrar.Parameter("rename_converted_counters", &TThis::RenameConvertedCounters)
        .Default(true);

    registrar.Parameter("enable_aggregation_workaround", &TThis::EnableAggregationWorkaround)
        .Default(false);
    // NB: Even though the alias is similar to the option above, its intent is different and corresponds to the new option below.
    registrar.Parameter("force_enable_aggregation_workaround_for_solomon", &TThis::ForceEnableAggregationWorkaroundForSolomon)
        .Alias("enable_solomon_aggregation_workaround")
        .Default(true);

    registrar.Parameter("mark_aggregates", &TThis::MarkAggregates)
        .Default(true);

    registrar.Parameter("strip_sensors_name_prefix", &TThis::StripSensorsNamePrefix)
        .Default(false);

    registrar.Parameter("sensor_component_delimiter", &TThis::SensorComponentDelimiter)
        .Default(".");
    registrar.Parameter("convert_sensor_component_names_to_camel_case", &TThis::ConvertSensorComponentNamesToCamelCase)
        .Default(false);

    registrar.Parameter("add_metric_type_label", &TThis::AddMetricTypeLabel)
        .Default(false);

    // We parse this config from GET-request parameters which also contains some other options.
    // In order not to clash with them in the future, we forbid some parameter names that already
    // have another meaning.
    registrar.Preprocessor([] (TThis* config) {
        auto registeredKeys = config->GetRegisteredKeys();
        for (const auto& key : TThis::ForbiddenParameterNames) {
            YT_VERIFY(!registeredKeys.contains(key));
        }
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->ConvertCountersToRateGauge && config->ConvertCountersToDeltaGauge) {
            THROW_ERROR_EXCEPTION("Options \"convert_counters_to_rate_gauge\" and \"convert_counters_to_delta_gauge\" cannot be both set to true");
        }

        for (auto c : config->SensorComponentDelimiter) {
            if (!std::isalnum(c) && !std::strchr(SensorComponentDelimiterSpecialCharacterWhitelist, c)) {
                THROW_ERROR_EXCEPTION("Character %Qv is not allowed in \"sensor_component_delimiter\"", c)
                    << TErrorAttribute("delimiter", config->SensorComponentDelimiter);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSolomonExporterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("grid_step", &TThis::GridStep)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("linger_timeout", &TThis::LingerTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("window_size", &TThis::WindowSize)
        .Default(12);

    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Default(1);
    registrar.Parameter("encoding_thread_pool_size", &TThis::EncodingThreadPoolSize)
        .Default(1);
    registrar.Parameter("thread_pool_polling_period", &TThis::ThreadPoolPollingPeriod)
        .Default(TDuration::MilliSeconds(10));
    registrar.Parameter("encoding_thread_pool_polling_period", &TThis::EncodingThreadPoolPollingPeriod)
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("export_summary", &TThis::ExportSummary)
        .Default(false);
    registrar.Parameter("export_summary_as_max", &TThis::ExportSummaryAsMax)
        .Default(true);
    registrar.Parameter("export_summary_as_avg", &TThis::ExportSummaryAsAvg)
        .Default(false);

    registrar.Parameter("enable_self_profiling", &TThis::EnableSelfProfiling)
        .Default(true);

    registrar.Parameter("report_build_info", &TThis::ReportBuildInfo)
        .Default(true);

    registrar.Parameter("report_kernel_version", &TThis::ReportKernelVersion)
        .Default(true);

    registrar.Parameter("report_restart", &TThis::ReportRestart)
        .Default(true);

    registrar.Parameter("read_delay", &TThis::ReadDelay)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("host", &TThis::Host)
        .Default();

    registrar.Parameter("instance_tags", &TThis::InstanceTags)
        .Default();

    registrar.Parameter("shards", &TThis::Shards)
        .Default();

    registrar.Parameter("response_cache_ttl", &TThis::ResponseCacheTtl)
        .Default(TDuration::Minutes(2));

    registrar.Parameter("update_sensor_service_tree_period", &TThis::UpdateSensorServiceTreePeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("producer_collection_batch_size", &TThis::ProducerCollectionBatchSize)
        .Default(DefaultProducerCollectionBatchSize)
        .GreaterThan(0);

    registrar.Postprocessor([] (TThis* config) {
        if (config->LingerTimeout.GetValue() % config->GridStep.GetValue() != 0) {
            THROW_ERROR_EXCEPTION("\"linger_timeout\" must be multiple of \"grid_step\"");
        }
    });

    registrar.Postprocessor([] (TThis* config) {
        for (const auto& [name, shard] : config->Shards) {
            if (!shard->GridStep) {
                continue;
            }

            if (shard->GridStep < config->GridStep) {
                THROW_ERROR_EXCEPTION("shard \"grid_step\" must be greater than global \"grid_step\"");
            }

            if (shard->GridStep->GetValue() % config->GridStep.GetValue() != 0) {
                THROW_ERROR_EXCEPTION("shard \"grid_step\" must be multiple of global \"grid_step\"");
            }

            if (config->LingerTimeout.GetValue() % shard->GridStep->GetValue() != 0) {
                THROW_ERROR_EXCEPTION("\"linger_timeout\" must be multiple shard \"grid_step\"");
            }
        }
    });
}

TShardConfigPtr TSolomonExporterConfig::MatchShard(const std::string& sensorName)
{
    TShardConfigPtr matchedShard;
    int matchSize = -1;

    for (const auto& [name, config] : Shards) {
        for (auto prefix : config->Filter) {
            if (!sensorName.starts_with(prefix)) {
                continue;
            }

            if (static_cast<int>(prefix.size()) > matchSize) {
                matchSize = prefix.size();
                matchedShard = config;
            }
        }
    }

    return matchedShard;
}

ESummaryPolicy TSolomonExporterConfig::GetSummaryPolicy() const
{
    auto policy = ESummaryPolicy::Default;
    if (ExportSummary) {
        policy |= ESummaryPolicy::All;
    }
    if (ExportSummaryAsMax) {
        policy |= ESummaryPolicy::Max;
    }
    if (ExportSummaryAsAvg) {
        policy |= ESummaryPolicy::Avg;
    }

    return policy;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
