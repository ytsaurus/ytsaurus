#include "config.h"

#include <yt/yt/core/http/config.h>
#include <yt/yt/core/https/config.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <yt/yt/library/profiling/solomon/config.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

void TCompanionMonitoringConfig::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TThis* config) {
        config->EnableSolomonAggregates = true;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TCompanionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("port", &TThis::Port)
        .Default(0)
        .GreaterThanOrEqual(0)
        .LessThan(65536);
    registrar.Parameter("monitoring_port", &TThis::MonitoringPort)
        .Default(0)
        .GreaterThanOrEqual(0)
        .LessThan(65536);
    registrar.Parameter("companion_process_count", &TThis::CompanionProcessCount)
        .Default(0)
        .GreaterThanOrEqual(0);
    registrar.Parameter("http_client_config", &TThis::HttpClientConfig)
        .DefaultNew();
    registrar.Parameter("https_client_config", &TThis::HttpsClientConfig)
        .DefaultNew();
    registrar.Parameter("http_poller_threads", &TThis::HttpPollerThreads)
        .GreaterThan(0)
        .Default(1);

    registrar.Postprocessor([] (TThis* config) {
        const auto& httpsConfig = config->HttpsClientConfig;
        if (!httpsConfig || !httpsConfig->Credentials || !httpsConfig->Credentials->PrivateKey) {
            return;
        }

        const auto& privateKey = httpsConfig->Credentials->PrivateKey;
        THROW_ERROR_EXCEPTION_UNLESS(
            privateKey->FileName && !privateKey->EnvironmentVariable && !privateKey->Value,
            "Companion HTTPS client private key must use \"file_name\"");
    });
}

////////////////////////////////////////////////////////////////////////////////

void TCompanionExecutionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_url", &TThis::ClusterUrl)
        .Default();
    registrar.Parameter("pipeline_path", &TThis::PipelinePath)
        .Default();
    registrar.Parameter("monitoring", &TThis::Monitoring)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TCompanionExecutionConfigPtr BuildCompanionExecutionConfig(
    const TCompanionConfigPtr& userConfig,
    const std::string& clusterUrl,
    const NYPath::TYPath& pipelinePath,
    const NProfiling::TSolomonExporterConfigPtr& solomonExporterConfig)
{
    auto config = ConvertTo<TCompanionExecutionConfigPtr>(userConfig);
    config->ClusterUrl = clusterUrl;
    config->PipelinePath = pipelinePath;
    if (solomonExporterConfig) {
        config->Monitoring = ConvertTo<TCompanionMonitoringConfigPtr>(solomonExporterConfig);
    }
    config->Postprocess();
    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
