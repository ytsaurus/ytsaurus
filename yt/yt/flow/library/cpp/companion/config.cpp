#include "config.h"

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
