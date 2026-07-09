#include "config.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow::NCompanion {

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
    registrar.Parameter("job_ttl_seconds", &TThis::JobTtlSeconds)
        .Default(600)
        .GreaterThanOrEqual(0);
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
}

////////////////////////////////////////////////////////////////////////////////

TCompanionExecutionConfigPtr BuildCompanionExecutionConfig(
    const TCompanionConfigPtr& userConfig,
    const std::string& clusterUrl,
    const NYPath::TYPath& pipelinePath)
{
    auto config = ConvertTo<TCompanionExecutionConfigPtr>(userConfig);
    config->ClusterUrl = clusterUrl;
    config->PipelinePath = pipelinePath;
    config->Postprocess();
    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
