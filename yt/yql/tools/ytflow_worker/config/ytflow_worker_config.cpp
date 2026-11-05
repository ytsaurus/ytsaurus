#include "ytflow_worker_config.h"

#include <yt/yt/core/logging/config.h>


namespace NYql::NYtflow {

void TLocalFileConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("name", &TThis::Name)
        .Default();

    registrar.Parameter("path", &TThis::Path)
        .Default();
}

void TWorkerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("pipeline_spec", &TThis::PipelineSpec)
        .DefaultNew();

    registrar.Parameter("dynamic_pipeline_spec", &TThis::DynamicPipelineSpec)
        .DefaultNew();

    registrar.Parameter("graceful_update", &TThis::GracefulUpdate)
        .Default();

    registrar.Parameter("run_vanilla", &TThis::RunVanilla)
        .Default();

    registrar.Parameter("set_flow_core_target", &TThis::SetFlowCoreTarget)
        .Default(true);

    registrar.Parameter("controller_count", &TThis::ControllerCount)
        .Default();

    registrar.Parameter("controller_cpu_limit", &TThis::ControllerCpuLimit)
        .Default();

    registrar.Parameter("controller_memory_limit", &TThis::ControllerMemoryLimit)
        .Default();

    registrar.Parameter("controller_rpc_port", &TThis::ControllerRpcPort)
        .Default()
        .LessThan(65536);

    registrar.Parameter("controller_monitoring_port", &TThis::ControllerMonitoringPort)
        .Default()
        .LessThan(65536);

    registrar.Parameter("controller_log_manager_config", &TThis::ControllerLogManagerConfig)
        .DefaultNew();

    registrar.Parameter("worker_count", &TThis::WorkerCount)
        .Default();

    registrar.Parameter("worker_cpu_limit", &TThis::WorkerCpuLimit)
        .Default();

    registrar.Parameter("worker_memory_limit", &TThis::WorkerMemoryLimit)
        .Default();

    registrar.Parameter("worker_rpc_port", &TThis::WorkerRpcPort)
        .Default()
        .LessThan(65536);

    registrar.Parameter("worker_monitoring_port", &TThis::WorkerMonitoringPort)
        .Default()
        .LessThan(65536);

    registrar.Parameter("worker_log_manager_config", &TThis::WorkerLogManagerConfig)
        .DefaultNew();

    registrar.Parameter("local_files", &TThis::LocalFiles)
        .Default();

    registrar.Parameter("pool", &TThis::Pool)
        .Default();

    registrar.Parameter("runtime_cluster", &TThis::RuntimeCluster)
        .Default();

    registrar.Parameter("network_project", &TThis::NetworkProject)
        .Default();

    registrar.Parameter("solomon_resolver_tag", &TThis::SolomonResolverTag)
        .Default();

    registrar.Parameter("description", &TThis::Description)
        .Default();

    registrar.Parameter("title", &TThis::Title)
        .Default();
}

} // namespace NYql::NYtflow
