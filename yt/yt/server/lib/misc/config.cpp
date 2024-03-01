#include "config.h"

#include <yt/yt/library/coredumper/config.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

namespace NYT {

using namespace NYTree;
using namespace NApi::NNative;

////////////////////////////////////////////////////////////////////////////////

void TServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bus_server", &TThis::BusServer)
        .DefaultNew();
    registrar.Parameter("rpc_server", &TThis::RpcServer)
        .DefaultNew();
    registrar.Parameter("core_dumper", &TThis::CoreDumper)
        .Default();

    registrar.Parameter("rpc_port", &TThis::RpcPort)
        .Default(0)
        .GreaterThanOrEqual(0)
        .LessThan(65536);
    registrar.Parameter("tvm_only_rpc_port", &TThis::TvmOnlyRpcPort)
        .Default(0)
        .GreaterThanOrEqual(0)
        .LessThan(65536);
    registrar.Parameter("monitoring_port", &TThis::MonitoringPort)
        .Default(0)
        .GreaterThanOrEqual(0)
        .LessThan(65536);

    registrar.Postprocessor([] (TThis* config) {
        if (config->RpcPort > 0) {
            if (config->BusServer->Port || config->BusServer->UnixDomainSocketPath) {
                THROW_ERROR_EXCEPTION("Explicit socket configuration for bus server is forbidden");
            }
            config->BusServer->Port = config->RpcPort;
        }
    });
}

NHttp::TServerConfigPtr TServerConfig::CreateMonitoringHttpServerConfig()
{
    auto config = New<NHttp::TServerConfig>();
    config->Port = MonitoringPort;
    config->BindRetryCount = BusServer->BindRetryCount;
    config->BindRetryBackoff = BusServer->BindRetryBackoff;
    config->ServerName = "HttpMon";
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TNativeServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_connection", &TThis::ClusterConnection);

    registrar.Parameter("cluster_connection_dynamic_config_mode", &TThis::ClusterConnectionDynamicConfigPolicy)
        .Default(EClusterConnectionDynamicConfigPolicy::FromStaticConfig);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskLocationConfig::ApplyDynamicInplace(const TDiskLocationDynamicConfig& dynamicConfig)
{
    UpdateYsonStructField(MinDiskSpace, dynamicConfig.MinDiskSpace);
}

void TDiskLocationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .NonEmpty();
    registrar.Parameter("min_disk_space", &TThis::MinDiskSpace)
        .GreaterThanOrEqual(0)
        .Default();
    registrar.Parameter("disk_family", &TThis::DiskFamily)
        .Default("UNKNOWN");
    registrar.Parameter("device_name", &TThis::DeviceName)
        .Default("UNKNOWN");
    registrar.Parameter("device_model", &TThis::DeviceModel)
        .Default("UNKNOWN");
}

////////////////////////////////////////////////////////////////////////////////

void TDiskLocationDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("min_disk_space", &TThis::MinDiskSpace)
        .GreaterThanOrEqual(0)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TDiskHealthCheckerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("check_period", &TThis::CheckPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("test_size", &TThis::TestSize)
        .InRange(0, 1_GB)
        .Default(1_MB);
    registrar.Parameter("timeout", &TThis::Timeout)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TFormatConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("default_attributes", &TThis::DefaultAttributes)
        .Default(NYTree::GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
}

////////////////////////////////////////////////////////////////////////////////

void TFormatConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("user_overrides", &TThis::UserOverrides)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TArchiveReporterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(true);
    registrar.Parameter("reporting_period", &TThis::ReportingPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("min_repeat_delay", &TThis::MinRepeatDelay)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("max_repeat_delay", &TThis::MaxRepeatDelay)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("max_items_in_batch", &TThis::MaxItemsInBatch)
        .Default(1000);
}

////////////////////////////////////////////////////////////////////////////////

void TArchiveHandlerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_in_progress_data_size", &TThis::MaxInProgressDataSize)
        .Default(250_MB);
    registrar.Parameter("path", &TThis::Path)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TJobReporterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("job_handler", &TThis::JobHandler)
        .DefaultNew();
    registrar.Parameter("operation_id_handler", &TThis::OperationIdHandler)
        .DefaultNew();
    registrar.Parameter("job_spec_handler", &TThis::JobSpecHandler)
        .DefaultNew();
    registrar.Parameter("job_stderr_handler", &TThis::JobStderrHandler)
        .DefaultNew();
    registrar.Parameter("job_fail_context_handler", &TThis::JobFailContextHandler)
        .DefaultNew();
    registrar.Parameter("job_profile_handler", &TThis::JobProfileHandler)
        .DefaultNew();

    registrar.Parameter("user", &TThis::User)
        .Default(NRpc::RootUserName);
    registrar.Parameter("report_statistics_lz4", &TThis::ReportStatisticsLz4)
        .Default(false);

    registrar.Parameter("max_in_progress_job_data_size", &TThis::MaxInProgressJobDataSize)
        .Default();
    registrar.Parameter("max_in_progress_operation_id_data_size", &TThis::MaxInProgressOperationIdDataSize)
        .Default();
    registrar.Parameter("max_in_progress_job_spec_data_size", &TThis::MaxInProgressJobSpecDataSize)
        .Default();
    registrar.Parameter("max_in_progress_job_stderr_data_size", &TThis::MaxInProgressJobStderrDataSize)
        .Default();
    registrar.Parameter("max_in_progress_job_fail_context_data_size", &TThis::MaxInProgressJobFailContextDataSize)
        .Default();

    registrar.Parameter("enable_job_reporter", &TThis::EnableJobReporter)
        .Default(true);
    registrar.Parameter("enable_job_spec_reporter", &TThis::EnableJobSpecReporter)
        .Default(true);
    registrar.Parameter("enable_job_stderr_reporter", &TThis::EnableJobStderrReporter)
        .Default(true);
    registrar.Parameter("enable_job_profile_reporter", &TThis::EnableJobProfileReporter)
        .Default(true);
    registrar.Parameter("enable_job_fail_context_reporter", &TThis::EnableJobFailContextReporter)
        .Default(true);

    registrar.Preprocessor([] (TThis* config) {
        config->OperationIdHandler->MaxInProgressDataSize = 10_MB;

        config->JobHandler->Path = NScheduler::GetOperationsArchiveJobsPath();
        config->OperationIdHandler->Path = NScheduler::GetOperationsArchiveOperationIdsPath();
        config->JobSpecHandler->Path = NScheduler::GetOperationsArchiveJobSpecsPath();
        config->JobStderrHandler->Path = NScheduler::GetOperationsArchiveJobStderrsPath();
        config->JobFailContextHandler->Path = NScheduler::GetOperationsArchiveJobFailContextsPath();
        config->JobProfileHandler->Path = NScheduler::GetOperationsArchiveJobProfilesPath();
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->MaxInProgressJobDataSize) {
            config->JobHandler->MaxInProgressDataSize = *config->MaxInProgressJobDataSize;
        }
        if (config->MaxInProgressOperationIdDataSize) {
            config->OperationIdHandler->MaxInProgressDataSize = *config->MaxInProgressOperationIdDataSize;
        }
        if (config->MaxInProgressJobSpecDataSize) {
            config->JobSpecHandler->MaxInProgressDataSize = *config->MaxInProgressJobSpecDataSize;
        }
        if (config->MaxInProgressJobStderrDataSize) {
            config->JobStderrHandler->MaxInProgressDataSize = *config->MaxInProgressJobStderrDataSize;
        }
        if (config->MaxInProgressJobFailContextDataSize) {
            config->JobFailContextHandler->MaxInProgressDataSize = *config->MaxInProgressJobFailContextDataSize;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void THeapProfilerTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("allocation_size", &TThis::AllocationSize)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(100_GB)
        .Default(0);
    registrar.Parameter("allocation_release_delay", &TThis::AllocationReleaseDelay)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
