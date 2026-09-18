#include "yql_ytflow_worker_config.h"
#include "yql_ytflow_config_clusters.h"
#include "yql_ytflow_utils.h"

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/yt/string/format.h>

#include <yql/essentials/core/yql_user_data_storage.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yql/providers/ytflow/provider/yql_ytflow_configuration.h>

#include <yt/yt/client/ypath/rich.h>
#include <yt/yt/core/ypath/helpers.h>

#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/system/tempfile.h>


namespace NYql::NYtflow {

namespace {

NYT::TNode GetCommonExcludeLogCategories()
{
    return NYT::TNode::CreateList()
        .Add("Bus")
        .Add("Dns")
        .Add("Concurrency")
        .Add("QueryClient")
        .Add("Profiling")
        .Add("RpcClient")
        .Add("Monitoring")
        .Add("Net")
        .Add("Solomon")
        .Add("Jaeger")
        .Add("RpcProxyClient")
        .Add("RpcServer")
        .Add("Dns")
        .Add("BufferMetrics");
}

NYT::TNode GetCommonLogManagerConfig()
{
    return NYT::TNode::CreateMap()
        ("rules", NYT::TNode::CreateList())
        ("writers", NYT::TNode::CreateMap());
}

void EnableStderrLogging(
    NYT::TNode& logManagerConfig,
    const TString& logLevel)
{
    logManagerConfig["rules"].Add(NYT::TNode::CreateMap()
        ("exclude_categories", GetCommonExcludeLogCategories())
        ("min_level", logLevel)
        ("writers", NYT::TNode::CreateList()
            .Add("Stderr")));

    logManagerConfig["writers"]
        ("Stderr", NYT::TNode::CreateMap()
            ("type", "stderr"));
}

void EnableQueueLogging(
    NYT::TNode& logManagerConfig,
    const TString& writerName,
    const TString& logPath,
    const TString& logLevel)
{
    logManagerConfig["rules"].Add(NYT::TNode::CreateMap()
        ("exclude_categories", GetCommonExcludeLogCategories())
        ("min_level", logLevel)
        ("writers", NYT::TNode::CreateList()
            .Add(writerName)));

    logManagerConfig["writers"]
        (writerName, NYT::TNode::CreateMap()
            ("type", "queue")
            ("queue_path", logPath));
}

void EnableFileLogging(
    NYT::TNode& logManagerConfig,
    const TString& writerName,
    const TString& logPath,
    const TString& logLevel,
    bool enableCompression,
    const TString& compressionMethod,
    int compressionLevel,
    i64 maxTotalSizeToKeep,
    i64 maxSegmentCountToKeep,
    TMaybe<i64> maxSegmentSize,
    TMaybe<TDuration> rotationPeriod)
{
    logManagerConfig["rules"].Add(NYT::TNode::CreateMap()
        ("exclude_categories", GetCommonExcludeLogCategories())
        ("min_level", logLevel)
        ("writers", NYT::TNode::CreateList()
            .Add(writerName)));

    auto rotationPolicy = NYT::TNode::CreateMap()
        ("max_total_size_to_keep", maxTotalSizeToKeep)
        ("max_segment_count_to_keep", maxSegmentCountToKeep);

    if (maxSegmentSize) {
        rotationPolicy
            ("max_segment_size", *maxSegmentSize);
    }

    if (rotationPeriod) {
        rotationPolicy
            ("rotation_period", rotationPeriod->MilliSeconds());
    }

    logManagerConfig["writers"]
        (writerName, NYT::TNode::CreateMap()
            ("type", "file")
            ("file_name", logPath)
            ("enable_compression", enableCompression)
            ("compression_method", compressionMethod)
            ("compression_level", compressionLevel)
            ("rotation_policy", rotationPolicy));
}

NYT::TNode MakeControllerLogManagerConfig(
    const TYtflowSettings& config,
    const TString& absolutePipelinePath,
    const TString& clusterRealName,
    const TString& logsDirectory)
{
    auto logManagerConfig = GetCommonLogManagerConfig();

    auto logLevel = config._ControllerLogLevel.Get();
    YQL_ENSURE(logLevel, "Ytflow._ControllerLogLevel system setting is not set");

    if (auto writeFullLogsToYT = config._ControllerWriteFullLogsToYT.Get();
        writeFullLogsToYT && writeFullLogsToYT.GetRef()
    ) {
        NYT::NYPath::TRichYPath logPath = NYT::NYPath::YPathJoin(
            absolutePipelinePath,
            CONTROLLER_LOGS_TABLE);

        logPath.SetCluster(clusterRealName);

        EnableQueueLogging(
            logManagerConfig,
            "ControllerFullLogWriter",
            ToString(logPath),
            *logLevel);
    } else if (
        auto enableStderrLogging = config._ControllerEnableStderrLogging.Get();
        enableStderrLogging && enableStderrLogging.GetRef()
    ) {
        EnableStderrLogging(logManagerConfig, *logLevel);
    }

    auto writeLogsToFile = config._ControllerWriteLogsToFile.Get();
    YQL_ENSURE(writeLogsToFile, "Ytflow._ControllerWriteLogsToFile system setting is not set");

    if (!*writeLogsToFile) {
        return logManagerConfig;
    }

    auto enableCompression = config._ControllerLoggingEnableCompression.Get();
    YQL_ENSURE(enableCompression, "Ytflow._ControllerLoggingEnableCompression system setting is not set");

    auto compressionMethod = config._ControllerLoggingCompressionMethod.Get();
    YQL_ENSURE(compressionMethod, "Ytflow._ControllerLoggingCompressionMethod system setting is not set");

    auto compressionLevel = config._ControllerLoggingCompressionLevel.Get();
    YQL_ENSURE(compressionLevel, "Ytflow._ControllerLoggingCompressionLevel system setting is not set");

    auto maxTotalSizeToKeep = config._ControllerLoggingMaxTotalSizeToKeep.Get();
    YQL_ENSURE(maxTotalSizeToKeep, "Ytflow._ControllerLoggingMaxTotalSizeToKeep system setting is not set");

    auto maxSegmentCountToKeep = config._ControllerLoggingMaxSegmentCountToKeep.Get();
    YQL_ENSURE(maxSegmentCountToKeep, "Ytflow._ControllerLoggingMaxSegmentCountToKeep system setting is not set");

    auto maxSegmentSize = config._ControllerLoggingMaxSegmentSize.Get();
    auto rotationPeriod = config._ControllerLoggingRotationPeriod.Get();

    YQL_ENSURE(
       maxSegmentSize.Defined() ^ rotationPeriod.Defined(),
       "Strictly one of Ytflow._ControllerLoggingMaxSegmentSize or "
       "Ytflow._ControllerLoggingRotationPeriod system settings should be set");

    EnableFileLogging(
        logManagerConfig,
        "ControllerFileWriter",
        logsDirectory + "/controller.log",
        *logLevel,
        *enableCompression,
        *compressionMethod,
        *compressionLevel,
        *maxTotalSizeToKeep,
        *maxSegmentCountToKeep,
        maxSegmentSize,
        rotationPeriod);

    return logManagerConfig;
}

NYT::TNode MakeWorkerLogManagerConfig(
    const TYtflowSettings& config,
    const TString& absolutePipelinePath,
    const TString& clusterRealName,
    const TString& logsDirectory)
{
    auto logManagerConfig = GetCommonLogManagerConfig();

    auto logLevel = config._WorkerLogLevel.Get();
    YQL_ENSURE(logLevel, "Ytflow._WorkerLogLevel system setting is not set");

    if (auto writeLogsToYT = config._WorkerWriteLogsToYT.Get();
        writeLogsToYT && writeLogsToYT.GetRef()
    ) {
        NYT::NYPath::TRichYPath logPath = NYT::NYPath::YPathJoin(
            absolutePipelinePath,
            WORKER_LOGS_TABLE);

        logPath.SetCluster(clusterRealName);

        EnableQueueLogging(
            logManagerConfig,
            "WorkerLogWriter",
            ToString(logPath),
            *logLevel);
    } else if (
        auto enableStderrLogging = config._WorkerEnableStderrLogging.Get();
        enableStderrLogging && enableStderrLogging.GetRef()
    ) {
        EnableStderrLogging(logManagerConfig, *logLevel);
    }

    auto writeLogsToFile = config._WorkerWriteLogsToFile.Get();
    YQL_ENSURE(writeLogsToFile, "Ytflow._WorkerWriteLogsToFile system setting is not set");

    if (!*writeLogsToFile) {
        return logManagerConfig;
    }

    auto enableCompression = config._WorkerLoggingEnableCompression.Get();
    YQL_ENSURE(enableCompression, "Ytflow._WorkerLoggingEnableCompression system setting is not set");

    auto compressionMethod = config._WorkerLoggingCompressionMethod.Get();
    YQL_ENSURE(compressionMethod, "Ytflow._WorkerLoggingCompressionMethod system setting is not set");

    auto compressionLevel = config._WorkerLoggingCompressionLevel.Get();
    YQL_ENSURE(compressionLevel, "Ytflow._WorkerLoggingCompressionLevel system setting is not set");

    auto maxTotalSizeToKeep = config._WorkerLoggingMaxTotalSizeToKeep.Get();
    YQL_ENSURE(maxTotalSizeToKeep, "Ytflow._WorkerLoggingMaxTotalSizeToKeep system setting is not set");

    auto maxSegmentCountToKeep = config._WorkerLoggingMaxSegmentCountToKeep.Get();
    YQL_ENSURE(maxSegmentCountToKeep, "Ytflow._WorkerLoggingMaxSegmentCountToKeep system setting is not set");

    auto maxSegmentSize = config._WorkerLoggingMaxSegmentSize.Get();
    auto rotationPeriod = config._WorkerLoggingRotationPeriod.Get();

    YQL_ENSURE(
       maxSegmentSize.Defined() ^ rotationPeriod.Defined(),
       "Strictly one of Ytflow._WorkerLoggingMaxSegmentSize or "
       "Ytflow._WorkerLoggingRotationPeriod settings should be set");

    EnableFileLogging(
        logManagerConfig,
        "WorkerFileWriter",
        logsDirectory + "/worker.log",
        *logLevel,
        *enableCompression,
        *compressionMethod,
        *compressionLevel,
        *maxTotalSizeToKeep,
        *maxSegmentCountToKeep,
        maxSegmentSize,
        rotationPeriod);

    return logManagerConfig;
}

} // anonymous namespace

TMaybe<NYT::TNode> NPrivate::SerializeUseCpuAwareBalancer(
    TMaybe<bool> useCpuAwareBalancer)
{
    if (!useCpuAwareBalancer) {
        return {};
    }

    return NYT::TNode::CreateMap()
        ("job_manager", NYT::TNode::CreateMap()
            ("use_cpu_aware_balancer", *useCpuAwareBalancer));
}

NYT::TNode MakeWorkerConfig(
    const TYqlOperationOptions& operationOptions,
    const TYtflowSettings& config,
    const TConfigClusters& configClusters,
    const TUserDataTable& userDataBlocks,
    const TVector<TFile>& files)
{
    auto logsDirectory = config._LogsDirectory.Get();
    YQL_ENSURE(logsDirectory, "Ytflow._LogsDirectory system setting is not set");

    auto clusterRealName = NPrivate::ResolvePipelineClusterName(config, configClusters);
    auto absolutePipelinePath = NPrivate::GetCanonicalPipelinePath(config);

    auto gracefulUpdate = config.GracefulUpdate.Get();
    YQL_ENSURE(gracefulUpdate, "Ytflow.GracefulUpdate pragma is not set");

    auto controllerCount = config.ControllerCount.Get();
    YQL_ENSURE(controllerCount, "Ytflow.ControllerCount pragma is not set");

    auto controllerCpuLimit = config.ControllerCpuLimit.Get();
    YQL_ENSURE(controllerCpuLimit, "Ytflow.ControllerCpuLimit pragma is not set");

    auto controllerMemoryLimit = config.ControllerMemoryLimit.Get();
    YQL_ENSURE(controllerMemoryLimit, "Ytflow.ControllerMemoryLimit pragma is not set");

    auto controllerRpcPort = config.ControllerRpcPort.Get();
    YQL_ENSURE(controllerRpcPort, "Ytflow.ControllerRpcPort pragma is not set");

    auto controllerMonitoringPort = config.ControllerMonitoringPort.Get();
    YQL_ENSURE(controllerMonitoringPort, "Ytflow.ControllerMonitoringPort pragma is not set");

    auto workerCount = config.WorkerCount.Get();
    YQL_ENSURE(workerCount, "Ytflow.WorkerCount pragma is not set");

    auto workerCpuLimit = config.WorkerCpuLimit.Get();
    YQL_ENSURE(workerCpuLimit, "Ytflow.WorkerCpuLimit pragma is not set");

    auto workerMemoryLimit = config.WorkerMemoryLimit.Get();
    YQL_ENSURE(workerMemoryLimit, "Ytflow.WorkerMemoryLimit pragma is not set");

    auto workerRpcPort = config.WorkerRpcPort.Get();
    YQL_ENSURE(workerRpcPort, "Ytflow.WorkerRpcPort pragma is not set");

    auto workerMonitoringPort = config.WorkerMonitoringPort.Get();
    YQL_ENSURE(workerMonitoringPort, "Ytflow.WorkerMonitoringPort pragma is not set");

    auto localFilesNode = NYT::TNode::CreateList();
    for (const auto& [userDataKey, userDataBlock] : userDataBlocks) {
        localFilesNode.Add(NYT::TNode::CreateMap()
            ("name", TUserDataStorage::MakeRelativeName(userDataKey.Alias()))
            ("path", TString(userDataBlock.FrozenFile->GetPath())));
    }

    for (const auto& file : files) {
        YQL_ENSURE(file.Disposition == EFileDisposition::Path);

        localFilesNode.Add(NYT::TNode::CreateMap()
            ("name", file.Name)
            ("path", file.Content));
    }

    auto workerConfig = NYT::TNode::CreateMap()
        ("cluster_url", clusterRealName)
        ("path", absolutePipelinePath)
        ("rpc_port", *controllerRpcPort)
        ("monitoring_port", *controllerMonitoringPort)
        ("graceful_update", *gracefulUpdate)
        ("controller_count", *controllerCount)
        ("controller_cpu_limit", *controllerCpuLimit)
        ("controller_memory_limit", static_cast<ui64>(*controllerMemoryLimit))
        ("controller_rpc_port", *controllerRpcPort)
        ("controller_monitoring_port", *controllerMonitoringPort)
        ("worker_count", *workerCount)
        ("worker_cpu_limit", *workerCpuLimit)
        ("worker_memory_limit", static_cast<ui64>(*workerMemoryLimit))
        ("worker_rpc_port", *workerRpcPort)
        ("worker_monitoring_port", *workerMonitoringPort)
        ("local_files", std::move(localFilesNode))
        ("enable_phdr_cache", false)
        ("runtime_cluster", config.GetRuntimeCluster())
        ("controller_log_manager_config", MakeControllerLogManagerConfig(
            config,
            absolutePipelinePath,
            clusterRealName,
            *logsDirectory))
        ("worker_log_manager_config", MakeWorkerLogManagerConfig(
            config,
            absolutePipelinePath,
            clusterRealName,
            *logsDirectory));

    if (auto pool = config.Pool.Get()) {
        workerConfig = workerConfig
            ("pool", *pool);
    }

    if (auto networkProject = config._NetworkProject.Get()) {
        workerConfig = workerConfig
            ("network_project", *networkProject)
            ("force_ipv6_as_node_address", true);
    }

    if (auto monitoringResolverTag = config._MonitoringResolverTag.Get()) {
        workerConfig = workerConfig
            ("solomon_resolver_tag", *monitoringResolverTag);
    }

    if (auto controllerConfig = config._ControllerConfig.Get()) {
        workerConfig = workerConfig
            ("controller", NYT::NodeFromYsonString(*controllerConfig));
    }

    auto dynamicPipelineSpec = NPrivate::SerializeUseCpuAwareBalancer(
        config._UseCpuAwareBalancer.Get());

    if (auto jobManagerConfig = config._JobManagerConfig.Get()) {
        auto overrides = NYT::NodeFromYsonString(*jobManagerConfig);
        if (!dynamicPipelineSpec) {
            dynamicPipelineSpec = NYT::TNode::CreateMap()("job_manager", std::move(overrides));
        } else {
            auto& jobManager = (*dynamicPipelineSpec)["job_manager"];
            for (const auto& [key, value] : overrides.AsMap()) {
                jobManager[key] = value;
            }
        }
    }
    if (dynamicPipelineSpec) {
        workerConfig = workerConfig
            ("dynamic_pipeline_spec", *dynamicPipelineSpec);
    }

    workerConfig = workerConfig
        ("title", NPrivate::MakeOperationTitle(operationOptions))
        ("description", NPrivate::MakeOperationDescription(
            operationOptions, config, configClusters));

    return workerConfig;
}

} // namespace NYql::NYtflow
