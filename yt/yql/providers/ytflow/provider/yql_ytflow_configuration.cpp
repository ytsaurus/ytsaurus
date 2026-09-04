#include "yql_ytflow_configuration.h"
#include "yql_ytflow_constants.h"

#include <util/string/split.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/string/join.h>


namespace NYql {

TString TYtflowSettings::GetRuntimeCluster() const
{
    if (auto runtimeCluster = RuntimeCluster.Get()) {
        return *runtimeCluster;
    } else if (auto cluster = Cluster.Get()) {
        return *cluster;
    }

    YQL_ENSURE(
        false,
        "Neither Ytflow.Cluster, nor Ytflow.RuntimeCluster pragmas are set");
}

TString TYtflowSettings::GetPipelinePath() const
{
    if (auto pipelinePath = PipelinePath.Get()) {
        return *pipelinePath;
    } else if (PipelineDirectory.Get() && PipelineName.Get()) {
        return Join('/', *PipelineDirectory.Get(), *PipelineName.Get());
    }

    YQL_ENSURE(
        false,
        "Neither Ytflow.PipelineDirectory and Ytflow.PipelineName, nor Ytflow.PipelinePath pragmas are set");
}

TString TYtflowSettings::GetYtConsumerPath() const
{
    if (auto ytConsumerPath = YtConsumerPath.Get()) {
        return *ytConsumerPath;
    }

    auto ytConsumerDirectoryString = YtConsumerDirectory.Get().GetOrElse(
        Join('/', GetPipelinePath(), YTFLOW_SUBDIRECTORY, YT_CONSUMERS_SUBDIRECTORY));

    auto ytConsumerNameString = YtConsumerName.Get().GetOrElse(
        TString(DEFAULT_YT_CONSUMER_NAME));

    return Join('/', ytConsumerDirectoryString, ytConsumerNameString);
}

bool TYtflowSettings::GetYtConsumerVital() const
{
    // TODO(ngc224): move to service gateway config defaults
    return YtConsumerVital.Get().GetOrElse(false);
}

TString TYtflowSettings::GetYtProducerPath() const
{
    if (auto ytProducerPath = YtProducerPath.Get()) {
        return *ytProducerPath;
    }

    auto ytProducerDirectoryString = YtProducerDirectory.Get().GetOrElse(
        Join('/', GetPipelinePath(), YTFLOW_SUBDIRECTORY, YT_PRODUCERS_SUBDIRECTORY));

    auto ytProducerNameString = YtProducerName.Get().GetOrElse(
        TString(DEFAULT_YT_PRODUCER_NAME));

    return Join('/', ytProducerDirectoryString, ytProducerNameString);
}

bool TYtflowSettings::GetEnableComputationPatternResources() const
{
    // Service configurations set this explicitly. Keep the conservative fallback
    // for standalone embedders that do not apply service defaults.
    return EnableComputationPatternResources.Get().GetOrElse(false);
}

TYtflowConfiguration::TYtflowConfiguration()
{
    REGISTER_SETTING(*this, Auth);

    REGISTER_SETTING(*this, Cluster);
    REGISTER_SETTING(*this, PathPrefix);

    REGISTER_SETTING(*this, TabletCellBundle);
    REGISTER_SETTING(*this, Account);
    REGISTER_SETTING(*this, PrimaryMedium);
    REGISTER_SETTING(*this, Pool);

    REGISTER_SETTING(*this, GracefulUpdate);
    REGISTER_SETTING(*this, UpdateTimeout)
        .Lower(TDuration::Zero());

    REGISTER_SETTING(*this, _RpcTimeout)
        .Lower(TDuration::Zero());

    REGISTER_SETTING(*this, _MasterLockTimeout)
        .Lower(TDuration::Zero());

    REGISTER_SETTING(*this, _MasterLockPingPeriod)
        .Lower(TDuration::Zero());

    REGISTER_SETTING(*this, _ControllerConfig);

    REGISTER_SETTING(*this, _JobManagerConfig);

    REGISTER_SETTING(*this, _FiniteStreams);

    REGISTER_SETTING(*this, ControllerCount)
        .Lower(1);

    REGISTER_SETTING(*this, ControllerCpuLimit)
        .Lower(0);

    REGISTER_SETTING(*this, ControllerMemoryLimit)
        .Lower(1);

    REGISTER_SETTING(*this, ControllerRpcPort)
        .Upper(65535);

    REGISTER_SETTING(*this, ControllerMonitoringPort)
        .Upper(65535);

    REGISTER_SETTING(*this, _UseCpuAwareBalancer);

    auto allowedLogLevels = std::initializer_list<TString>{
        "trace", "debug", "info", "warning", "error", "alert", "fatal"
    };

    auto allowedCompressionMethods = std::initializer_list<TString>{
        "Gzip", "Zstd"
    };

    REGISTER_SETTING(*this, _ControllerWriteFullLogsToYT);
    REGISTER_SETTING(*this, _ControllerWriteLogsToFile);
    REGISTER_SETTING(*this, _ControllerEnableStderrLogging);
    REGISTER_SETTING(*this, _ControllerLogLevel)
        .Enum(allowedLogLevels);

    REGISTER_SETTING(*this, _ControllerLoggingEnableCompression);
    REGISTER_SETTING(*this, _ControllerLoggingCompressionMethod)
        .Enum(allowedCompressionMethods);

    REGISTER_SETTING(*this, _ControllerLoggingCompressionLevel);
    REGISTER_SETTING(*this, _ControllerLoggingMaxTotalSizeToKeep);
    REGISTER_SETTING(*this, _ControllerLoggingMaxSegmentCountToKeep);
    REGISTER_SETTING(*this, _ControllerLoggingMaxSegmentSize);
    REGISTER_SETTING(*this, _ControllerLoggingRotationPeriod);

    REGISTER_SETTING(*this, WorkerCount)
        .Lower(1);

    REGISTER_SETTING(*this, WorkerCpuLimit)
        .Lower(0);

    REGISTER_SETTING(*this, WorkerMemoryLimit)
        .Lower(1);

    REGISTER_SETTING(*this, WorkerRpcPort)
        .Upper(65535);

    REGISTER_SETTING(*this, WorkerMonitoringPort)
        .Upper(65535);

    REGISTER_SETTING(*this, EnableComputationPatternResources);

    REGISTER_SETTING(*this, _WorkerWriteLogsToYT);
    REGISTER_SETTING(*this, _WorkerWriteLogsToFile);
    REGISTER_SETTING(*this, _WorkerEnableStderrLogging);
    REGISTER_SETTING(*this, _WorkerLogLevel)
        .Enum(allowedLogLevels);

    REGISTER_SETTING(*this, _WorkerLoggingEnableCompression);
    REGISTER_SETTING(*this, _WorkerLoggingCompressionMethod)
        .Enum(allowedCompressionMethods);

    REGISTER_SETTING(*this, _WorkerLoggingCompressionLevel);
    REGISTER_SETTING(*this, _WorkerLoggingMaxTotalSizeToKeep);
    REGISTER_SETTING(*this, _WorkerLoggingMaxSegmentCountToKeep);
    REGISTER_SETTING(*this, _WorkerLoggingMaxSegmentSize);
    REGISTER_SETTING(*this, _WorkerLoggingRotationPeriod);

    REGISTER_SETTING(*this, _LogsDirectory);

    REGISTER_SETTING(*this, _NetworkProject);
    REGISTER_SETTING(*this, _MonitoringResolverTag);
    REGISTER_SETTING(*this, _MonitoringProject);
    REGISTER_SETTING(*this, _MonitoringCluster);
    REGISTER_SETTING(*this, _UIOrigin);

    REGISTER_SETTING(*this, YtPartitionCount);
    REGISTER_SETTING(*this, YtTtl);

    REGISTER_SETTING(*this, LookupJoinInflightRowLimit)
        .Lower(0);

    REGISTER_SETTING(*this, LookupJoinInflightLookupLimit)
        .Lower(0);

    REGISTER_SETTING(*this, LookupJoinLookupTimeout)
        .Lower(TDuration::Zero());

    REGISTER_SETTING(*this, LogbrokerAbcService);
    REGISTER_SETTING(*this, LogbrokerAbcId);
    REGISTER_SETTING(*this, LogbrokerResponsible);

    REGISTER_SETTING(*this, LogbrokerConsumerPath);
    REGISTER_SETTING(*this, LogbrokerConsumerImportant);
    REGISTER_SETTING(*this, LogbrokerConsumerAvailabilityPeriodSeconds);
    // duplicate enum values to break dependencies from non opensource code
    // TODO(ngc224): eliminate duplication
    REGISTER_SETTING(*this, LogbrokerConsumerLimitsMode)
        .Enum({"wait", "notify"});

    REGISTER_SETTING(*this, LogbrokerTopicPartitionCount);
    REGISTER_SETTING(*this, LogbrokerTopicRetentionPeriodSeconds);
    REGISTER_SETTING(*this, LogbrokerTopicAllowUnauthenticatedRead);
    REGISTER_SETTING(*this, LogbrokerTopicAllowUnauthenticatedWrite);
    REGISTER_SETTING(*this, LogbrokerTopicFederationAccount);
    REGISTER_SETTING(*this, LogbrokerTopicMaxPartitionsCount);
    REGISTER_SETTING(*this, LogbrokerTopicAutoPartitioningStabilizationWindowSeconds);
    REGISTER_SETTING(*this, LogbrokerTopicAutoPartitioningUpUtilizationPercent);
    REGISTER_SETTING(*this, LogbrokerTopicAutoPartitioningDownUtilizationPercent);
    REGISTER_SETTING(*this, LogbrokerTopicAutoPartitioningStrategy);
    REGISTER_SETTING(*this, LogbrokerTopicPartitionMetricsEnabled);

    REGISTER_SETTING(*this, LogbrokerSubject);
    REGISTER_SETTING(*this, LogbrokerSupportedCodecs)
        .Parser([](const TString& v) {
            TSet<TString> codecs;
            StringSplitter(v).SplitBySet(",;| ").ParseInto(&codecs);
            TSet<TString> res;

            // duplicate enum values to break dependencies from non opensource code
            // TODO(ngc224): eliminate duplication
            static const TSet<TString> supportedCodecs = {"RAW", "GZIP", "LZOP", "ZSTD"};

            for (const auto& codec : codecs) {
                auto normalizedCodec = ::to_upper(codec);
                if (!supportedCodecs.contains(normalizedCodec)) {
                    throw yexception() << "Codec '" << normalizedCodec << "' not supported, available options are: "
                        << JoinSeq(", ", supportedCodecs);
                }

                res.insert(std::move(normalizedCodec));
            }

            return res;
        });
    REGISTER_SETTING(*this, _LogbrokerConfigManagerPollingPeriod);

    REGISTER_SETTING(*this, _LogbrokerMirrorToCluster)
        .Parser([](const TString& v) {
            TSet<TString> res;
            StringSplitter(v).SplitBySet(",;| ").ParseInto(&res);
            return res;
        });

    REGISTER_SETTING(*this, LogbrokerWriteCompressionCodec)
        .Enum({"raw", "gzip", "lzop", "zstd"});

    REGISTER_SETTING(*this, LogbrokerWriteCompressionLevel);

    REGISTER_SETTING(*this, SolomonMetricNameLabel);
    REGISTER_SETTING(*this, SolomonWriteCompressionCodec)
        .Enum({"none", "deflate", "gzip"});
    REGISTER_SETTING(*this, SolomonSkipMetricsWithNullTimestamp);
    REGISTER_SETTING(*this, MoniumMetricNameLabel);
    REGISTER_SETTING(*this, MoniumMetricCollectionInterval)
        .Lower(TDuration::Zero());
    REGISTER_SETTING(*this, MoniumGrid)
        .Lower(TDuration::Zero());
    REGISTER_SETTING(*this, MoniumServiceMetricsTtl)
        .Lower(TDuration::Zero());
    REGISTER_SETTING(*this, MoniumClusterMetricsTtl)
        .Lower(TDuration::Zero());
    REGISTER_SETTING(*this, MoniumShardMetricsTtl)
        .Lower(TDuration::Zero());
    REGISTER_SETTING(*this, _MoniumListShardsPageSize)
        .Lower(0);
    REGISTER_SETTING(*this, MoniumPrependProjectToResourceIds);
    REGISTER_SETTING(*this, MoniumSkipMetricsWithNullTimestamp);
    REGISTER_SETTING(*this, _MoniumDriverSecure);
    REGISTER_SETTING(*this, _MoniumPrepareResources);

    REGISTER_SETTING(*this, _SwitchComputationNodeBufferSizeBytes);

    REGISTER_SETTING(*this, RuntimeCluster);

    REGISTER_SETTING(*this, PipelineDirectory);
    REGISTER_SETTING(*this, PipelineName);
    REGISTER_SETTING(*this, PipelinePath);

    REGISTER_SETTING(*this, YtConsumerDirectory);
    REGISTER_SETTING(*this, YtConsumerName);
    REGISTER_SETTING(*this, YtConsumerPath);
    REGISTER_SETTING(*this, YtConsumerVital);

    REGISTER_SETTING(*this, YtProducerDirectory);
    REGISTER_SETTING(*this, YtProducerName);
    REGISTER_SETTING(*this, YtProducerPath);

    REGISTER_SETTING(*this, _RunVanillaOperation);
    REGISTER_SETTING(*this, _DumpPipelineSpecToDirectory);
}

} // namespace NYql
