#pragma once

#include <library/cpp/string_utils/parse_size/parse_size.h>

#include <yql/essentials/providers/common/config/yql_dispatch.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <memory>


namespace NYql {

struct TYtflowSettings
{
public:
    using TConstPtr = std::shared_ptr<const TYtflowSettings>;

private:
    static constexpr NCommon::EConfSettingType Static = NCommon::EConfSettingType::Static;

public:
    NCommon::TConfSetting<TString, Static> Auth;

    NCommon::TConfSetting<TString, Static> Cluster;
    NCommon::TConfSetting<TString, Static> PathPrefix;

    NCommon::TConfSetting<TString, Static> TabletCellBundle;
    NCommon::TConfSetting<TString, Static> Account;
    NCommon::TConfSetting<TString, Static> PrimaryMedium;
    NCommon::TConfSetting<TString, Static> Pool;

    NCommon::TConfSetting<bool, Static> GracefulUpdate;
    NCommon::TConfSetting<TDuration, Static> UpdateTimeout;
    NCommon::TConfSetting<TDuration, Static> _RpcTimeout;
    NCommon::TConfSetting<TDuration, Static> _MasterLockTimeout;
    NCommon::TConfSetting<TDuration, Static> _MasterLockPingPeriod;

    // YSON map patched into the controller job's "controller" config section.
    NCommon::TConfSetting<TString, Static> _ControllerConfig;
    // YSON map merged into the dynamic pipeline spec's "job_manager" section.
    NCommon::TConfSetting<TString, Static> _JobManagerConfig;

    NCommon::TConfSetting<bool, Static> _FiniteStreams;

    NCommon::TConfSetting<uint64_t, Static> ControllerCount;
    NCommon::TConfSetting<double, Static> ControllerCpuLimit;
    NCommon::TConfSetting<NSize::TSize, Static> ControllerMemoryLimit;
    NCommon::TConfSetting<uint64_t, Static> ControllerRpcPort;
    NCommon::TConfSetting<uint64_t, Static> ControllerMonitoringPort;
    NCommon::TConfSetting<bool, Static> _UseCpuAwareBalancer;

    NCommon::TConfSetting<bool, Static> _ControllerWriteFullLogsToYT;
    NCommon::TConfSetting<bool, Static> _ControllerWriteLogsToFile;
    NCommon::TConfSetting<bool, Static> _ControllerEnableStderrLogging;
    NCommon::TConfSetting<TString, Static> _ControllerLogLevel;
    NCommon::TConfSetting<bool, Static> _ControllerLoggingEnableCompression;
    NCommon::TConfSetting<TString, Static> _ControllerLoggingCompressionMethod;
    NCommon::TConfSetting<int, Static> _ControllerLoggingCompressionLevel;
    NCommon::TConfSetting<NSize::TSize, Static> _ControllerLoggingMaxTotalSizeToKeep;
    NCommon::TConfSetting<i64, Static> _ControllerLoggingMaxSegmentCountToKeep;
    NCommon::TConfSetting<NSize::TSize, Static> _ControllerLoggingMaxSegmentSize;
    NCommon::TConfSetting<TDuration, Static> _ControllerLoggingRotationPeriod;

    NCommon::TConfSetting<uint64_t, Static> WorkerCount;
    NCommon::TConfSetting<double, Static> WorkerCpuLimit;
    NCommon::TConfSetting<NSize::TSize, Static> WorkerMemoryLimit;
    NCommon::TConfSetting<uint64_t, Static> WorkerRpcPort;
    NCommon::TConfSetting<uint64_t, Static> WorkerMonitoringPort;
    NCommon::TConfSetting<bool, Static> EnableComputationPatternResources;

    NCommon::TConfSetting<bool, Static> _WorkerWriteLogsToYT;
    NCommon::TConfSetting<bool, Static> _WorkerWriteLogsToFile;
    NCommon::TConfSetting<bool, Static> _WorkerEnableStderrLogging;
    NCommon::TConfSetting<TString, Static> _WorkerLogLevel;
    NCommon::TConfSetting<bool, Static> _WorkerLoggingEnableCompression;
    NCommon::TConfSetting<TString, Static> _WorkerLoggingCompressionMethod;
    NCommon::TConfSetting<int, Static> _WorkerLoggingCompressionLevel;
    NCommon::TConfSetting<NSize::TSize, Static> _WorkerLoggingMaxTotalSizeToKeep;
    NCommon::TConfSetting<i64, Static> _WorkerLoggingMaxSegmentCountToKeep;
    NCommon::TConfSetting<NSize::TSize, Static> _WorkerLoggingMaxSegmentSize;
    NCommon::TConfSetting<TDuration, Static> _WorkerLoggingRotationPeriod;

    NCommon::TConfSetting<TString, Static> _LogsDirectory;

    NCommon::TConfSetting<TString, Static> _NetworkProject;
    NCommon::TConfSetting<TString, Static> _MonitoringResolverTag;
    NCommon::TConfSetting<TString, Static> _MonitoringProject;
    NCommon::TConfSetting<TString, Static> _MonitoringCluster;
    NCommon::TConfSetting<TString, Static> _UIOrigin;

    NCommon::TConfSetting<uint64_t, Static> YtPartitionCount;
    NCommon::TConfSetting<TDuration, Static> YtTtl;

    NCommon::TConfSetting<uint64_t, Static> LookupJoinInflightRowLimit;
    NCommon::TConfSetting<uint64_t, Static> LookupJoinInflightLookupLimit;
    NCommon::TConfSetting<TDuration, Static> LookupJoinLookupTimeout;

    NCommon::TConfSetting<TString, Static> LogbrokerAbcService;
    NCommon::TConfSetting<TString, Static> LogbrokerAbcId;
    NCommon::TConfSetting<TString, Static> LogbrokerResponsible;

    NCommon::TConfSetting<TString, Static> LogbrokerConsumerPath;
    NCommon::TConfSetting<bool, Static> LogbrokerConsumerImportant;
    NCommon::TConfSetting<uint64_t, Static> LogbrokerConsumerAvailabilityPeriodSeconds;
    NCommon::TConfSetting<TString, Static> LogbrokerConsumerLimitsMode;

    NCommon::TConfSetting<TString, Static> LogbrokerSubject;
    NCommon::TConfSetting<TSet<TString>, Static> LogbrokerSupportedCodecs;
    NCommon::TConfSetting<TDuration, Static> _LogbrokerConfigManagerPollingPeriod;

    NCommon::TConfSetting<uint64_t, Static> LogbrokerTopicPartitionCount;
    NCommon::TConfSetting<uint64_t, Static> LogbrokerTopicRetentionPeriodSeconds;
    NCommon::TConfSetting<bool, Static> LogbrokerTopicAllowUnauthenticatedRead;
    NCommon::TConfSetting<bool, Static> LogbrokerTopicAllowUnauthenticatedWrite;
    NCommon::TConfSetting<TString, Static> LogbrokerTopicFederationAccount;
    NCommon::TConfSetting<uint64_t, Static> LogbrokerTopicMaxPartitionsCount;
    NCommon::TConfSetting<uint64_t, Static> LogbrokerTopicAutoPartitioningStabilizationWindowSeconds;
    NCommon::TConfSetting<uint64_t, Static> LogbrokerTopicAutoPartitioningUpUtilizationPercent;
    NCommon::TConfSetting<uint64_t, Static> LogbrokerTopicAutoPartitioningDownUtilizationPercent;
    NCommon::TConfSetting<TString, Static> LogbrokerTopicAutoPartitioningStrategy;
    NCommon::TConfSetting<bool, Static> LogbrokerTopicPartitionMetricsEnabled;

    NCommon::TConfSetting<TSet<TString>, Static> _LogbrokerMirrorToCluster;

    NCommon::TConfSetting<TString, Static> LogbrokerWriteCompressionCodec;
    NCommon::TConfSetting<uint8_t, Static> LogbrokerWriteCompressionLevel;

    NCommon::TConfSetting<TString, Static> SolomonMetricNameLabel;
    NCommon::TConfSetting<TString, Static> SolomonWriteCompressionCodec;
    NCommon::TConfSetting<bool, Static> SolomonSkipMetricsWithNullTimestamp;
    NCommon::TConfSetting<TString, Static> MoniumMetricNameLabel;
    NCommon::TConfSetting<bool, Static> MoniumSkipMetricsWithNullTimestamp;
    NCommon::TConfSetting<bool, Static> _MoniumDriverSecure;
    NCommon::TConfSetting<ui64, Static> _MoniumListShardsPageSize;
    NCommon::TConfSetting<bool, Static> _MoniumPrepareResources;
    NCommon::TConfSetting<bool, Static> MoniumPrependProjectToResourceIds;
    NCommon::TConfSetting<TDuration, Static> MoniumMetricCollectionInterval;
    NCommon::TConfSetting<TDuration, Static> MoniumGrid;
    NCommon::TConfSetting<TDuration, Static> MoniumServiceMetricsTtl;
    NCommon::TConfSetting<TDuration, Static> MoniumClusterMetricsTtl;
    NCommon::TConfSetting<TDuration, Static> MoniumShardMetricsTtl;

    NCommon::TConfSetting<uint64_t, Static> _SwitchComputationNodeBufferSizeBytes;

    NCommon::TConfSetting<bool, Static> _RunVanillaOperation;
    NCommon::TConfSetting<TString, Static> _DumpPipelineSpecToDirectory;

public:
    TString GetRuntimeCluster() const;
    TString GetPipelinePath() const;
    TString GetYtConsumerPath() const;
    bool GetYtConsumerVital() const;
    TString GetYtProducerPath() const;
    bool GetEnableComputationPatternResources() const;

protected:
    NCommon::TConfSetting<TString, Static> RuntimeCluster;

    NCommon::TConfSetting<TString, Static> PipelineDirectory;
    NCommon::TConfSetting<TString, Static> PipelineName;
    NCommon::TConfSetting<TString, Static> PipelinePath;

    NCommon::TConfSetting<TString, Static> YtConsumerDirectory;
    NCommon::TConfSetting<TString, Static> YtConsumerName;
    NCommon::TConfSetting<TString, Static> YtConsumerPath;
    NCommon::TConfSetting<bool, Static> YtConsumerVital;

    NCommon::TConfSetting<TString, Static> YtProducerDirectory;
    NCommon::TConfSetting<TString, Static> YtProducerName;
    NCommon::TConfSetting<TString, Static> YtProducerPath;
};

struct TYtflowConfiguration
    : public TYtflowSettings
    , public NCommon::TSettingDispatcher
{
public:
    using TPtr = TIntrusivePtr<TYtflowConfiguration>;

    TYtflowConfiguration();

    template <class TProtoConfig>
    void Init(const TProtoConfig& config);
};

} // namespace NYql

#include "yql_ytflow_configuration-inl.h"
