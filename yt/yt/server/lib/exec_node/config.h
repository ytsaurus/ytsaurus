#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_agent/config.h>

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/nbd/config.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/library/dns_over_rpc/client/config.h>

#include <yt/yt/library/gpu/config.h>

#include <yt/yt/library/tracing/jaeger/public.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/ytree/node.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TSlotLocationConfig
    : public NServer::TDiskLocationConfig
{
    //! Maximum reported total disk capacity.
    std::optional<i64> DiskQuota;

    //! Reserve subtracted from disk capacity.
    i64 DiskUsageWatermark;

    TString MediumName;

    //! Enforce disk space limits using disk quota.
    bool EnableDiskQuota;

    REGISTER_YSON_STRUCT(TSlotLocationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlotLocationConfig)

////////////////////////////////////////////////////////////////////////////////

struct TNumaNodeConfig
    : public virtual NYTree::TYsonStruct
{
    i64 NumaNodeId;
    i64 CpuCount;
    TString CpuSet;

    REGISTER_YSON_STRUCT(TNumaNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNumaNodeConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSlotManagerTestingConfig
    : public virtual NYTree::TYsonStruct
{
    //! If set, slot manager does not report JobProxyUnavailableAlert
    //! allowing scheduler to schedule jobs to current node. Such jobs are
    //! going to be aborted instead of failing; that is exactly what we test
    //! using this switch.
    bool SkipJobProxyUnavailableAlert;

    REGISTER_YSON_STRUCT(TSlotManagerTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlotManagerTestingConfig)

struct TSlotManagerConfig
    : public virtual NYTree::TYsonStruct
{
    //! Root path for slot directories.
    std::vector<TSlotLocationConfigPtr> Locations;

    //! Enable using tmpfs on the node.
    bool EnableTmpfs;

    //! Use MNT_DETACH when tmpfs umount called. When option enabled the "Device is busy" error is impossible,
    //! because actual umount will be performed by Linux core asynchronously.
    bool DetachedTmpfsUmount;

    //! Polymorphic job environment configuration.
    NJobProxy::TJobEnvironmentConfig JobEnvironment;

    bool EnableReadWriteCopy;

    bool EnableArtifactCopyTracking;

    //! If set, user job will not receive uid.
    //! For testing purposes only.
    bool DoNotSetUserId;

    //! Chunk size used for copying chunks if #copy_chunks is set to %true in operation spec.
    i64 FileCopyChunkSize;

    TDuration DiskResourcesUpdatePeriod;

    TDuration SlotLocationStatisticsUpdatePeriod;

    //! Default medium used to run jobs without disk requests.
    TString DefaultMediumName;

    TSlotManagerTestingConfigPtr Testing;

    std::vector<TNumaNodeConfigPtr> NumaNodes;

    REGISTER_YSON_STRUCT(TSlotManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlotManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSlotManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    bool DisableJobsOnGpuCheckFailure;

    //! Enforce disk space limits in periodic disk resources update.
    bool CheckDiskSpaceLimit;

    //! How to distribute cpu resources between 'common' and 'idle' slots.
    double IdleCpuFraction;

    bool EnableNumaNodeScheduling;

    bool EnableJobEnvironmentResurrection;

    int MaxConsecutiveGpuJobFailures;

    int MaxConsecutiveJobAborts;

    TConstantBackoffOptions DisableJobsBackoffStrategy;

    // COMPAT(psushin): temporary flag to disable CloseAllDescriptors machinery.
    bool ShouldCloseDescriptors;

    TDuration SlotInitTimeout;

    TDuration SlotReleaseTimeout;

    TDuration VolumeReleaseTimeout;

    bool AbortOnFreeVolumeSynchronizationFailed;

    bool AbortOnJobsDisabled;

    bool EnableContainerDeviceChecker;

    bool RestartContainerAfterFailedDeviceCheck;

    //! Polymorphic job environment configuration.
    NJobProxy::TJobEnvironmentConfig JobEnvironment;

    REGISTER_YSON_STRUCT(TSlotManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlotManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TVolumeManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    //! For testing.
    std::optional<TDuration> DelayAfterLayerImported;

    bool EnableAsyncLayerRemoval;

    bool AbortOnOperationWithVolumeFailed;

    bool AbortOnOperationWithLayerFailed;

    //! For testing purpuses.
    bool ThrowOnPrepareVolume;

    REGISTER_YSON_STRUCT(TVolumeManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TVolumeManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TUserJobSensor
    : public NYTree::TYsonStruct
{
    NProfiling::EMetricType Type;

    TString ProfilingName;

    REGISTER_YSON_STRUCT(TUserJobSensor);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobSensor)

////////////////////////////////////////////////////////////////////////////////

//! Description of a user job monitoring sensor which is produced from a given job statistic.
struct TUserJobStatisticSensor
    : public TUserJobSensor
{
    NYPath::TYPath Path;

    REGISTER_YSON_STRUCT(TUserJobStatisticSensor);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobStatisticSensor)

////////////////////////////////////////////////////////////////////////////////

struct TUserJobMonitoringDynamicConfig
    : public NYTree::TYsonStruct
{
    THashMap<TString, TUserJobStatisticSensorPtr> StatisticSensors;

    static const THashMap<TString, TUserJobStatisticSensorPtr>& GetDefaultStatisticSensors();

    REGISTER_YSON_STRUCT(TUserJobMonitoringDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobMonitoringDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct THeartbeatReporterDynamicConfigBase
    : public NYTree::TYsonStruct
{
    NConcurrency::TRetryingPeriodicExecutorOptions HeartbeatExecutor;

    //! Timeout of the exec node heartbeat RPC request.
    TDuration HeartbeatTimeout;

    bool EnableTracing;

    NTracing::TSamplerConfigPtr TracingSampler;

    REGISTER_YSON_STRUCT(THeartbeatReporterDynamicConfigBase);

    static void Register(TRegistrar registrar);
};

void FormatValue(TStringBuilderBase* builder, const THeartbeatReporterDynamicConfigBase& config, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TControllerAgentConnectorDynamicConfig
    : public THeartbeatReporterDynamicConfigBase
{
    TDuration JobStalenessDelay;

    TDuration SettleJobsTimeout;

    TDuration TestHeartbeatDelay;

    NConcurrency::TThroughputThrottlerConfigPtr StatisticsThrottler;
    TDuration RunningJobStatisticsSendingBackoff;

    REGISTER_YSON_STRUCT(TControllerAgentConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConnectorDynamicConfig)

void FormatValue(TStringBuilderBase* builder, const TControllerAgentConnectorDynamicConfig& config, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TMasterConnectorDynamicConfig
    : public THeartbeatReporterDynamicConfigBase
{
    REGISTER_YSON_STRUCT(TMasterConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkCacheDynamicConfig
    : public NYTree::TYsonStruct
{
    bool TestCacheLocationDisabling;

    REGISTER_YSON_STRUCT(TChunkCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerConnectorDynamicConfig
    : public THeartbeatReporterDynamicConfigBase
{
    bool SendHeartbeatOnResourcesReleased;

    bool IncludeReleasingResourcesInSchedulerHeartbeat;

    bool UseProfilingTagsFromScheduler;

    REGISTER_YSON_STRUCT(TSchedulerConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobInputCacheDynamicConfig
    : public NYTree::TYsonStruct
{
    bool Enabled;

    std::optional<i64> JobCountThreshold;

    NChunkClient::TBlockCacheDynamicConfigPtr BlockCache;
    TSlruCacheDynamicConfigPtr MetaCache;

    i64 TotalInFlightBlockSize;

    double FallbackTimeoutFraction;

    REGISTER_YSON_STRUCT(TJobInputCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobInputCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TGpuManagerTestingConfig
    : public NYTree::TYsonStruct
{
    //! This is a special testing option.
    //! Instead of normal gpu discovery, it forces the node to believe the number of GPUs passed in the config.
    bool TestResource;

    //! These options enable testing gpu layers and setup commands.
    bool TestLayers;

    bool TestSetupCommands;

    bool TestExtraGpuCheckCommandFailure;

    int TestGpuCount;

    double TestUtilizationGpuRate;

    TDuration TestGpuInfoUpdatePeriod;

    REGISTER_YSON_STRUCT(TGpuManagerTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuManagerTestingConfig);

////////////////////////////////////////////////////////////////////////////////

struct TGpuManagerConfig
    : public NYTree::TYsonStruct
{
    bool Enable;

    std::optional<NYPath::TYPath> DriverLayerDirectoryPath;
    std::optional<TString> DriverVersion;

    NGpu::TGpuInfoSourceConfigPtr GpuInfoSource;

    TGpuManagerTestingConfigPtr Testing;

    REGISTER_YSON_STRUCT(TGpuManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TGpuManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    TDuration HealthCheckTimeout;
    TDuration HealthCheckPeriod;
    TDuration HealthCheckFailureBackoff;

    TDuration RdmaDeviceInfoUpdateTimeout;
    TDuration RdmaDeviceInfoUpdatePeriod;

    std::optional<TShellCommandConfigPtr> JobSetupCommand;

    NConcurrency::TPeriodicExecutorOptions DriverLayerFetching;

    THashMap<TString, TString> CudaToolkitMinDriverVersion;

    NGpu::TGpuInfoSourceConfigPtr GpuInfoSource;

    //! This option is specific to nvidia-container-runtime.
    TString DefaultNvidiaDriverCapabilities;

    REGISTER_YSON_STRUCT(TGpuManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TShellCommandConfig
    : public NYTree::TYsonStruct
{
    TString Path;
    std::vector<TString> Args;

    REGISTER_YSON_STRUCT(TShellCommandConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TShellCommandConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTestingConfig
    : public NYTree::TYsonStruct
{
    bool FailAddressResolve;

    REGISTER_YSON_STRUCT(TTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTestingConfig)

struct TJobProbeConfig
    : public NYTree::TYsonStruct
{
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TJobProbeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProbeConfig)

struct TJobCommonConfig
    : public NYTree::TYsonStruct
{
    int NodeDirectoryPrepareRetryCount;

    TDuration NodeDirectoryPrepareBackoffTime;

    TDuration JobProxyPreparationTimeout;

    TDuration WaitingForJobCleanupTimeout;

    std::optional<TDuration> JobPrepareTimeLimit;

    //! This option is used for testing purposes only.
    //! Adds inner errors for failed jobs.
    bool TestJobErrorTruncation;

    TDuration MemoryTrackerCachePeriod;

    TDuration SMapsMemoryTrackerCachePeriod;

    TUserJobMonitoringDynamicConfigPtr UserJobMonitoring;

    TDuration SensorDumpTimeout;

    bool TreatJobProxyFailureAsAbort;

    std::optional<TShellCommandConfigPtr> JobSetupCommand;
    TString SetupCommandUser;

    std::optional<int> StatisticsOutputTableCountLimit;

    //! Job throttler config, eg. its RPC timeout and backoff.
    NJobProxy::TJobThrottlerConfigPtr JobThrottler;

    i64 VirtualSandboxSquashFSBlockSize;

    TJobProbeConfigPtr JobProbe;

    TTestingConfigPtr Testing;

    REGISTER_YSON_STRUCT(TJobCommonConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobCommonConfig)

////////////////////////////////////////////////////////////////////////////////

struct TAllocationConfig
    : public NYTree::TYsonStruct
{
    bool EnableMultipleJobs;

    REGISTER_YSON_STRUCT(TAllocationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAllocationConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobControllerDynamicConfig
    : public NYTree::TYsonStruct
{
    TConstantBackoffOptions OperationInfoRequestBackoffStrategy;

    TDuration WaitingForResourcesTimeout;
    // COMPAT(arkady-e1ppa): Remove when CA&Sched are update to
    // a proper version of 24.1/24.2
    bool DisableLegacyAllocationPreparation;

    TDuration CpuOverdraftTimeout;

    //! Default disk space request.
    i64 MinRequiredDiskSpace;

    TDuration MemoryOverdraftTimeout;

    TDuration ResourceAdjustmentPeriod;

    TDuration RecentlyRemovedJobsCleanPeriod;
    TDuration RecentlyRemovedJobsStoreTimeout;

    TDuration JobProxyBuildInfoUpdatePeriod;

    bool DisableJobProxyProfiling;

    NJobProxy::TJobProxyDynamicConfigPtr JobProxy;

    TDuration UnknownOperationJobsRemovalDelay;

    TDuration DisabledJobsInterruptionTimeout;

    TJobCommonConfigPtr JobCommon;

    TDuration ProfilingPeriod;

    bool ProfileJobProxyProcessExit;

    //! This option is used for testing purposes only.
    //! Adds delay before starting a job.
    std::optional<TDuration> TestResourceAcquisitionDelay;

    TAllocationConfigPtr Allocation;

    REGISTER_YSON_STRUCT(TJobControllerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobControllerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TNbdClientConfig
    : public NYTree::TYsonStruct
{
    TDuration IOTimeout;
    TDuration ReconnectTimeout;
    int ConnectionCount;

    REGISTER_YSON_STRUCT(TNbdClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNbdClientConfig)

////////////////////////////////////////////////////////////////////////////////

struct TNbdConfig
    : public NYTree::TYsonStruct
{
    bool Enabled;
    TNbdClientConfigPtr Client;
    NNbd::TNbdServerConfigPtr Server;
    i64 BlockCacheCompressedDataCapacity;

    REGISTER_YSON_STRUCT(TNbdConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNbdConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobProxyLoggingConfig
    : public NYTree::TYsonStruct
{
    EJobProxyLoggingMode Mode;

    NLogging::TLogManagerConfigPtr LogManagerTemplate;

    std::optional<TString> JobProxyStderrPath;
    std::optional<TString> ExecutorStderrPath;

    REGISTER_YSON_STRUCT(TJobProxyLoggingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyLoggingConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobProxyConfig
    : public NYTree::TYsonStruct
{
    TJobProxyLoggingConfigPtr JobProxyLogging;

    NTracing::TJaegerTracerConfigPtr JobProxyJaeger;

    NDns::TDnsOverRpcResolverConfigPtr JobProxyDnsOverRpcResolver;

    NAuth::TAuthenticationManagerConfigPtr JobProxyAuthenticationManager;

    NJobProxy::TCoreWatcherConfigPtr CoreWatcher;

    TDuration SupervisorRpcTimeout;

    TDuration JobProxyHeartbeatPeriod;

    bool JobProxySendHeartbeatBeforeAbort;

    //! This is a special testing option.
    //! Instead of actually setting root fs, it just provides special environment variable.
    bool TestRootFS;

    //! This option is used for testing purposes only.
    //! It runs job shell under root user instead of slot user.
    bool TestPollJobShell;

    //! This option can disable memory limit check for user jobs.
    //! Used in arcadia tests, since it's almost impossible to set
    //! proper memory limits for asan builds.
    bool CheckUserJobMemoryLimit;

    //! Enables job abort on violated memory reserve.
    bool AlwaysAbortOnMemoryReserveOverdraft;

    //! Forward variables from job proxy environment to user job.
    bool ForwardAllEnvironmentVariables;

    REGISTER_YSON_STRUCT(TJobProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyConfig)

////////////////////////////////////////////////////////////////////////////////

struct TLogDumpConfig
    : public NYTree::TYsonStruct
{
    i64 BufferSize;

    // Name of the log writer which is used for dump.
    TString LogWriterName;

    REGISTER_YSON_STRUCT(TLogDumpConfig);

    static void Register(TRegistrar registrar);
};


DEFINE_REFCOUNTED_TYPE(TLogDumpConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobProxyLogManagerConfig
    : public NYTree::TYsonStruct
{
    TString Directory;

    int ShardingKeyLength;

    TDuration LogsStoragePeriod;

    // Value std::nullopt means unlimited concurrency.
    int DirectoryTraversalConcurrency;

    TLogDumpConfigPtr LogDump;

    REGISTER_YSON_STRUCT(TJobProxyLogManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyLogManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TLogDumpDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<i64> BufferSize;

    // Name of the log writer which is used for dump.
    std::optional<TString> LogWriterName;

    REGISTER_YSON_STRUCT(TLogDumpDynamicConfig);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_STRUCT(TLogDumpDynamicConfig)

DEFINE_REFCOUNTED_TYPE(TLogDumpDynamicConfig)

struct TJobProxyLogManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<TDuration> LogsStoragePeriod;
    std::optional<int> DirectoryTraversalConcurrency;

    TLogDumpDynamicConfigPtr LogDump;

    REGISTER_YSON_STRUCT(TJobProxyLogManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyLogManagerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

struct TExecNodeConfig
    : public NYTree::TYsonStruct
{
    //! Bind mounts added for all user job containers.
    //! Should include ChunkCache if artifacts are passed by symlinks.
    std::vector<NJobProxy::TBindConfigPtr> RootFSBinds;

    TSlotManagerConfigPtr SlotManager;

    TGpuManagerConfigPtr GpuManager;

    NProfiling::TSolomonExporterConfigPtr JobProxySolomonExporter;

    TJobProxyConfigPtr JobProxy;

    TJobProxyLogManagerConfigPtr JobProxyLogManager;

    REGISTER_YSON_STRUCT(TExecNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecNodeConfig)

////////////////////////////////////////////////////////////////////////////////

struct TExecNodeDynamicConfig
    : public NYTree::TYsonStruct
{
    TMasterConnectorDynamicConfigPtr MasterConnector;

    TSlotManagerDynamicConfigPtr SlotManager;

    TVolumeManagerDynamicConfigPtr VolumeManager;

    TGpuManagerDynamicConfigPtr GpuManager;

    TJobProxyLogManagerDynamicConfigPtr JobProxyLogManager;

    TJobControllerDynamicConfigPtr JobController;

    NServer::TJobReporterConfigPtr JobReporter;

    TSchedulerConnectorDynamicConfigPtr SchedulerConnector;

    TControllerAgentConnectorDynamicConfigPtr ControllerAgentConnector;

    NConcurrency::TThroughputThrottlerConfigPtr UserJobContainerCreationThrottler;

    TChunkCacheDynamicConfigPtr ChunkCache;

    TJobInputCacheDynamicConfigPtr JobInputCache;

    // NB(yuryalekseev): At the moment dynamic NBD config is used only to create
    // NBD server during startup or to dynamically enable/disable creation of NBD volumes.
    TNbdConfigPtr Nbd;

    REGISTER_YSON_STRUCT(TExecNodeDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
